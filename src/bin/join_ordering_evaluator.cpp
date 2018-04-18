#include <array>
#include <chrono>
#include <thread>
#include <fstream>
#include <iostream>

#include <cxxopts.hpp>
#include <statistics/generate_table_statistics.hpp>
#include <experimental/filesystem>
#include <random>

#include "constant_mappings.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/product.hpp"
#include "concurrency/transaction_context.hpp"
#include "concurrency/transaction_manager.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/utils/flatten_pqp.hpp"
#include "operators/export_binary.hpp"
#include "operators/import_binary.hpp"
#include "operators/limit.hpp"
#include "optimizer/join_ordering/abstract_join_plan_node.hpp"
#include "cost_model/cost.hpp"
#include "cost_model/cost_model_naive.hpp"
#include "cost_model/cost_model_linear.hpp"
#include "import_export/csv_parser.hpp"
#include "optimizer/join_ordering/dp_ccp.hpp"
#include "optimizer/join_ordering/dp_ccp_top_k.hpp"
#include "optimizer/strategy/join_ordering_rule.hpp"
#include "statistics/table_statistics.hpp"
#include "optimizer/table_statistics_cache.hpp"
#include "planviz/lqp_visualizer.hpp"
#include "planviz/sql_query_plan_visualizer.hpp"
#include "scheduler/current_scheduler.hpp"
#include "statistics/statistics_import_export.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "sql/sql.hpp"
#include "storage/storage_manager.hpp"
#include "tpch/tpch_db_generator.hpp"
#include "tpch/tpch_queries.hpp"
#include "utils/timer.hpp"
#include "utils/table_generator2.hpp"
#include "utils/format_duration.hpp"

namespace {
using namespace std::string_literals;  // NOLINT
using namespace opossum;  // NOLINT

using QueryID = size_t;

// Used to config out()
auto verbose = true;

/**
 * @return std::cout if `verbose` is true, otherwise returns a discarding stream
 */
std::ostream &out() {
  if (verbose) {
    return std::cout;
  }

  // Create no-op stream that just swallows everything streamed into it
  // See https://stackoverflow.com/a/11826666
  class NullBuffer : public std::streambuf {
   public:
    int overflow(int c) { return c; }
  };

  static NullBuffer null_buffer;
  static std::ostream null_stream(&null_buffer);

  return null_stream;
}

struct PlanCostSample final {
  Cost est_cost{0.0f};
  Cost re_est_cost{0.0f};
  Cost aim_cost{0.0f};
  Cost abs_est_cost_error{0.0f};
  Cost abs_re_est_cost_error{0.0f};
};

PlanCostSample create_plan_cost_sample(const AbstractCostModel &cost_model,
                                       const std::vector<std::shared_ptr<AbstractOperator>> &operators) {
  PlanCostSample sample;

  for (const auto &op : operators) {
    const auto aim_cost = cost_model.get_reference_operator_cost(op);
    sample.aim_cost += aim_cost;

    if (op->lqp_node()) {
      const auto est_cost = cost_model.estimate_lqp_node_cost(op->lqp_node());
        sample.est_cost += est_cost;
        if (aim_cost) {
          sample.abs_est_cost_error += std::fabs(est_cost - aim_cost);
        }
    }

    const auto re_est_cost = cost_model.estimate_operator_cost(op);
      sample.re_est_cost += re_est_cost;
      sample.abs_re_est_cost_error += std::fabs(re_est_cost - aim_cost);
  }

  return sample;
}

std::ostream &operator<<(std::ostream &stream, const PlanCostSample &sample) {
  stream << sample.est_cost << "," << sample.re_est_cost << "," << sample.aim_cost << ","
         << sample.abs_est_cost_error << "," << sample.abs_re_est_cost_error;
  return stream;
}

class AbstractJoinOrderingWorkload {
 public:
  virtual ~AbstractJoinOrderingWorkload() = default;

  virtual void setup() = 0;
  virtual size_t query_count() const = 0;
  virtual std::string get_query(const size_t query_idx) const = 0;
  virtual std::string get_query_name(const size_t query_idx) const = 0;
};

class TpchJoinOrderingWorkload : public AbstractJoinOrderingWorkload {
 public:
  TpchJoinOrderingWorkload(float scale_factor, const std::optional<std::vector<QueryID>>& query_ids):
    _scale_factor(scale_factor)
  {
    if (!query_ids) {
      std::copy(std::begin(tpch_supported_queries), std::end(tpch_supported_queries),
                std::back_inserter(_query_ids));
    } else {
      _query_ids = *query_ids;
    }
  }

  void setup() override {
    out() << "-- Generating TPCH tables with scale factor " << _scale_factor << std::endl;
    TpchDbGenerator{_scale_factor}.generate_and_store();
    out() << "-- Done." << std::endl;
  }

  size_t query_count() const override {
    return _query_ids.size();
  }

  std::string get_query(const size_t query_idx) const override {
    return tpch_queries[_query_ids[query_idx]];
  }

  std::string get_query_name(const size_t query_idx) const override {
    return "TPCH"s + "-" + std::to_string(_query_ids[query_idx] + 1);
  }

 private:
  float _scale_factor;
  std::vector<QueryID> _query_ids;
};

class JobWorkload : public AbstractJoinOrderingWorkload {
 public:
  JobWorkload(const std::optional<std::vector<std::string>>& query_names)
  {
    if (query_names) {
      _query_names = *query_names;
    } else {
      _query_names = {
      "1a", "1b", "1c", "1d", "2a", "2b", "2c", "2d",
      // "3a", "3b", "3c"
      "4a", "4b", "4c",
      // "5a", "5b", "5c",
      "6a",
      // "6b",
      "6c",
      //"6d",
      "6e",
      //"6f",
      "7a", "7b",
      //"7c",
      "8a", "8b", "8c", "8d",
      //"9a",
      "9b",
      // "9c", "9d",
      "10a", "10b", "10c", "11a", "11b",
      //"11c", "11d", "12a", "12b", "12c",
      "13a", "13b", "13c", "13d",
      //"14a", "14b", "14c",
      "15a", "15b", "15c", "15d", "16a", "16b", "16c", "16d", "17a", "17b",
      "17c", "17d", "17e", "17f",
      //"18a","18b", "18c", "19a",
      "19b",
      //"19c","19d", "20a", "20b", "20c", "21a", "21b", "21c", "22a", "22b",
      //"22c","22d", "23a", "23b", "23c", "24a",
//     "24b",
//     "25a",
//     "25b",
//     "25c",
//     "26a",
//     "26b",
//     "26c",
//     "27a",
//     "27b",
//     "27c",
//     "28a",
//     "28b",
//     "28c",
//     "29a",
//     "29b",
//     "29c",
//     "30a",
//     "30b",
//     "30c",
//     "31a",
//     "31b",
//     "31c",
      "32a",
      "32b",
      //"33a",
      "33b",
      // "33c"
      };
    }
  }

  void setup() override {
    const auto table_names = std::vector<std::string>{
    "aka_name",
    "aka_title",
    "cast_info",
    "char_name",
    "comp_cast_type",
    "company_name",
    "company_type",
    "complete_cast",
    "info_type",
    "keyword",
    "kind_type",
    "link_type",
    "movie_companies",
    "movie_info",
    "movie_info_idx",
    "movie_keyword",
    "movie_link",
    "name",
    "person_info",
    "role_type",
    "title"
    };

    const auto csvs_path = "/home/Moritz.Eyssen/imdb/csv/";

    for (const auto& table_name : table_names) {
      const auto table_csv_path = csvs_path + table_name + ".csv";
      const auto table_binary_path = csvs_path + table_name + ".bin";
      const auto table_statistics_path = csvs_path + table_name + ".statistics.json";

      out() << "Processing '" << table_name << "'" << std::endl;

      Timer timer;
      auto table = std::shared_ptr<Table>();
      if (std::experimental::filesystem::exists(table_binary_path.c_str())) {
        out() << "  Loading Binary" << std::endl;
        auto import_binary = std::make_shared<ImportBinary>(table_binary_path);
        import_binary->execute();
        table = std::const_pointer_cast<Table>(import_binary->get_output());
      } else {
        out() << "  Loading CSV" << std::endl;
        table = CsvParser{}.parse(table_csv_path);
      }
      out() << "   Done: " << table->row_count() << " rows" << std::endl;
      out() << "   Duration: " << format_duration(std::chrono::duration_cast<std::chrono::nanoseconds>(timer.lap()))
                << std::endl;

      try {
        if (std::experimental::filesystem::exists(table_statistics_path.c_str())) {
          out() << "  Loading Statistics" << std::endl;
          const auto table_statistics = import_table_statistics(table_statistics_path);
          table->set_table_statistics(std::make_shared<TableStatistics>(table_statistics));
          out() << "   Done" << std::endl;
        }
      } catch (const std::exception& e) {
        out() << "ERROR while importing statistics: " << e.what() << std::endl;
      }

      out() << "  Adding to StorageManager" << std::endl;
      StorageManager::get().add_table(table_name, std::const_pointer_cast<Table>(table));
      out() << "   Done" << std::endl;
      out() << "   Duration: " << format_duration(std::chrono::duration_cast<std::chrono::nanoseconds>(timer.lap()))
                << std::endl;

      if (!std::experimental::filesystem::exists(table_statistics_path.c_str())) {
        out() << "  Exporting Statistics" << std::endl;
        export_table_statistics(*table->table_statistics(), table_statistics_path);
        out() << "   Done" << std::endl;
      }

      if (!std::experimental::filesystem::exists(table_binary_path.c_str())) {
        out() << "  Saving binary" << std::endl;
        auto table_wrapper = std::make_shared<TableWrapper>(table);
        table_wrapper->execute();
        auto export_binary = std::make_shared<ExportBinary>(table_wrapper, table_binary_path);
        export_binary->execute();
        out() << "   Done" << std::endl;
        out() << "   Duration: " << format_duration(std::chrono::duration_cast<std::chrono::nanoseconds>(timer.lap()))
                  << std::endl;
      }
    }
  }

  size_t query_count() const override {
    return _query_names.size();
  }

  std::string get_query(const size_t query_idx) const override {
    auto query_file_directory = std::string{"/home/Moritz.Eyssen/hyrise/third_party/join-order-benchmark/"};

    const auto query_file_path = query_file_directory + _query_names[query_idx] + ".sql";

    std::ifstream query_file(query_file_path);
    Assert(query_file.good(), std::string("Failed to open '") + query_file_path + "'");

    query_file.seekg(0, std::ios::end);
    const auto size = query_file.tellg();
    auto query_string = std::string(static_cast<size_t>(size), ' ');
    query_file.seekg(0, std::ios::beg);
    query_file.read(&query_string[0], size);

    return query_string;
  }

  std::string get_query_name(const size_t query_idx) const override {
    return "JOB"s + "-" + _query_names[query_idx];
  }

 private:
  std::vector<std::string> _query_names;
};

}

int main(int argc, char ** argv) {
  auto scale_factor = 0.1f;
  auto cost_model_str = "linear"s;
  auto visualize = false;
  auto plan_timeout_seconds = std::optional<long>{0};
  auto query_timeout_seconds = std::optional<long>{0};
  auto dynamic_plan_timeout = std::optional<long>{0};
  auto dynamic_plan_timeout_enabled = true;
  auto workload_str = "tpch"s;
  auto max_plan_count = std::optional<size_t>{0};
  auto save_results = false;

  cxxopts::Options cli_options_description{"Hyrise Join Ordering Evaluator", ""};

  // clang-format off
  cli_options_description.add_options()
    ("help", "print this help message")
    ("v,verbose", "Print log messages", cxxopts::value<bool>(verbose)->default_value("true"))
    ("s,scale", "Database scale factor (1.0 ~ 1GB). TPCH only", cxxopts::value<float>(scale_factor)->default_value("0.001"))
    ("m,cost_model", "CostModel to use (all, naive, linear)", cxxopts::value<std::string>(cost_model_str)->default_value(cost_model_str))  // NOLINT
    ("timeout-plan", "Timeout per plan, in seconds. Default: 120", cxxopts::value<long>(*plan_timeout_seconds)->default_value("120"))  // NOLINT
    ("dynamic-timeout-plan", "If active, lower timeout to current fastest plan.", cxxopts::value<bool>(dynamic_plan_timeout_enabled)->default_value("true"))  // NOLINT
    ("timeout-query", "Timeout per plan, in seconds. Default: 1800", cxxopts::value<long>(*query_timeout_seconds)->default_value("1800"))  // NOLINT
    ("max-plan-count", "Maximum number of plans per query to execute. Default: 100", cxxopts::value<size_t>(*max_plan_count)->default_value("100"))  // NOLINT
    ("visualize", "Visualize every query plan", cxxopts::value<bool>(visualize)->default_value("false"))  // NOLINT
    ("w,workload", "Workload to run (tpch, job). Default: tpch", cxxopts::value(workload_str)->default_value(workload_str))  // NOLINT
    ("save-results", "Save head of result tables.", cxxopts::value(save_results)->default_value("false"))  // NOLINT
    ("queries", "Specify queries to run, default is all of the workload that are supported", cxxopts::value<std::vector<std::string>>()); // NOLINT
  ;
  // clang-format on

  cli_options_description.parse_positional("queries");
  const auto cli_parse_result = cli_options_description.parse(argc, argv);

  // Display usage and quit
  if (cli_parse_result.count("help")) {
    std::cout << cli_options_description.help({}) << std::endl;
    return 0;
  }

  out() << "- Hyrise Join Ordering Evaluator" << std::endl;

  // Process "queries" parameter
  std::optional<std::vector<std::string>> query_name_strs;
  if (cli_parse_result.count("queries")) {
    query_name_strs = cli_parse_result["queries"].as<std::vector<std::string>>();
  }

  if (query_name_strs) {
    out() << "-- Benchmarking queries ";
    for (const auto& query_name : *query_name_strs) {
      out() << query_name << " ";
    }
    out() << std::endl;
  } else {
    out() << "-- Benchmarking all supported queries of workload" << std::endl;
  }

  // Initialise workload
  auto workload = std::shared_ptr<AbstractJoinOrderingWorkload>();
  if (workload_str == "tpch") {
    out() << "-- Using TPCH workload" << std::endl;
    std::optional<std::vector<QueryID>> query_ids;
    if (query_name_strs) {
      query_ids.emplace();
      for (const auto& query_name : *query_name_strs) {
        query_ids->emplace_back(std::stoi(query_name) - 1);  // Offset because TPC-H query 1 has index 0
      }
    }
    workload = std::make_shared<TpchJoinOrderingWorkload>(scale_factor, query_ids);
  } else if (workload_str == "job") {
    out() << "-- Using Join Order Benchmark workload" << std::endl;
    workload = std::make_shared<JobWorkload>(query_name_strs);
  } else {
    Fail("Unknown workload");
  }

  // Process "timeout-plan/query" parameters
  if (*plan_timeout_seconds <= 0) {
    plan_timeout_seconds.reset();
    out() << "-- No plan timeout" << std::endl;
  } else {
    out() << "-- Plans will timeout after " << *plan_timeout_seconds << " seconds" << std::endl;
  }
  if (*query_timeout_seconds <= 0) {
    query_timeout_seconds.reset();
    out() << "-- No query timeout" << std::endl;
  } else {
    out() << "-- Queries will timeout after " << *query_timeout_seconds << " seconds" << std::endl;
  }
  if (dynamic_plan_timeout_enabled) {
    out() << "-- Plan timeout is dynamic" << std::endl;
  } else {
    out() << "-- Dynamic plan timeout is disabled" << std::endl;
  }

  // Process "max-plan-count" parameter
  if (*max_plan_count <= 0) {
    max_plan_count.reset();
    out() << "-- Executing all plans of a query" << std::endl;
  } else {
    out() << "-- Executing a maximum of " << *max_plan_count << " plans per query" << std::endl;
  }

  // Process "cost-model" parameter
  auto cost_models = std::vector<std::shared_ptr<AbstractCostModel>>{};
  if (cost_model_str == "naive" || cost_model_str == "all") {
    out() << "-- Using CostModelNaive" << std::endl;
    cost_models.emplace_back(std::make_shared<CostModelNaive>());
  }
  if (cost_model_str == "linear" || cost_model_str == "all") {
    out() << "-- Using CostModelLinear" << std::endl;
    cost_models.emplace_back(std::make_shared<CostModelLinear>());
  }
  Assert(!cost_models.empty(), "No CostModel specified");

  // Setup workload
  out() << "-- Setting up workload" << std::endl;
  workload->setup();

  out() << std::endl;

  for (const auto& cost_model : cost_models) {
    out() << "-- Evaluating Cost Model " << cost_model->name() << std::endl;

    for (auto query_idx = size_t{0}; query_idx < workload->query_count(); ++query_idx) {
      const auto sql = workload->get_query(query_idx);
      const auto query_name = workload->get_query_name(query_idx);
      const auto evaluation_name = query_name + "-" + cost_model->name() + "-" + (IS_DEBUG ? "debug"s : "release"s);

      out() << "--- Evaluating Query: " << evaluation_name << std::endl;
      auto pipeline_statement = SQL{sql}.disable_mvcc().pipeline_statement();

      const auto lqp = pipeline_statement.get_optimized_logical_plan();
      const auto lqp_root = LogicalPlanRootNode::make(lqp);
      const auto join_graph = JoinGraph::from_lqp(lqp);

      DpCcpTopK dp_ccp_top_k{DpSubplanCacheTopK::NO_ENTRY_LIMIT, cost_model};

      dp_ccp_top_k(join_graph);

      JoinVertexSet all_vertices{join_graph->vertices.size()};
      all_vertices.flip();
      const auto join_plans = dp_ccp_top_k.subplan_cache()->get_best_plans(all_vertices);

      out() << "----- Generated plans: " << join_plans.size() <<
                   ", EstCostRange: " << join_plans.begin()->plan_cost << " -> " << join_plans.rbegin()->plan_cost << std::endl;

      auto plan_durations = std::vector<long>(join_plans.size(), 0);
      auto plan_cost_samples = std::vector<PlanCostSample>(join_plans.size());
      auto best_plan_milliseconds = std::numeric_limits<long>::max();

      // Shuffle plans
      std::vector<size_t> plan_indices(join_plans.size());
      std::iota(plan_indices.begin(), plan_indices.end(), 0);

      if (plan_indices.size() > 30) {
        std::random_device rd;
        std::mt19937 g(rd());
        auto b = plan_indices.begin();
        std::advance(b, 20);
        std::shuffle(b, plan_indices.end(), g);
      }

      if (max_plan_count) {
        plan_indices.resize(std::min(plan_indices.size(), *max_plan_count));
      }

      //
      const auto query_execution_begin = std::chrono::steady_clock::now();
      auto save_plan_results = save_results;

      //
      auto current_plan_timeout = plan_timeout_seconds;

      for (auto i = size_t{0}; i < plan_indices.size(); ++i) {
        const auto current_plan_idx = plan_indices[i];
        auto join_plan_iter = join_plans.begin();
        std::advance(join_plan_iter, current_plan_idx);
        const auto &join_plan = *join_plan_iter;

        out() << "------- Plan " << current_plan_idx << ", estimated cost: " << join_plan.plan_cost << std::endl;

        // Timeout query
        if (query_timeout_seconds) {
          const auto now = std::chrono::steady_clock::now();
          const auto query_duration = now - query_execution_begin;

          if (std::chrono::duration_cast<std::chrono::seconds>(query_duration).count() >= *query_timeout_seconds) {
            out() << "-------- Query timeout" << std::endl;
            break;
          }
        }

        // Create LQP from join plan
        const auto join_ordered_sub_lqp = join_plan.lqp;
        for (const auto &parent_relation : join_graph->output_relations) {
          parent_relation.output->set_input(parent_relation.input_side, join_ordered_sub_lqp);
        }

        const auto pqp = LQPTranslator{}.translate_node(lqp_root->left_input());

        auto transaction_context = TransactionManager::get().new_transaction_context();
        pqp->set_transaction_context_recursively(transaction_context);

        // Schedule timeout
        if (current_plan_timeout) {
          const auto seconds = *current_plan_timeout;
          std::thread timeout_thread([transaction_context, seconds]() {
            std::this_thread::sleep_for(std::chrono::seconds(seconds + 2));
            if (transaction_context->rollback(TransactionPhaseSwitch::Lenient)) {
              out() << "-------- Query timeout signalled" << std::endl;
            }
          });
          timeout_thread.detach();
        }

        SQLQueryPlan plan;
        plan.add_tree_by_root(pqp);

        Timer timer;
        CurrentScheduler::schedule_and_wait_for_tasks(plan.create_tasks());

        if (!transaction_context->commit(TransactionPhaseSwitch::Lenient)) {
          out() << "-------- Query timeout accepted" << std::endl;
        } else {
          const auto plan_duration = timer.lap();

          plan_durations[current_plan_idx] = plan_duration.count();

          const auto operators = flatten_pqp(pqp);
          const auto plan_cost_sample = create_plan_cost_sample(*cost_model, operators);
          plan_cost_samples[current_plan_idx] = plan_cost_sample;

          /**
           * Visualize
           */
          if (visualize) {
            GraphvizConfig graphviz_config;
            graphviz_config.format = "svg";
            VizGraphInfo viz_graph_info;
            viz_graph_info.bg_color = "black";
            try {
              SQLQueryPlanVisualizer visualizer{graphviz_config, viz_graph_info, {}, {}};
              visualizer.set_cost_model(cost_model);
              visualizer.visualize(plan, "tmp.dot",
                                   std::string("viz/") + evaluation_name + "_" + std::to_string(current_plan_idx) +
                                   "_" +
                                   std::to_string(plan_duration.count()) + ".svg");
            } catch (const std::exception& e) {
              out() << "-------- Error while visualizing: " << e.what() << std::endl;
            }
          }

          if (save_plan_results) {
            auto output_wrapper = std::make_shared<TableWrapper>(plan.tree_roots().at(0)->get_output());
            output_wrapper->execute();

            auto limit = std::make_shared<Limit>(output_wrapper, 500);
            limit->execute();

            std::ofstream output_file(query_name + ".result.txt");
            output_file << "Total Row Count: " << plan.tree_roots().at(0)->get_output()->row_count() << std::endl;
            output_file << std::endl;
            Print::print(limit->get_output(), 0, output_file);

            save_plan_results = false;
          }
          
          /**
           * Adjust dynamic timeout
           */
          const auto plan_milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(plan_duration).count();
          if (plan_milliseconds < best_plan_milliseconds && dynamic_plan_timeout_enabled) {
            best_plan_milliseconds = plan_milliseconds;
            current_plan_timeout = (best_plan_milliseconds / 1000) * 1.2f;
            out() << "------- New dynamic timeout is " << *current_plan_timeout << " seconds" << std::endl;
          }
        }

        /**
         * CSV
         */
        auto csv = std::ofstream{evaluation_name + ".csv"};
        csv << "Idx,Duration,EstCost,ReEstCost,AimCost,AbsEstCostError,AbsReEstCostError" << "\n";
        for (auto plan_idx = size_t{0}; plan_idx < plan_durations.size(); ++plan_idx) {
          csv << plan_idx << "," << plan_durations[plan_idx] << "," << plan_cost_samples[plan_idx] << "\n";
        }
        csv.close();
      }
    }
  }
}
