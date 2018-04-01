#include <array>
#include <chrono>
#include <thread>
#include <fstream>
#include <iostream>

#include <cxxopts.hpp>

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
#include "optimizer/join_ordering/abstract_join_plan_node.hpp"
#include "optimizer/join_ordering/cost.hpp"
#include "optimizer/join_ordering/cost_model_naive.hpp"
#include "optimizer/join_ordering/cost_model_segmented.hpp"
#include "optimizer/join_ordering/dp_ccp.hpp"
#include "optimizer/join_ordering/dp_ccp_top_k.hpp"
#include "optimizer/strategy/join_ordering_rule.hpp"
#include "optimizer/table_statistics.hpp"
#include "planviz/lqp_visualizer.hpp"
#include "planviz/sql_query_plan_visualizer.hpp"
#include "scheduler/current_scheduler.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "sql/sql.hpp"
#include "storage/storage_manager.hpp"
#include "tpch/tpch_db_generator.hpp"
#include "tpch/tpch_queries.hpp"
#include "utils/timer.hpp"
#include "utils/table_generator2.hpp"

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
    const auto aim_cost = cost_model.get_operator_cost(*op, OperatorCostMode::TargetCost);
    if (aim_cost) {
      sample.aim_cost += *aim_cost;
    }

    if (op->lqp_node()) {
      const auto est_cost = cost_model.get_node_cost(*op->lqp_node());
      if (est_cost) {
        sample.est_cost += *est_cost;
        if (aim_cost) {
          sample.abs_est_cost_error += std::fabs(*est_cost - *aim_cost);
        }
      }
    }

    const auto re_est_cost = cost_model.get_operator_cost(*op, OperatorCostMode::PredictedCost);
    if (re_est_cost) {
      sample.re_est_cost += *re_est_cost;
      if (aim_cost) {
        sample.abs_re_est_cost_error += std::fabs(*re_est_cost - *aim_cost);
      }
    }
  }

  return sample;
}

std::ostream &operator<<(std::ostream &stream, const PlanCostSample &sample) {
  stream << sample.est_cost << "," << sample.re_est_cost << "," << sample.aim_cost << ","
         << sample.abs_est_cost_error << "," << sample.abs_re_est_cost_error;
  return stream;
}
}

int main(int argc, char ** argv) {
  auto scale_factor = 0.1f;
  auto cost_model_str = "segmented"s;
  auto visualize = false;
  auto timeout_seconds = std::optional<long>{0};

  cxxopts::Options cli_options_description{"Hyrise Join Ordering Evaluator", ""};

  // clang-format off
  cli_options_description.add_options()
    ("help", "print this help message")
    ("v,verbose", "Print log messages", cxxopts::value<bool>(verbose)->default_value("true"))
    ("s,scale", "Database scale factor (1.0 ~ 1GB)", cxxopts::value<float>(scale_factor)->default_value("0.001"))
    ("m,cost_model", "CostModel to use (naive, segmented)", cxxopts::value<std::string>(cost_model_str)->default_value(cost_model_str))  // NOLINT
    ("t,timeout", "Timeout per query, in seconds", cxxopts::value<long>(*timeout_seconds)->default_value("0"))  // NOLINT
    ("visualize", "Visualize query plan", cxxopts::value<bool>(visualize)->default_value("false"))  // NOLINT
    ("queries", "Specify queries to run, default is all that are supported", cxxopts::value<std::vector<QueryID>>()); // NOLINT
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

  // Build list of query ids to be benchmarked and display it
  std::vector<QueryID> query_ids;
  if (cli_parse_result.count("queries")) {
    const auto cli_query_ids = cli_parse_result["queries"].as<std::vector<QueryID>>();
    for (const auto cli_query_id : cli_query_ids) {
      query_ids.emplace_back(cli_query_id - 1);  // Offset because TPC-H query 1 has index 0
    }
  } else {
    std::copy(std::begin(opossum::tpch_supported_queries), std::end(opossum::tpch_supported_queries),
              std::back_inserter(query_ids));
  }
  out() << "- Benchmarking Queries ";
  for (const auto query_id : query_ids) {
    out() << (query_id + 1) << " ";
  }
  out() << std::endl;

  if (*timeout_seconds <= 0) {
    timeout_seconds.reset();
    out() << "-- No timeout" << std::endl;
  } else {
    out() << "-- Queries will timeout after " << *timeout_seconds << std::endl;
  }

  out() << "-- Generating TPCH tables with scale factor " << scale_factor << std::endl;
  TpchDbGenerator{scale_factor}.generate_and_store();
  out() << "-- Done." << std::endl;

  auto cost_model = std::shared_ptr<AbstractCostModel>();
  if (cost_model_str == "naive") {
    out() << "-- Using CostModelNaive" << std::endl;
    cost_model = std::make_shared<CostModelNaive>();
  } else if (cost_model_str == "segmented") {
    out() << "-- Using CostModelSegmented" << std::endl;
    cost_model = std::make_shared<CostModelSegmented>();
  } else {
    Fail("Unknown cost model");
  }

  for (const auto tpch_query_idx : query_ids) {
    const auto sql = tpch_queries[tpch_query_idx];
    const auto query_name = "TPCH-"s + std::to_string(tpch_query_idx + 1);
    out() << "-- Query: " << query_name << std::endl;

    const auto evaluation_name =
    (IS_DEBUG ? "debug"s : "release"s) + "-" + query_name + "-sf" + std::to_string(scale_factor);

    auto plan_durations = std::vector<std::chrono::microseconds>();
    auto plan_cost_samples = std::vector<PlanCostSample>();

    auto pipeline_statement = SQL{sql}.disable_mvcc().pipeline_statement();

    const auto lqp = pipeline_statement.get_optimized_logical_plan();
    const auto lqp_root = LogicalPlanRootNode::make(lqp);
    const auto join_graph = JoinGraph::from_lqp(lqp);

    DpCcpTopK dp_ccp_top_k{DpSubplanCacheTopK::NO_ENTRY_LIMIT, cost_model};

    dp_ccp_top_k(join_graph);

    JoinVertexSet all_vertices{join_graph->vertices.size()};
    all_vertices.flip();
    const auto join_plans = dp_ccp_top_k.subplan_cache()->get_best_plans(all_vertices);

    out() << "-- Generated plans: " << join_plans.size() << std::endl;

    auto current_plan_idx = 0;

    for (const auto &join_plan : join_plans) {
      out() << "-- Plan Index: " << current_plan_idx << std::endl;

      const auto join_ordered_sub_lqp = join_plan->to_lqp();
      for (const auto &parent_relation : join_graph->output_relations) {
        parent_relation.output->set_input(parent_relation.input_side, join_ordered_sub_lqp);
      }

      const auto pqp = LQPTranslator{}.translate_node(lqp_root->left_input());

      auto transaction_context = TransactionManager::get().new_transaction_context();
      pqp->set_transaction_context_recursively(transaction_context);

      if (timeout_seconds) {
        const auto seconds = *timeout_seconds;
        std::thread timeout_thread([transaction_context, seconds]() {
          std::this_thread::sleep_for(std::chrono::seconds(seconds));
          transaction_context->rollback(TransactionPhaseSwitch::Lenient);
        });
        timeout_thread.detach();
      }

      SQLQueryPlan plan;
      plan.add_tree_by_root(pqp);

      Timer timer;
      CurrentScheduler::schedule_and_wait_for_tasks(plan.create_tasks());

      if (!transaction_context->commit(TransactionPhaseSwitch::Lenient)) {
        out() << "-- Query took too long" << std::endl;
        plan_durations.emplace_back(0);
        plan_cost_samples.emplace_back(PlanCostSample{});
        ++current_plan_idx;
      } else {
        const auto plan_duration = timer.lap();

        plan_durations.emplace_back(plan_duration);

        const auto operators = flatten_pqp(pqp);
        plan_cost_samples.emplace_back(create_plan_cost_sample(*cost_model, operators));

        /**
         * Visualize
         */
        if (visualize) {
          GraphvizConfig graphviz_config;
          graphviz_config.format = "svg";
          VizGraphInfo viz_graph_info;
          viz_graph_info.bg_color = "black";

          SQLQueryPlanVisualizer visualizer{graphviz_config, viz_graph_info, {}, {}};
          visualizer.set_cost_model(cost_model);
          visualizer.visualize(plan, "tmp.dot",
                               std::string("viz/") + evaluation_name + "_" + std::to_string(current_plan_idx) + "_" +
                               std::to_string(plan_duration.count()) + ".svg");
        }
      }

      /**
       * CSV
       */
      auto csv = std::ofstream{evaluation_name + ".csv"};
      csv << "Idx,Duration,EstCost,ReEstCost,AimCost,AbsEstCostError,AbsReEstCostError" << "\n";
      for (auto plan_idx = size_t{0}; plan_idx < plan_durations.size(); ++plan_idx) {
        csv << plan_idx << "," << plan_durations[plan_idx].count() << "," << plan_cost_samples[plan_idx] << "\n";
      }
      csv.close();

      ++current_plan_idx;
    }
  }
}
