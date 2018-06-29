#include "joe_plan.hpp"

#include <experimental/filesystem>

#include "cost_model/cost_feature_lqp_node_proxy.hpp"
#include "cost_model/cost_feature_operator_proxy.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "utils/execute_with_timeout.hpp"

#include "boost/interprocess/sync/file_lock.hpp"
#include "boost/interprocess/sync/sharable_lock.hpp"
#include "boost/interprocess/sync/scoped_lock.hpp"

#include "out.hpp"
#include "joe_query_iteration.hpp"
#include "joe_query.hpp"
#include "joe.hpp"
#include "optimizer/join_ordering/join_graph_builder.hpp"

namespace opossum {

std::ostream &operator<<(std::ostream &stream, const JoePlanSample &sample) {
  stream <<
    sample.execution_duration.count() << "," <<
    sample.est_cost << "," <<
    sample.re_est_cost << "," <<
    sample.aim_cost << "," <<
    sample.abs_est_cost_error << "," <<
    sample.abs_re_est_cost_error << "," <<
    sample.cecaching_duration.count() << "," <<
         sample.hash;
  return stream;
}

JoePlan::JoePlan(JoeQueryIteration& query_iteration, const Cost estimated_cost, const std::shared_ptr<AbstractLQPNode>& lqp, const size_t idx):
  query_iteration(query_iteration), lqp(lqp), idx(idx)
{
  sample.hash = lqp->hash();
  sample.est_cost = estimated_cost;
}

void JoePlan::run() {
  out() << "----- Evaluating plan " << idx << "/" << (query_iteration.plans.size() - 1) << std::endl;

  auto& config = query_iteration.query.joe.config;

  /**
   *  Translate to PQP
   */
  const auto pqp = LQPTranslator{}.translate_node(lqp);
  const auto operators = flatten_pqp(pqp);

  /**
   * Execute plan
   */
  // init_cardinality_cache_entries(operators);

  auto timer = Timer{};
  auto timed_out = false;
  if (query_iteration.current_plan_timeout) {
    timed_out = execute_with_timeout(pqp, std::chrono::seconds(*query_iteration.current_plan_timeout));
  } else {
    execute_with_timeout(pqp, std::nullopt);
  }
  sample.execution_duration = timer.lap();

  /**
   * Evaluate plan execution
   */
  sample_cost_features(operators);
  cache_cardinalities(operators);

  if (timed_out) {
    sample.execution_duration = std::chrono::microseconds{0};
    out() << "---- Query timed out" << std::endl;
    return;
  }

  ++query_iteration.plans_execution_count;

  // Visualize after execution
  SQLQueryPlan plan;
  plan.add_tree_by_root(pqp);
  if (config->visualize) visualize(plan);

  /**
   * Adjust dynamic timeout
   */
  if (config->dynamic_plan_timeout_enabled) {
   if (!query_iteration.sample.best_plan || sample.execution_duration < query_iteration.sample.best_plan->sample.execution_duration) {
      query_iteration.current_plan_timeout =
        std::chrono::duration_cast<std::chrono::seconds>(sample.execution_duration) +
        std::chrono::seconds(2);
      out() << "----- New dynamic timeout is " << query_iteration.current_plan_timeout->count() << " seconds"
            << std::endl;
    }
  }

  if (query_iteration.query.save_plan_results) save_plan_result_table(plan);
}

void JoePlan::sample_cost_features(const std::vector<std::shared_ptr<AbstractOperator>> &operators) {
  auto& config = query_iteration.query.joe.config;

  if (!config->cost_sample_dir) return;

  const auto path = *config->cost_sample_dir + "/" + "CostFeatureSamples" + (IS_DEBUG ? "-Debug" : "-Release") + ".json";

  nlohmann::json json;

  std::ifstream istream;
  std::ofstream ostream;

  // "Touch" the file
  if (!std::experimental::filesystem::exists(path)) {
    std::cout << "Touch CostFeatureSamples '" << path << "'" << std::endl;
    std::ofstream{path, std::ios_base::app};
  }

  {
    boost::interprocess::file_lock file_lock(path.c_str());
    boost::interprocess::scoped_lock<boost::interprocess::file_lock> lock(file_lock);

    istream.open(path);

    istream.seekg(0, std::ios_base::end);
    if (istream.tellg() != 0) {
      istream.seekg(0, std::ios_base::beg);

      istream >> json;
    }

    for (const auto& op: operators) {
      const auto operator_type = operator_type_to_string.left.at(op->type());
      if (!json.count(operator_type)) { 
        json[operator_type] = nlohmann::json::array();
      }

      nlohmann::json sample;
      for (const auto& cost_feature_pair : cost_feature_to_string.left) {
        const auto cost_feature_sample = CostFeatureOperatorProxy{op}.extract_feature(cost_feature_pair.first);

        sample[cost_feature_pair.second] = cost_feature_sample.to_string();
      }
      sample["Runtime"] = op->base_performance_data().total.count();

      json[operator_type].push_back(sample);
    }

    ostream.open(path);
    ostream << json;

    ostream.flush(); // Make sure write-to-disk happened before unlocking
  }
}

void JoePlan::visualize(const SQLQueryPlan& plan) {
  auto& config = query_iteration.query.joe.config;

  try {
    SQLQueryPlanVisualizer visualizer{{}, {}, {}, {}};
    visualizer.set_cost_model(config->cost_model);

    std::stringstream stream;
    stream << config->evaluation_dir << "/viz/";
    stream << "WIter." << query_iteration.query.joe.workload_iteration_idx << ".";
    stream << query_iteration.query.sample.name << ".QIter." << query_iteration.idx;
    stream << ".Plan." << idx << ".svg";

    visualizer.visualize(plan, config->tmp_dot_file_path, stream.str());
  } catch (const std::exception &e) {
    out() << "----- Error while visualizing: " << e.what() << std::endl;
  }
}

void JoePlan::save_plan_result_table(const SQLQueryPlan& plan) {
  auto output_wrapper = std::make_shared<TableWrapper>(plan.tree_roots().at(0)->get_output());
  output_wrapper->execute();

  auto limit = std::make_shared<Limit>(output_wrapper, 500);
  limit->execute();

  std::ofstream output_file(query_iteration.query.prefix() + "ResultTable.txt");
  output_file << "Total Row Count: " << plan.tree_roots().at(0)->get_output()->row_count() << std::endl;
  output_file << std::endl;
  Print::print(limit->get_output(), 0, output_file);

  query_iteration.query.save_plan_results = false;
}

void JoePlan::init_cardinality_cache_entries(const std::vector<std::shared_ptr<AbstractOperator>> &operators) {
  Timer timer([&](const auto& duration) {
    sample.cecaching_duration += duration;
  });

  auto& config = query_iteration.query.joe.config;

  const auto cardinality_estimation_cache = config->cardinality_estimation_cache;

  for (const auto& op : operators) {
    if (!op->lqp_node()) return;
    if (!op->get_output()) return;

    const auto lqp = op->lqp_node();

    if (lqp->type() != LQPNodeType::Predicate && lqp->type() != LQPNodeType::Join && lqp->type() != LQPNodeType::StoredTable) return;

    auto join_graph_builder = JoinGraphBuilder{};
    join_graph_builder.traverse(lqp);

    BaseJoinGraph base_join_graph{join_graph_builder.vertices(), join_graph_builder.predicates()};

    cardinality_estimation_cache->put(base_join_graph, CardinalityEstimationCache::TIMEOUT_CARDINALITY);
  }
}

void JoePlan::cache_cardinalities(const std::vector<std::shared_ptr<AbstractOperator>> &operators) {
  Timer timer([&](const auto& duration) {
    sample.cecaching_duration += duration;
  });

  auto& config = query_iteration.query.joe.config;

  const auto cardinality_estimation_cache = config->cardinality_estimation_cache;

  for (const auto& op : operators) {
    if (!op->lqp_node()) return;
    if (!op->get_output()) return;

    const auto lqp = op->lqp_node();

    if (lqp->type() != LQPNodeType::Predicate && lqp->type() != LQPNodeType::Join && lqp->type() != LQPNodeType::StoredTable) return;

    auto join_graph_builder = JoinGraphBuilder{};
    join_graph_builder.traverse(lqp);

    cardinality_estimation_cache->put({join_graph_builder.vertices(), join_graph_builder.predicates()}, op->get_output()->row_count());
  }
}

}  // namespace opossum