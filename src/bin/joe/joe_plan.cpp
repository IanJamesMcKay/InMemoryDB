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

namespace opossum {

std::ostream &operator<<(std::ostream &stream, const JoePlanSample &sample) {
  stream <<
    sample.execution_duration.count() << "," <<
    sample.est_cost << "," <<
    sample.re_est_cost << "," <<
    sample.aim_cost << "," <<
    sample.abs_est_cost_error << "," <<
    sample.abs_re_est_cost_error << "," <<
         sample.hash;
  return stream;
}

JoePlan::JoePlan(JoeQueryIteration& query_iteration, const Cost estimated_cost, const std::shared_ptr<AbstractLQPNode>& lqp, const size_t idx):
  query_iteration(query_iteration), lqp(lqp), idx(idx)
{
  name = query_iteration.name + "-" + std::to_string(idx);
  sample.hash = lqp->hash();
  sample.est_cost = estimated_cost;
}

void JoePlan::run() {
  out() << "----- Evaluating " << query_iteration.name << " plan " << idx << "/" << (query_iteration.plans.size() - 1) << std::endl;

  auto& config = query_iteration.query.config;

  /**
   *  Translate to PQP
   */
  LQPTranslator lqp_translator;
  lqp_translator.add_post_operator_callback(
  std::make_shared<CardinalityCachingCallback>(config->cardinality_estimation_cache));
  const auto pqp = lqp_translator.translate_node(lqp);


  /**
   * Execute plan
   */
  auto timer = Timer{};
  auto timed_out = false;
  if (query_iteration.current_plan_timeout) {
    timed_out = execute_with_timeout(pqp, std::chrono::seconds(*query_iteration.current_plan_timeout));
  } else {
    execute_with_timeout(pqp, std::nullopt);
  }

  // Visualize after execution
  SQLQueryPlan plan;
  plan.add_tree_by_root(pqp);
  if (config->visualize) visualize(plan);

  if (timed_out) {
    out() << "---- Query timed out" << std::endl;
    return;
  }

  ++query_iteration.plans_execution_count;

  /**
   * Evaluate plan execution
   */
  sample.execution_duration = timer.lap();

  const auto operators = flatten_pqp(pqp);
  sample_cost_features(operators);


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
  auto& config = query_iteration.query.config;

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
  auto& config = query_iteration.query.config;

  try {
    SQLQueryPlanVisualizer visualizer{{}, {}, {}, {}};
    visualizer.set_cost_model(config->cost_model);
    visualizer.visualize(plan, config->tmp_dot_file_path,
                         std::string(config->evaluation_dir + "/viz/") + name + ".svg");
  } catch (const std::exception &e) {
    out() << "----- Error while visualizing: " << e.what() << std::endl;
  }
}

void JoePlan::save_plan_result_table(const SQLQueryPlan& plan) {
  auto output_wrapper = std::make_shared<TableWrapper>(plan.tree_roots().at(0)->get_output());
  output_wrapper->execute();

  auto limit = std::make_shared<Limit>(output_wrapper, 500);
  limit->execute();

  std::ofstream output_file(query_iteration.query.config->evaluation_prefix + query_iteration.query.sample.name + ".ResultTable.txt");
  output_file << "Total Row Count: " << plan.tree_roots().at(0)->get_output()->row_count() << std::endl;
  output_file << std::endl;
  Print::print(limit->get_output(), 0, output_file);

  query_iteration.query.save_plan_results = false;
}

}  // namespace opossum