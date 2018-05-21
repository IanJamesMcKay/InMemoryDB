#include "joe_plan.hpp"

#include "cost_model/cost_feature_lqp_node_proxy.hpp"
#include "cost_model/cost_feature_operator_proxy.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "utils/execute_with_timeout.hpp"

#include "out.hpp"
#include "joe_query_iteration.hpp"
#include "joe_query.hpp"

namespace opossum {

std::ostream &operator<<(std::ostream &stream, const JoePlanSample &sample) {
  stream <<
    sample.hash << "," <<
    sample.execution_duration.count() << "," <<
    sample.est_cost << "," <<
    sample.re_est_cost << "," <<
    sample.aim_cost << "," <<
    sample.abs_est_cost_error << "," <<
    sample.abs_re_est_cost_error;
  return stream;
}

JoePlan::JoePlan(JoeQueryIteration& query_iteration, const std::shared_ptr<AbstractLQPNode>& lqp, const size_t idx):
  query_iteration(query_iteration), lqp(lqp), idx(idx)
{
  name = query_iteration.name + "-" + std::to_string(idx);
}

void JoePlan::run() {
  out() << "----- Evaluating plan" << std::endl;

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

  if (timed_out) {
    out() << "---- Query timed out" << std::endl;
    return;
  }

  /**
   * Evaluate plan execution
   */
  sample.execution_duration = timer.lap();
  const auto operators = flatten_pqp(pqp);
  create_cost_sample(operators);

  SQLQueryPlan plan;
  plan.add_tree_by_root(pqp);

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

  if (config->visualize) visualize(plan);
  if (query_iteration.query.save_plan_results) save_plan_result_table(plan);
}

void JoePlan::create_cost_sample(const std::vector<std::shared_ptr<AbstractOperator>> &operators) {
  auto& config = query_iteration.query.config;

  for (const auto &op : operators) {
    const auto aim_cost = config->cost_model->get_reference_operator_cost(op);
    sample.aim_cost += aim_cost;

    if (op->lqp_node()) {
      const auto est_cost = config->cost_model->estimate_cost(CostFeatureLQPNodeProxy(op->lqp_node()));
      sample.est_cost += est_cost;
      if (aim_cost) {
        sample.abs_est_cost_error += std::fabs(est_cost - aim_cost);
      }
    }

    const auto re_est_cost = config->cost_model->estimate_cost(CostFeatureOperatorProxy(op));
    sample.re_est_cost += re_est_cost;
    sample.abs_re_est_cost_error += std::fabs(re_est_cost - aim_cost);
  }
}

void JoePlan::visualize(const SQLQueryPlan& plan) {
  auto& config = query_iteration.query.config;

  GraphvizConfig graphviz_config;
  graphviz_config.format = "svg";
  VizGraphInfo viz_graph_info;
  viz_graph_info.bg_color = "black";

  try {
    SQLQueryPlanVisualizer visualizer{graphviz_config, viz_graph_info, {}, {}};
    visualizer.set_cost_model(config->cost_model);
    visualizer.visualize(plan, config->tmp_dot_file_path,
                         std::string(config->evaluation_dir + "/viz/") + name +
                         "_" + std::to_string(sample.execution_duration.count()) + ".svg");
  } catch (const std::exception &e) {
    out() << "----- Error while visualizing: " << e.what() << std::endl;
  }
}

void JoePlan::save_plan_result_table(const SQLQueryPlan& plan) {
  auto output_wrapper = std::make_shared<TableWrapper>(plan.tree_roots().at(0)->get_output());
  output_wrapper->execute();

  auto limit = std::make_shared<Limit>(output_wrapper, 500);
  limit->execute();

  std::ofstream output_file(query_iteration.query.sample.name + ".result.txt");
  output_file << "Total Row Count: " << plan.tree_roots().at(0)->get_output()->row_count() << std::endl;
  output_file << std::endl;
  Print::print(limit->get_output(), 0, output_file);

  query_iteration.query.save_plan_results = false;
}

}  // namespace opossum