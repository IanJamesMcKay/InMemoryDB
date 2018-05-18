#pragma once

#include <chrono>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "sql/sql_query_plan.hpp"

namespace opossum {

//
//struct JoinPlanState final {
//  size_t idx{0};
//  JoinPlanNode join_plan;
//};
//
//std::ostream &operator<<(std::ostream &stream, const PlanMeasurement &sample) {
//  stream << sample.execution_duration << "," << sample.est_cost << "," << sample.re_est_cost << "," << sample.aim_cost << ","
//         << sample.abs_est_cost_error << "," << sample.abs_re_est_cost_error;
//  return stream;
//}
//
//
//void evaluate_join_plan(QueryState& query_state,
//                        QueryIterationState& query_iteration_state,
//                        JoinPlanState& join_plan_state,
//                        const std::shared_ptr<AbstractCostModel>& cost_model) {
//
//  /**
//   * CSV
//   */
//  if (config.save_query_iterations_results) {
//    auto csv = std::ofstream{evaluation_dir + "/" + query_iteration_state.name + ".csv"};
//    csv << "Idx,Duration,EstCost,ReEstCost,AimCost,AbsEstCostError,AbsReEstCostError" << "\n";
//    for (auto plan_idx = size_t{0}; plan_idx < query_iteration_state.measurements.size(); ++plan_idx) {
//      csv << plan_idx << "," << query_iteration_state.measurements[plan_idx] << "\n";
//    }
//    csv.close();
//  }
//}

class AbstractOperator;
class JoeQueryIteration;

struct JoePlanSample final {
  size_t hash{0};
  std::chrono::microseconds execution_duration{0};
  Cost est_cost{0.0f};
  Cost re_est_cost{0.0f};
  Cost aim_cost{0.0f};
  Cost abs_est_cost_error{0.0f};
  Cost abs_re_est_cost_error{0.0f};
};

std::ostream &operator<<(std::ostream &stream, const JoePlanSample &sample);

class JoePlan final {
 public:
  JoePlan(JoeQueryIteration& query_iteration, const std::shared_ptr<AbstractLQPNode>& lqp, const size_t idx);

  void run();

  JoeQueryIteration& query_iteration;
  std::shared_ptr<AbstractLQPNode> lqp;
  const size_t idx;

  std::string name;
  JoePlanSample sample;

  void create_cost_sample(const std::vector<std::shared_ptr<AbstractOperator>> &operators);
  void visualize(const SQLQueryPlan& plan);
  void save_plan_result_table(const SQLQueryPlan& plan);
};

}  // namespace opossum
