#include "cardinality_estimator_execution.hpp"

#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "cost_model/cost_model_naive.hpp"
#include "optimizer/join_ordering/build_lqp_for_predicate.hpp"
#include "optimizer/optimizer.hpp"
#include "optimizer/join_ordering/dp_ccp.hpp"
#include "optimizer/strategy/join_ordering_rule.hpp"
#include "optimizer/strategy/rule_batch.hpp"
#include "operators/abstract_operator.hpp"
#include "storage/table.hpp"
#include "scheduler/current_scheduler.hpp"
#include "sql/sql_query_plan.hpp"

namespace opossum {

CardinalityEstimatorExecution::CardinalityEstimatorExecution() {
  _optimizer = std::make_shared<Optimizer>();

  const auto cost_model = std::make_shared<CostModelNaive>();
  const auto internal_cardinality_estimator = std::make_shared<CardinalityEstimatorColumnStatistics>();

  const auto dp_ccp = std::make_shared<DpCcp>(cost_model, internal_cardinality_estimator);

  RuleBatch rule_batch(RuleBatchExecutionPolicy::Once);
  rule_batch.add_rule(std::make_shared<JoinOrderingRule>(dp_ccp));

  _optimizer->add_rule_batch(rule_batch);
}

Cardinality CardinalityEstimatorExecution::estimate(const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices,
                                                    const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates) const {
  if (vertices.empty()) return 0.0f;

  auto lqp = vertices.front();

  for (auto vertex_idx = size_t{1}; vertex_idx < vertices.size(); ++vertex_idx) {
    lqp = JoinNode::make(JoinMode::Cross, lqp, vertices[vertex_idx]);
  }

  for (const auto& predicate : predicates) {
    lqp = build_lqp_for_predicate(*predicate, lqp);
  }

  lqp->print();
  lqp = _optimizer->optimize(lqp);

  std::cout << "CardinalityEstimatorExecution: " << std::endl;
  lqp->print();

  const auto pqp = LQPTranslator{}.translate_node(lqp);

  SQLQueryPlan sql_query_plan;
  sql_query_plan.add_tree_by_root(pqp);

  const auto tasks = sql_query_plan.create_tasks();
  std::cout << " Executing..." << std::endl;
  CurrentScheduler::schedule_and_wait_for_tasks(tasks);
  std::cout << " Returned " << pqp->get_output()->row_count() << " rows" << std::endl;

  return pqp->get_output()->row_count();
}

}  // namespace opossum
