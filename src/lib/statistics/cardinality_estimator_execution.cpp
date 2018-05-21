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
#include "utils/execute_with_timeout.hpp"

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

  lqp = _optimizer->optimize(lqp);

  std::cout << "CardinalityEstimatorExecution: " << std::endl;
  lqp->print();

  const auto pqp = LQPTranslator{}.translate_node(lqp);

  std::cout << " Executing..." << std::endl;

  const auto timed_out = execute_with_timeout(pqp, timeout);

  if (timed_out) {
    std::cout << " Timed out" << std::endl;
    return 1000.f * 1000.f * 1000.f * 1000.f;
  } else {
    std::cout << " Returned " << pqp->get_output()->row_count() << " rows" << std::endl;
    return pqp->get_output()->row_count();
  }
}

}  // namespace opossum
