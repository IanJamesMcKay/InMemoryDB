#include "build_join_plan.hpp"

#include <algorithm>

#include "cost_model/cost_lqp.hpp"
#include "cost_model/abstract_cost_model.hpp"
#include "cost_model/cost_feature_join_plan_proxy.hpp"
#include "join_plan_join_node.hpp"
#include "join_plan_predicate.hpp"
#include "join_plan_vertex_node.hpp"
#include "statistics/abstract_cardinality_estimator.hpp"
#include "statistics/table_statistics.hpp"
#include "optimizer/table_statistics_cache.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "utils/assert.hpp"
#include "build_lqp_for_predicate.hpp"

namespace {

using namespace opossum;  // NOLINT

Cost cost_predicate(const std::shared_ptr<AbstractJoinPlanPredicate>& predicate,
                                                BaseJoinGraph join_graph,
                                                const AbstractCostModel& cost_model,
                                                const AbstractCardinalityEstimator& cardinality_estimator) {
  switch (predicate->type()) {
    case JoinPlanPredicateType::Atomic: {
      const auto atomic_predicate = std::static_pointer_cast<JoinPlanAtomicPredicate>(predicate);

      const auto cost_proxy = CostFeatureGenericProxy::from_join_plan_predicate(atomic_predicate, join_graph, cardinality_estimator);
      return cost_model.estimate_cost(cost_proxy);
    }
    case JoinPlanPredicateType::LogicalOperator: {
      const auto& logical_operator_predicate = static_cast<const JoinPlanLogicalPredicate&>(*predicate);

      const auto left_cost = cost_predicate(logical_operator_predicate.left_operand, join_graph, cost_model, cardinality_estimator);

      switch (logical_operator_predicate.logical_operator) {
        case JoinPlanPredicateLogicalOperator::And: {
          join_graph.predicates.emplace_back(logical_operator_predicate.left_operand);
          return left_cost + cost_predicate(logical_operator_predicate.right_operand, join_graph, cost_model, cardinality_estimator);
        }
        case JoinPlanPredicateLogicalOperator::Or: {
          const auto right_cost = cost_predicate(logical_operator_predicate.right_operand, join_graph, cost_model, cardinality_estimator);

          auto left_join_graph = join_graph;
          left_join_graph.predicates.emplace_back(logical_operator_predicate.left_operand);

          auto right_join_graph = join_graph;
          right_join_graph.predicates.emplace_back(logical_operator_predicate.right_operand);

          auto output_join_graph = join_graph;
          output_join_graph.predicates.emplace_back(predicate);

          const auto union_cost = cost_model.estimate_cost(CostFeatureGenericProxy::from_union(output_join_graph, left_join_graph, right_join_graph, cardinality_estimator));

          return left_cost + right_cost + union_cost;
        }
      }
    }
  }

  Fail("Should be unreachable, but clang doesn't realize");
}

void add_predicate(const std::shared_ptr<AbstractJoinPlanPredicate>& predicate,
                   JoinPlan& join_plan_node,
                   const AbstractCostModel& cost_model,
                   const AbstractCardinalityEstimator& cardinality_estimator) {
//  Assert(join_plan_node.lqp->get_statistics()->row_count() == cardinality_estimator.estimate(join_plan_node.join_graph.vertices, join_plan_node.join_graph.predicates), "Row counts are diverged");
//  Assert(cost_lqp(join_plan_node.lqp, cost_model) == join_plan_node.plan_cost, "Costs are diverged!");

//  std::cout << "Before" << std::endl;
//  join_plan_node.lqp->print();
//  std::cout << "/Before" << std::endl;

  join_plan_node.lqp = build_lqp_for_predicate(*predicate, join_plan_node.lqp);
  join_plan_node.plan_cost += cost_predicate(predicate, join_plan_node.join_graph, cost_model, cardinality_estimator);

//  std::cout << "After" << std::endl;
//  join_plan_node.lqp->print();
//  std::cout << "/After" << std::endl;

  join_plan_node.join_graph.predicates.emplace_back(predicate);

//  Assert(join_plan_node.lqp->get_statistics()->row_count() == cardinality_estimator.estimate(join_plan_node.join_graph.vertices, join_plan_node.join_graph.predicates), "Row counts are diverged");
//  Assert(cost_lqp(join_plan_node.lqp, cost_model) == join_plan_node.plan_cost, "Costs have diverged!");

}

//void order_predicates(std::vector<std::shared_ptr<AbstractJoinPlanPredicate>>& predicates,
//                      const JoinPlan& join_plan_node,
//                      const AbstractCostModel& cost_model,
//                      const AbstractCardinalityEstimator& cardinality_estimator) {
//  const auto sort_predicate = [&](auto& left, auto& right) {
//    BaseJoin
//
//    return cardinality_estimator.estimate(add_predicate(*left, join_plan_node, cost_model, statistics_cache).lqp)->row_count() <
//    statistics_cache.get(add_predicate(*right, join_plan_node, cost_model, statistics_cache).lqp)->row_count();
//  };
//
//  std::sort(predicates.begin(), predicates.end(), sort_predicate);
//}

}  // namespace

namespace opossum {

JoinPlan build_join_plan_join_node(
    const AbstractCostModel& cost_model,
    const JoinPlan& left_input,
    const JoinPlan& right_input,
    const std::vector<std::shared_ptr<AbstractJoinPlanPredicate>>& predicates,
    const AbstractCardinalityEstimator& cardinality_estimator) {
  JoinPlan join_plan_node{nullptr,
                              left_input.plan_cost + right_input.plan_cost,
                              BaseJoinGraph::from_joined_graphs(left_input.join_graph, right_input.join_graph)};

  auto primary_join_predicate = std::shared_ptr<JoinPlanAtomicPredicate>{};
  auto secondary_predicates = predicates;

  /**
   * Find primary join predicate - needs to be atomic and have one argument in the right and one in the left sub plan
   */
  const auto iter = std::find_if(secondary_predicates.begin(), secondary_predicates.end(), [&](const auto& predicate) {
    if (predicate->type() != JoinPlanPredicateType::Atomic) return false;
    const auto atomic_predicate = std::static_pointer_cast<JoinPlanAtomicPredicate>(predicate);
    if (!is_lqp_column_reference(atomic_predicate->right_operand)) return false;

    const auto right_operand_column_reference = boost::get<LQPColumnReference>(atomic_predicate->right_operand);

    const auto left_operand_in_left_child = left_input.lqp->find_output_column_id(atomic_predicate->left_operand);
    const auto left_operand_in_right_child = right_input.lqp->find_output_column_id(atomic_predicate->left_operand);
    const auto right_operand_in_left_child = left_input.lqp->find_output_column_id(right_operand_column_reference);
    const auto right_operand_in_right_child = right_input.lqp->find_output_column_id(right_operand_column_reference);

    return (left_operand_in_left_child && right_operand_in_right_child) ||
           (left_operand_in_right_child && right_operand_in_left_child);
  });

  if (iter != secondary_predicates.end()) {
    primary_join_predicate = std::static_pointer_cast<JoinPlanAtomicPredicate>(*iter);

    auto left_operand = primary_join_predicate->left_operand;
    auto predicate_condition = primary_join_predicate->predicate_condition;
    auto right_operand = boost::get<LQPColumnReference>(primary_join_predicate->right_operand);

    if (!left_input.lqp->find_output_column_id(left_operand)) {
      std::swap(left_operand, right_operand);
      predicate_condition = flip_predicate_condition(predicate_condition);
    }
    DebugAssert(left_input.lqp->find_output_column_id(left_operand) && right_input.lqp->find_output_column_id(right_operand),
                "Predicate not applicable to sub plans");

    primary_join_predicate =
        std::make_shared<JoinPlanAtomicPredicate>(left_operand, predicate_condition, right_operand);

    secondary_predicates.erase(iter);
  }

  /**
   * Compute costs and statistics
   */
  auto lqp = std::shared_ptr<AbstractLQPNode>();

  // Compute cost&statistics of primary join predicate, if it exists. Otherwise assume a cross join.
  CostFeatureGenericProxy cost_feature_proxy;

  if (!primary_join_predicate) {
    join_plan_node.lqp = JoinNode::make(JoinMode::Cross, left_input.lqp, right_input.lqp);

    cost_feature_proxy = CostFeatureGenericProxy::from_cross_join(left_input.join_graph, right_input.join_graph, cardinality_estimator);
  } else {
    const auto right_operand_column_reference = boost::get<LQPColumnReference>(primary_join_predicate->right_operand);

    join_plan_node.lqp = JoinNode::make(JoinMode::Inner,
                                        std::make_pair(primary_join_predicate->left_operand,
                                                       right_operand_column_reference),
                                        primary_join_predicate->predicate_condition, left_input.lqp, right_input.lqp);

    cost_feature_proxy = CostFeatureGenericProxy::from_join_plan_predicate(primary_join_predicate, left_input.join_graph, right_input.join_graph, cardinality_estimator);

    join_plan_node.join_graph.predicates.emplace_back(primary_join_predicate);
  }

  join_plan_node.plan_cost += cost_model.estimate_cost(cost_feature_proxy);

//  join_plan_node.lqp->print();

//  Assert(join_plan_node.lqp->get_statistics()->row_count() == cardinality_estimator.estimate(join_plan_node.join_graph.vertices, join_plan_node.join_graph.predicates), "Row counts are diverged");
//  Assert(cost_lqp(join_plan_node.lqp, cost_model) == join_plan_node.plan_cost, "Costs have diverged!");

//  order_predicates(secondary_predicates, join_plan_node, cost_model, cardinality_estimator);

  // Apply remaining predicates
  for (const auto& predicate : secondary_predicates) {
    add_predicate(predicate, join_plan_node, cost_model, cardinality_estimator);
  }

  return join_plan_node;
}

JoinPlan build_join_plan_vertex_node(
    const AbstractCostModel& cost_model,
    const std::shared_ptr<AbstractLQPNode>& vertex_node,
    const std::vector<std::shared_ptr<AbstractJoinPlanPredicate>>& predicates,
    const AbstractCardinalityEstimator& cardinality_estimator) {
  auto join_plan_node = JoinPlan{vertex_node, 0.0f, {{vertex_node}, {}}};

//  order_predicates(predicates, join_plan_node, cost_model, cardinality_estimator);

  for (const auto& predicate : predicates) {
    add_predicate(predicate, join_plan_node, cost_model, cardinality_estimator);
  }

  return join_plan_node;
}

}  // namespace opossum
