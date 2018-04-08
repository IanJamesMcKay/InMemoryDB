#include "build_join_plan.hpp"

#include <algorithm>

#include "abstract_cost_model.hpp"
#include "join_plan_join_node.hpp"
#include "join_plan_predicate.hpp"
#include "join_plan_vertex_node.hpp"
#include "optimizer/table_statistics.hpp"
#include "optimizer/table_statistics_cache.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "utils/assert.hpp"

namespace {

using namespace opossum;  // NOLINT

JoinPlanNode add_predicate(const AbstractJoinPlanPredicate& predicate,
                                JoinPlanNode join_plan_node, const AbstractCostModel& cost_model, const TableStatisticsCache& statistics_cache) {
  switch (predicate.type()) {
    case JoinPlanPredicateType::Atomic: {
      const auto& atomic_predicate = static_cast<const JoinPlanAtomicPredicate&>(predicate);

      join_plan_node.lqp = PredicateNode::make(atomic_predicate.left_operand,
                                               atomic_predicate.predicate_condition,
                                               atomic_predicate.right_operand,
                                               join_plan_node.lqp);

      const auto predicate_cost = cost_model.estimate_lqp_node_cost(join_plan_node.lqp);
      join_plan_node.plan_cost += predicate_cost;

      return join_plan_node;
    }
    case JoinPlanPredicateType::LogicalOperator: {
      const auto& logical_operator_predicate = static_cast<const JoinPlanLogicalPredicate&>(predicate);
      const auto left_operand_join_plan = add_predicate(*logical_operator_predicate.left_operand, join_plan_node, cost_model, statistics_cache);
      const auto left_operand_added_cost = left_operand_join_plan.plan_cost - join_plan_node.plan_cost;

      switch (logical_operator_predicate.logical_operator) {
        case JoinPlanPredicateLogicalOperator::And: {
          return add_predicate(*logical_operator_predicate.right_operand, left_operand_join_plan, cost_model, statistics_cache);
        }
        case JoinPlanPredicateLogicalOperator::Or: {
          auto right_operand_join_plan = add_predicate(*logical_operator_predicate.right_operand, join_plan_node, cost_model, statistics_cache);
          const auto right_operand_added_cost = right_operand_join_plan.plan_cost - join_plan_node.plan_cost;

          join_plan_node.lqp = UnionNode::make(UnionMode::Positions, left_operand_join_plan.lqp, right_operand_join_plan.lqp);

          const auto union_cost = cost_model.estimate_lqp_node_cost(join_plan_node.lqp);
          join_plan_node.plan_cost += union_cost + left_operand_added_cost + right_operand_added_cost;
          return join_plan_node;
        }
      }
    }
  }

  Fail("Should be unreachable, but clang doesn't realize");
}

void order_predicates(std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates,
                      const JoinPlanNode& join_plan_node, const AbstractCostModel& cost_model, const TableStatisticsCache& statistics_cache) {
  const auto sort_predicate = [&](auto& left, auto& right) {
    return statistics_cache.get(add_predicate(*left, join_plan_node, cost_model, statistics_cache).lqp)->row_count() <
    statistics_cache.get(add_predicate(*right, join_plan_node, cost_model, statistics_cache).lqp)->row_count();
  };

  std::sort(predicates.begin(), predicates.end(), sort_predicate);
}

}  // namespace

namespace opossum {

JoinPlanNode build_join_plan_join_node(
    const AbstractCostModel& cost_model,
    const JoinPlanNode& left_input,
    const JoinPlanNode& right_input,
    const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates,
    const TableStatisticsCache& statistics_cache) {
  JoinPlanNode join_plan_node{nullptr, left_input.plan_cost + right_input.plan_cost};

  auto primary_join_predicate = std::shared_ptr<const JoinPlanAtomicPredicate>{};
  auto secondary_predicates = predicates;

  /**
   * Find primary join predicate - needs to be atomic and have one argument in the right and one in the left sub plan
   */
  const auto iter = std::find_if(secondary_predicates.begin(), secondary_predicates.end(), [&](const auto& predicate) {
    if (predicate->type() != JoinPlanPredicateType::Atomic) return false;
    const auto atomic_predicate = std::static_pointer_cast<const JoinPlanAtomicPredicate>(predicate);
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
    primary_join_predicate = std::static_pointer_cast<const JoinPlanAtomicPredicate>(*iter);

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
  const auto left_statistics = statistics_cache.get(left_input.lqp);
  const auto right_statistics = statistics_cache.get(right_input.lqp);
  auto lqp = std::shared_ptr<AbstractLQPNode>();

  // Compute cost&statistics of primary join predicate, if it exists. Otherwise assume a cross join.
  if (!primary_join_predicate) {
    join_plan_node.lqp = JoinNode::make(JoinMode::Cross, left_input.lqp, right_input.lqp);
  } else {
    const auto right_operand_column_reference = boost::get<LQPColumnReference>(primary_join_predicate->right_operand);

    join_plan_node.lqp = JoinNode::make(JoinMode::Inner,
                                        std::make_pair(primary_join_predicate->left_operand,
                                                       right_operand_column_reference),
                                        primary_join_predicate->predicate_condition, left_input.lqp, right_input.lqp);
  }

  const auto join_cost = cost_model.estimate_lqp_node_cost(join_plan_node.lqp);
  join_plan_node.plan_cost += join_cost;
  order_predicates(secondary_predicates, join_plan_node, cost_model, statistics_cache);

  // Apply remaining predicates
  for (const auto& predicate : secondary_predicates) {
    join_plan_node = add_predicate(*predicate, join_plan_node, cost_model, statistics_cache);
  }

  return join_plan_node;
}

JoinPlanNode build_join_plan_vertex_node(
    const AbstractCostModel& cost_model,
    const std::shared_ptr<AbstractLQPNode>& vertex_node,
    std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> predicates, const TableStatisticsCache& statistics_cache) {
  auto join_plan_node = JoinPlanNode{vertex_node, 0.0f};
  order_predicates(predicates, join_plan_node, cost_model, statistics_cache);

  for (const auto& predicate : predicates) {
    join_plan_node = add_predicate(*predicate, join_plan_node, cost_model, statistics_cache);
  }

  return join_plan_node;
}

}  // namespace opossum
