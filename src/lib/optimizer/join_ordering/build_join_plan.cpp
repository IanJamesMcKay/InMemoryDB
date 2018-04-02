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

struct JoinPlanPredicateEstimate final {
  JoinPlanPredicateEstimate(const float cost, const std::shared_ptr<AbstractLQPNode>& lqp)
  : cost(cost), lqp(lqp) {
    DebugAssert(cost >= 0.0f && !std::isnan(cost), "Invalid cost");
  }

  const float cost;
  std::shared_ptr<AbstractLQPNode> lqp;
};

JoinPlanPredicateEstimate estimate_predicate(const AbstractJoinPlanPredicate& predicate,
std::shared_ptr<AbstractLQPNode> lqp, const AbstractCostModel& cost_model, const TableStatisticsCache& statistics_cache) {
  switch (predicate.type()) {
    case JoinPlanPredicateType::Atomic: {
      const auto& atomic_predicate = static_cast<const JoinPlanAtomicPredicate&>(predicate);

      const auto left_column_id = lqp->find_output_column_id(atomic_predicate.left_operand);
      DebugAssert(left_column_id, "Couldn't resolve " + atomic_predicate.left_operand.description());

      auto cost = Cost{0.0f};

      lqp = PredicateNode::make(atomic_predicate.left_operand, atomic_predicate.predicate_condition, atomic_predicate.right_operand, lqp);

      if (is_lqp_column_reference(atomic_predicate.right_operand)) {
        const auto right_operand_column_reference = boost::get<LQPColumnReference>(atomic_predicate.right_operand);
        const auto right_column_id = lqp->find_output_column_id(right_operand_column_reference);
        DebugAssert(right_column_id, "Couldn't resolve " + right_operand_column_reference.description());

        cost = cost_model.cost_table_scan(statistics_cache.get(lqp), *left_column_id, atomic_predicate.predicate_condition, *right_column_id);
      } else {
        Assert(atomic_predicate.right_operand.type() == typeid(AllTypeVariant), "Unexpected type");

        const auto right_operand_value = boost::get<AllTypeVariant>(atomic_predicate.right_operand);

        cost = cost_model.cost_table_scan(statistics_cache.get(lqp), *left_column_id, atomic_predicate.predicate_condition, right_operand_value);
      }

      return {cost, lqp};
    }
    case JoinPlanPredicateType::LogicalOperator: {
      const auto& logical_operator_predicate = static_cast<const JoinPlanLogicalPredicate&>(predicate);
      const auto left_operand_estimate = estimate_predicate(*logical_operator_predicate.left_operand, lqp, cost_model, statistics_cache);

      switch (logical_operator_predicate.logical_operator) {
        case JoinPlanPredicateLogicalOperator::And: {
          const auto right_operand_estimate =
          estimate_predicate(*logical_operator_predicate.right_operand, left_operand_estimate.lqp, cost_model, statistics_cache);
          return {left_operand_estimate.cost + right_operand_estimate.cost, right_operand_estimate.lqp};
        }
        case JoinPlanPredicateLogicalOperator::Or: {
          const auto right_operand_estimate =
          estimate_predicate(*logical_operator_predicate.right_operand, lqp, cost_model, statistics_cache);

          auto union_costs = cost_model.cost_union_positions(statistics_cache.get(left_operand_estimate.lqp), statistics_cache.get(right_operand_estimate.lqp));

          lqp = UnionNode::make(UnionMode::Positions, left_operand_estimate.lqp, right_operand_estimate.lqp);
          return {left_operand_estimate.cost + right_operand_estimate.cost + union_costs, lqp};
        }
      }
    }
  }

  Fail("Should be unreachable, but clang doesn't realize");
  return {0.0f, nullptr};
}

void order_predicates(std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates,
                                             const std::shared_ptr<AbstractLQPNode>& lqp, const AbstractCostModel& cost_model, const TableStatisticsCache& statistics_cache) {
  const auto sort_predicate = [&](auto& left, auto& right) {
    return statistics_cache.get(estimate_predicate(*left, lqp, cost_model, statistics_cache).lqp)->row_count() <
    statistics_cache.get(estimate_predicate(*right, lqp, cost_model, statistics_cache).lqp)->row_count();
  };

  std::sort(predicates.begin(), predicates.end(), sort_predicate);
}

}  // namespace

namespace opossum {

std::shared_ptr<JoinPlanJoinNode> build_join_plan_join_node(
    const AbstractCostModel& cost_model, const std::shared_ptr<const AbstractJoinPlanNode>& left_child,
    const std::shared_ptr<const AbstractJoinPlanNode>& right_child,
    const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates, const TableStatisticsCache& statistics_cache) {

  auto primary_join_predicate = std::shared_ptr<const JoinPlanAtomicPredicate>{};
  auto secondary_predicates = predicates;
  auto node_cost = 0.0f;
  auto statistics = std::shared_ptr<TableStatistics>{};

  /**
   * Find primary join predicate - needs to be atomic and have one argument in the right and one in the left sub plan
   */
  const auto iter = std::find_if(secondary_predicates.begin(), secondary_predicates.end(), [&](const auto& predicate) {
    if (predicate->type() != JoinPlanPredicateType::Atomic) return false;
    const auto atomic_predicate = std::static_pointer_cast<const JoinPlanAtomicPredicate>(predicate);
    if (!is_lqp_column_reference(atomic_predicate->right_operand)) return false;

    const auto right_operand_column_reference = boost::get<LQPColumnReference>(atomic_predicate->right_operand);

    const auto left_operand_in_left_child = left_child->find_column_id(atomic_predicate->left_operand);
    const auto left_operand_in_right_child = right_child->find_column_id(atomic_predicate->left_operand);
    const auto right_operand_in_left_child = left_child->find_column_id(right_operand_column_reference);
    const auto right_operand_in_right_child = right_child->find_column_id(right_operand_column_reference);

    return (left_operand_in_left_child && right_operand_in_right_child) ||
           (left_operand_in_right_child && right_operand_in_left_child);
  });

  if (iter != secondary_predicates.end()) {
    primary_join_predicate = std::static_pointer_cast<const JoinPlanAtomicPredicate>(*iter);

    auto left_operand = primary_join_predicate->left_operand;
    auto predicate_condition = primary_join_predicate->predicate_condition;
    auto right_operand = boost::get<LQPColumnReference>(primary_join_predicate->right_operand);

    if (!left_child->find_column_id(left_operand)) {
      std::swap(left_operand, right_operand);
      predicate_condition = flip_predicate_condition(predicate_condition);
    }
    DebugAssert(left_child->find_column_id(left_operand) && right_child->find_column_id(right_operand),
                "Predicate not applicable to sub plans");

    primary_join_predicate =
        std::make_shared<JoinPlanAtomicPredicate>(left_operand, predicate_condition, right_operand);

    secondary_predicates.erase(iter);
  }

  /**
   * Compute costs and statistics
   */
  const auto left_statistics = statistics_cache.get(left_child->to_lqp());
  const auto right_statistics = statistics_cache.get(right_child->to_lqp());
  auto lqp = std::shared_ptr<AbstractLQPNode>();

  // Compute cost&statistics of primary join predicate, if it exists. Otherwise assume a cross join.
  if (!primary_join_predicate) {
    lqp = JoinNode::make(JoinMode::Cross, left_child->to_lqp(), right_child->to_lqp());
    node_cost = cost_model.cost_product(left_statistics, right_statistics);
  } else {
    const auto left_column_id = left_child->find_column_id(primary_join_predicate->left_operand);
    DebugAssert(left_column_id, "Couldn't resolve " + primary_join_predicate->left_operand.description());

    const auto right_operand_column_reference = boost::get<LQPColumnReference>(primary_join_predicate->right_operand);
    const auto right_column_id = right_child->find_column_id(right_operand_column_reference);
    DebugAssert(right_column_id, "Couldn't resolve " + right_operand_column_reference.description());

    // If scan type is equals, assume HashJoin can be used, otherwise its SortMergeJoin
    if (primary_join_predicate->predicate_condition == PredicateCondition::Equals) {
      node_cost +=
          cost_model.cost_join_hash(left_statistics, right_statistics, JoinMode::Inner,
                                    {*left_column_id, *right_column_id}, primary_join_predicate->predicate_condition);
    } else {
      node_cost += cost_model.cost_join_sort_merge(left_statistics, right_statistics, JoinMode::Inner,
                                                   {*left_column_id, *right_column_id},
                                                   primary_join_predicate->predicate_condition);
    }

    lqp = JoinNode::make(JoinMode::Inner, std::make_pair(primary_join_predicate->left_operand, right_operand_column_reference), primary_join_predicate->predicate_condition, left_child->to_lqp(), right_child->to_lqp());
  }

  order_predicates(secondary_predicates, lqp, cost_model, statistics_cache);

  // Apply remaining predicates
  for (const auto& predicate : secondary_predicates) {
    const auto predicate_estimate = estimate_predicate(*predicate, lqp, cost_model, statistics_cache);

    node_cost += predicate_estimate.cost;
    lqp = predicate_estimate.lqp;
  }

  return std::make_shared<JoinPlanJoinNode>(left_child, right_child, statistics_cache.get(lqp), primary_join_predicate, secondary_predicates,
                                            node_cost);
}

std::shared_ptr<JoinPlanVertexNode> build_join_plan_vertex_node(
    const AbstractCostModel& cost_model,
    const std::shared_ptr<AbstractLQPNode>& vertex_node,
    std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> predicates, const TableStatisticsCache& statistics_cache) {
  auto lqp = vertex_node;

  order_predicates(predicates, lqp, cost_model, statistics_cache);

  auto node_cost = 0.0f;

  for (const auto& predicate : predicates) {
    const auto predicate_estimate = estimate_predicate(*predicate, lqp, cost_model, statistics_cache);

    node_cost += predicate_estimate.cost;
    lqp = predicate_estimate.lqp;
  }

  return std::make_shared<JoinPlanVertexNode>(vertex_node, predicates, statistics_cache.get(lqp), node_cost);
}

}  // namespace opossum
