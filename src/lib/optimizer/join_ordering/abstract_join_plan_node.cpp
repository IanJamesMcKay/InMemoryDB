#include "abstract_join_plan_node.hpp"

#include "join_graph.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "utils/print_directed_acyclic_graph.hpp"

namespace opossum {

AbstractJoinPlanNode::AbstractJoinPlanNode(
    const JoinPlanNodeType type)
    : _type(type) {}

JoinPlanNodeType AbstractJoinPlanNode::type() const { return _type; }

std::shared_ptr<TableStatistics> AbstractJoinPlanNode::statistics() const { return _statistics; }

float AbstractJoinPlanNode::node_cost() const {
  return _node_cost;
}

float AbstractJoinPlanNode::plan_cost() const {
  return _plan_cost;
}

std::shared_ptr<const AbstractJoinPlanNode> AbstractJoinPlanNode::left_child() const {
  return _left_child;
}

std::shared_ptr<const AbstractJoinPlanNode> AbstractJoinPlanNode::right_child() const {
  return _right_child;
}

void AbstractJoinPlanNode::print(std::ostream& stream) const {
  const auto get_children_fn = [](const auto& node) {
    std::vector<std::shared_ptr<const AbstractJoinPlanNode>> children;
    if (node->_left_child) children.emplace_back(node->_left_child);
    if (node->_right_child) children.emplace_back(node->_right_child);
    return children;
  };
  const auto node_print_fn = [](const auto& node, auto& stream) {
    stream << "NodeCost=" << node->node_cost() << "; PlanCost=" << node->plan_cost() << "; RowCount=" << node->statistics()->row_count() << "; "
           << node->description();
  };

  print_directed_acyclic_graph<const AbstractJoinPlanNode>(shared_from_this(), get_children_fn, node_print_fn, stream);
}

JoinPlanPredicateEstimate AbstractJoinPlanNode::_estimate_predicate(
    const std::shared_ptr<const AbstractJoinPlanPredicate>& predicate,
    const std::shared_ptr<TableStatistics>& statistics) const {
  switch (predicate->type()) {
    case JoinPlanPredicateType::Atomic: {
      const auto atomic_predicate = std::static_pointer_cast<const JoinPlanAtomicPredicate>(predicate);

      const auto left_column_id = find_column_id(atomic_predicate->left_operand);
      DebugAssert(left_column_id, "Couldn't resolve " + atomic_predicate->left_operand.description());

      std::shared_ptr<TableStatistics> table_statistics;

      if (is_lqp_column_reference(atomic_predicate->right_operand)) {
        const auto right_operand_column_reference = boost::get<LQPColumnReference>(atomic_predicate->right_operand);
        const auto right_column_id = find_column_id(right_operand_column_reference);
        DebugAssert(right_column_id, "Couldn't resolve " + right_operand_column_reference.description());

        table_statistics =
            statistics->predicate_statistics(*left_column_id, atomic_predicate->predicate_condition, *right_column_id);
      } else {
        table_statistics = statistics->predicate_statistics(*left_column_id, atomic_predicate->predicate_condition,
                                                            atomic_predicate->right_operand);
      }

      return {statistics->row_count(), table_statistics};
    }
    case JoinPlanPredicateType::LogicalOperator: {
      const auto logical_operator_predicate =
          std::static_pointer_cast<const JoinPlanLogicalPredicate>(predicate);
      const auto left_operand_estimate = _estimate_predicate(logical_operator_predicate->left_operand, statistics);

      switch (logical_operator_predicate->logical_operator) {
        case JoinPlanPredicateLogicalOperator::And: {
          const auto right_operand_estimate =
              _estimate_predicate(logical_operator_predicate->right_operand, left_operand_estimate.statistics);
          return {left_operand_estimate.cost + right_operand_estimate.cost, right_operand_estimate.statistics};
        }
        case JoinPlanPredicateLogicalOperator::Or: {
          const auto right_operand_estimate =
              _estimate_predicate(logical_operator_predicate->right_operand, statistics);
          const auto post_left_operand_row_count = left_operand_estimate.statistics->row_count();
          const auto post_right_operand_row_count = right_operand_estimate.statistics->row_count();

          auto union_costs = 0.0f;

          if (post_left_operand_row_count > 0.0f && post_right_operand_row_count > 0.0f) {
            // Union costs ~= sort left and sort right, then iterate over both to merge
            // if one side has no row, the Union operator can just forward - and we avoid NaNs caused by std::log(0)
            union_costs = post_left_operand_row_count * std::log(post_left_operand_row_count) +
                          post_right_operand_row_count * std::log(post_right_operand_row_count) +
                          post_left_operand_row_count + post_right_operand_row_count;
          }

          const auto union_statistics =
              left_operand_estimate.statistics->generate_disjunction_statistics(right_operand_estimate.statistics);

          return {left_operand_estimate.cost + right_operand_estimate.cost + union_costs, union_statistics};
        }
      }
    }
  }

  Fail("Should be unreachable, but clang doesn't realize");
  return {0.0f, nullptr};
}

std::shared_ptr<AbstractLQPNode> AbstractJoinPlanNode::_insert_predicate(
    const std::shared_ptr<AbstractLQPNode>& lqp,
    const std::shared_ptr<const AbstractJoinPlanPredicate>& predicate) const {
  switch (predicate->type()) {
    case JoinPlanPredicateType::Atomic: {
      const auto atomic_predicate = std::static_pointer_cast<const JoinPlanAtomicPredicate>(predicate);
      const auto predicate_node = std::make_shared<PredicateNode>(
          atomic_predicate->left_operand, atomic_predicate->predicate_condition, atomic_predicate->right_operand);
      predicate_node->set_left_input(lqp);
      return predicate_node;
    }
    case JoinPlanPredicateType::LogicalOperator: {
      const auto logical_operator_predicate =
          std::static_pointer_cast<const JoinPlanLogicalPredicate>(predicate);

      switch (logical_operator_predicate->logical_operator) {
        case JoinPlanPredicateLogicalOperator::And: {
          const auto post_left_predicate = _insert_predicate(lqp, logical_operator_predicate->left_operand);
          return _insert_predicate(post_left_predicate, logical_operator_predicate->right_operand);
        }
        case JoinPlanPredicateLogicalOperator::Or: {
          const auto post_left_predicate = _insert_predicate(lqp, logical_operator_predicate->left_operand);
          const auto post_right_predicate = _insert_predicate(lqp, logical_operator_predicate->right_operand);

          const auto union_node = std::make_shared<UnionNode>(UnionMode::Positions);
          union_node->set_left_input(post_left_predicate);
          union_node->set_right_input(post_right_predicate);

          return union_node;
        }
      }
    }
  }

  Fail("Should be unreachable, but clang doesn't realize");
  return nullptr;
}

void AbstractJoinPlanNode::_order_predicates(std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates, const std::shared_ptr<TableStatistics>& statistics) const {
  const auto sort_predicate = [&](auto& left, auto& right) {
    return _estimate_predicate(left, statistics).statistics->row_count() < _estimate_predicate(right, statistics).statistics->row_count();
  };

  std::sort(predicates.begin(), predicates.end(), sort_predicate);
}

}  // namespace opossum
