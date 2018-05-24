#include "abstract_join_plan_node.hpp"

#include "join_graph.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "utils/print_directed_acyclic_graph.hpp"

namespace opossum {

AbstractJoinPlanNode::AbstractJoinPlanNode(const JoinPlanNodeType type, const Cost node_cost,
                                           const std::shared_ptr<TableStatistics>& statistics,
                                           const std::shared_ptr<const AbstractJoinPlanNode>& left_child,
                                           const std::shared_ptr<const AbstractJoinPlanNode>& right_child)
    : _type(type), _node_cost(node_cost), _statistics(statistics), _left_child(left_child), _right_child(right_child) {}

JoinPlanNodeType AbstractJoinPlanNode::type() const { return _type; }

std::shared_ptr<TableStatistics> AbstractJoinPlanNode::statistics() const { return _statistics; }

float AbstractJoinPlanNode::node_cost() const { return _node_cost; }

float AbstractJoinPlanNode::plan_cost() const {
  auto plan_cost = _node_cost;
  plan_cost += _left_child ? _left_child->plan_cost() : 0;
  plan_cost += _right_child ? _right_child->plan_cost() : 0;
  return plan_cost;
}

std::shared_ptr<const AbstractJoinPlanNode> AbstractJoinPlanNode::left_child() const { return _left_child; }

std::shared_ptr<const AbstractJoinPlanNode> AbstractJoinPlanNode::right_child() const { return _right_child; }

void AbstractJoinPlanNode::print(std::ostream& stream) const {
  const auto get_children_fn = [](const auto& node) {
    std::vector<std::shared_ptr<const AbstractJoinPlanNode>> children;
    if (node->_left_child) children.emplace_back(node->_left_child);
    if (node->_right_child) children.emplace_back(node->_right_child);
    return children;
  };
  const auto node_print_fn = [](const auto& node, auto& stream) {
    stream << "NodeCost=" << node->node_cost() << "; PlanCost=" << node->plan_cost()
           << "; RowCount=" << node->statistics()->row_count() << "; " << node->description();
  };

  print_directed_acyclic_graph<const AbstractJoinPlanNode>(shared_from_this(), get_children_fn, node_print_fn, stream);
}

std::shared_ptr<AbstractLQPNode> AbstractJoinPlanNode::_insert_predicate(
const std::shared_ptr<AbstractLQPNode>& lqp,
const std::shared_ptr<AbstractJoinPlanPredicate>& predicate) const {
  switch (predicate->type()) {
    case JoinPlanPredicateType::Atomic: {
      const auto atomic_predicate = std::static_pointer_cast<const JoinPlanAtomicPredicate>(predicate);
      const auto predicate_node = std::make_shared<PredicateNode>(
      atomic_predicate->left_operand, atomic_predicate->predicate_condition, atomic_predicate->right_operand);
      predicate_node->set_left_input(lqp);
      return predicate_node;
    }
    case JoinPlanPredicateType::LogicalOperator: {
      const auto logical_operator_predicate = std::static_pointer_cast<const JoinPlanLogicalPredicate>(predicate);

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


}  // namespace opossum
