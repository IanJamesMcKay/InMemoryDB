#include "join_plan_join_node.hpp"

#include "abstract_cost_model.hpp"

namespace opossum {

JoinPlanJoinNode::JoinPlanJoinNode(
    const std::shared_ptr<const AbstractJoinPlanNode>& left_child,
    const std::shared_ptr<const AbstractJoinPlanNode>& right_child,
    const std::shared_ptr<TableStatistics>& statistics,
    const std::shared_ptr<const JoinPlanAtomicPredicate>& primary_join_predicate,
    const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& secondary_predicates, const Cost node_cost)
    : AbstractJoinPlanNode(JoinPlanNodeType::Join, node_cost, statistics, left_child, right_child),
      _primary_join_predicate(primary_join_predicate),
      _secondary_predicates(secondary_predicates) {

}

std::shared_ptr<const JoinPlanAtomicPredicate> JoinPlanJoinNode::primary_join_predicate() const {
  return _primary_join_predicate;
}

const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& JoinPlanJoinNode::secondary_predicates() const {
  return _secondary_predicates;
}

bool JoinPlanJoinNode::contains_vertex(const std::shared_ptr<AbstractLQPNode>& node) const {
  return _left_child->contains_vertex(node) || _right_child->contains_vertex(node);
}

std::optional<ColumnID> JoinPlanJoinNode::find_column_id(const LQPColumnReference& column_reference) const {
  const auto column_id_in_left_child = _left_child->find_column_id(column_reference);
  const auto column_id_in_right_child = _right_child->find_column_id(column_reference);

  if (!column_id_in_left_child && !column_id_in_right_child) {
    return std::nullopt;
  }

  return column_id_in_left_child
             ? column_id_in_left_child
             : static_cast<ColumnID>(*column_id_in_right_child + _left_child->output_column_count());
}

std::shared_ptr<AbstractLQPNode> JoinPlanJoinNode::to_lqp() const {
  std::shared_ptr<AbstractLQPNode> lqp;

  /**
   * Turn primary join predicate into inner join, if it exists. Otherwise create a cross join
   */
  if (!_primary_join_predicate) {
    lqp = std::make_shared<JoinNode>(JoinMode::Cross);
  } else {
    auto left_operand = _primary_join_predicate->left_operand;
    auto right_operand = boost::get<LQPColumnReference>(_primary_join_predicate->right_operand);
    auto predicate_condition = _primary_join_predicate->predicate_condition;

    const auto join_node = std::make_shared<JoinNode>(
        JoinMode::Inner, LQPColumnReferencePair{left_operand, right_operand}, predicate_condition);
    //join_node->set_implementation(JoinOperatorImplementation::SortMerge);
    lqp = join_node;
  }

  lqp->set_left_input(_left_child->to_lqp());
  lqp->set_right_input(_right_child->to_lqp());

  /**
   * Apply all other predicates as PredicateNodes
   */
  for (const auto& predicate : _secondary_predicates) {
    lqp = _insert_predicate(lqp, predicate);
  }

  return lqp;
}

size_t JoinPlanJoinNode::output_column_count() const {
  return _left_child->output_column_count() + _right_child->output_column_count();
}

std::string JoinPlanJoinNode::description() const {
  std::stringstream stream;

  if (_primary_join_predicate) {
    _primary_join_predicate->print(stream);
    stream << " ";
  }

  stream << "{";
  for (size_t predicate_idx = 0; predicate_idx < _secondary_predicates.size(); ++predicate_idx) {
    _secondary_predicates[predicate_idx]->print(stream);
    if (predicate_idx + 1 < _secondary_predicates.size()) {
      stream << ", ";
    }
  }
  stream << "}";
  return stream.str();
}

}  // namespace opossum
