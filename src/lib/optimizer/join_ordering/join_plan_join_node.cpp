#include "join_plan_join_node.hpp"

namespace opossum {

JoinPlanJoinNode::JoinPlanJoinNode(const std::shared_ptr<const AbstractJoinPlanNode>& left_child,
                                   const std::shared_ptr<const AbstractJoinPlanNode>& right_child,
                                   std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> predicates)
    : AbstractJoinPlanNode(JoinPlanNodeType::Join) {
  _left_child = left_child;
  _right_child = right_child;
  _secondary_predicates = std::move(predicates);

  /**
   * Find primary join predicate - needs to be atomic and have one argument in the right and one in the left sub plan
   */
  const auto iter = std::find_if(_secondary_predicates.begin(), _secondary_predicates.end(), [&](const auto& predicate) {
    if (predicate->type() != JoinPlanPredicateType::Atomic) return false;
    const auto atomic_predicate = std::static_pointer_cast<const JoinPlanAtomicPredicate>(predicate);
    if (!is_lqp_column_reference(atomic_predicate->right_operand)) return false;

    const auto right_operand_column_reference = boost::get<LQPColumnReference>(atomic_predicate->right_operand);

    const auto left_operand_in_left_child = _left_child->find_column_id(atomic_predicate->left_operand);
    const auto left_operand_in_right_child = _right_child->find_column_id(atomic_predicate->left_operand);
    const auto right_operand_in_left_child = _left_child->find_column_id(right_operand_column_reference);
    const auto right_operand_in_right_child = _right_child->find_column_id(right_operand_column_reference);

    return (left_operand_in_left_child && right_operand_in_right_child) || (left_operand_in_right_child && right_operand_in_left_child);
  });

  if (iter != _secondary_predicates.end()) {
    auto primary_join_predicate = std::static_pointer_cast<const JoinPlanAtomicPredicate>(*iter);

    auto left_operand = primary_join_predicate->left_operand;
    auto predicate_condition = primary_join_predicate->predicate_condition;
    auto right_operand = boost::get<LQPColumnReference>(primary_join_predicate->right_operand);

    if (!_left_child->find_column_id(left_operand)) {
      std::swap(left_operand, right_operand);
      predicate_condition = flip_predicate_condition(predicate_condition);
    }
    DebugAssert(_left_child->find_column_id(left_operand) && _right_child->find_column_id(right_operand),
                "Predicate not applicable to sub plans");

    _primary_join_predicate =
        std::make_shared<JoinPlanAtomicPredicate>(left_operand, predicate_condition, right_operand);

    _secondary_predicates.erase(iter);
  }

  /**
   * Compute costs and statistics
   */
  const auto left_statistics = _left_child->statistics();
  const auto right_statistics = _right_child->statistics();
  const auto row_count_left = left_statistics->row_count();
  const auto row_count_right = right_statistics->row_count();

  // Compute cost&statistics of primary join predicate, if it exists. Otherwise assume a cross join.
  if (!_primary_join_predicate) {
    _statistics = left_statistics->generate_cross_join_statistics(right_statistics);
    _node_cost = left_statistics->row_count() * right_statistics->row_count();
  } else {
    const auto left_column_id = _left_child->find_column_id(_primary_join_predicate->left_operand);
    DebugAssert(left_column_id, "Couldn't resolve " + _primary_join_predicate->left_operand.description());

    const auto right_operand_column_reference = boost::get<LQPColumnReference>(_primary_join_predicate->right_operand);
    const auto right_column_id = _right_child->find_column_id(right_operand_column_reference);
    DebugAssert(right_column_id, "Couldn't resolve " + right_operand_column_reference.description());

    const auto join_column_ids = ColumnIDPair{*left_column_id, *right_column_id};

    // If scan type is equals, assume HashJoin can be used, otherwise its SortMergeJoin
    if (_primary_join_predicate->predicate_condition == PredicateCondition::Equals) {
      // Cost for HashJoin
      // 1.2f is the magic multiplier as by
      // "R. Krishnamurthy, H. Boral, and C. Zaniolo. Optimization of nonrecursive queries"
      _node_cost += 1.2f * row_count_left;
    } else {
      // Cost for SortMergeJoin
      _node_cost += row_count_left * std::log(row_count_left) + row_count_right * std::log(row_count_right);
    }

    _statistics = left_statistics->generate_predicated_join_statistics(
        right_statistics, JoinMode::Inner, join_column_ids, _primary_join_predicate->predicate_condition);
  }

  _order_predicates(_secondary_predicates, _statistics);

  // Apply remaining predicates
  for (const auto& predicate : _secondary_predicates) {
    const auto predicate_estimate = _estimate_predicate(predicate, _statistics);

    _node_cost += predicate_estimate.cost;
    _statistics = predicate_estimate.statistics;
  }

  _plan_cost = _node_cost + _left_child->plan_cost() + _right_child->plan_cost();
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

    const auto join_node = std::make_shared<JoinNode>(JoinMode::Inner, LQPColumnReferencePair{left_operand, right_operand},
                                     predicate_condition);
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
