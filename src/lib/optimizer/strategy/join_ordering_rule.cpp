#include "join_ordering_rule.hpp"

#include "optimizer/join_ordering/dp_ccp.hpp"
#include "optimizer/join_ordering/join_graph_builder.hpp"
#include "optimizer/join_ordering/abstract_join_plan_node.hpp"

namespace opossum {

std::string JoinOrderingRule::name() const {
  return "Join Ordering Rule";
}

bool JoinOrderingRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root) {
  if (!_applicable(root)) {
    return _apply_to_children(root);
  }

  const auto join_graph = JoinGraphBuilder{}(root);  // NOLINT

  for (const auto& vertex : join_graph->vertices) {
    vertex->clear_parents();
  }

  const auto join_plan = DpCcp{join_graph}();
  const auto lqp = join_plan->to_lqp();

  for (const auto& parent_relation : join_graph->parent_relations) {
    parent_relation.parent->set_child(parent_relation.child_side, lqp);
  }

  for (const auto& vertex : join_graph->vertices) {
    _apply_to_children(vertex);
  }

  return false;
}

bool JoinOrderingRule::_applicable(const std::shared_ptr<AbstractLQPNode>& node) const {
  if (!node) return true;

  switch (node->type()) {
    case LQPNodeType::Join:
    case LQPNodeType::Predicate:
    case LQPNodeType::Union:
    case LQPNodeType::Aggregate:
    case LQPNodeType::Mock:
    case LQPNodeType::Limit:
    case LQPNodeType::Projection:
    case LQPNodeType::Sort:
    case LQPNodeType::StoredTable:
    case LQPNodeType::Validate:
      return _applicable(node->left_child()) && _applicable(node->right_child());
    default:
      return false;
  }
}

}  // namespace opossum
