#include "join_ordering_rule.hpp"

#include "optimizer/join_ordering/abstract_join_ordering_algorithm.hpp"
#include "optimizer/join_ordering/abstract_join_plan_node.hpp"
#include "optimizer/join_ordering/dp_ccp.hpp"
#include "optimizer/join_ordering/join_graph_builder.hpp"

namespace opossum {

JoinOrderingRule::JoinOrderingRule(const std::shared_ptr<AbstractJoinOrderingAlgorithm>& join_ordering_algorithm)
    : _join_ordering_algorithm(join_ordering_algorithm) {}

std::string JoinOrderingRule::name() const { return "Join Ordering Rule"; }

bool JoinOrderingRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root) {
  if (!_applicable(root)) {
    return _apply_to_inputs(root);
  }

  const auto join_graph = JoinGraphBuilder{}(root);  // NOLINT

  for (const auto& vertex : join_graph->vertices) {
    vertex->clear_outputs();
  }

  const auto join_plan = (*_join_ordering_algorithm)(join_graph);
  const auto lqp = join_plan.lqp;

  for (const auto& parent_relation : join_graph->output_relations) {
    parent_relation.output->set_input(parent_relation.input_side, lqp);
  }

  for (const auto& vertex : join_graph->vertices) {
    _apply_to_inputs(vertex);
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
      return _applicable(node->left_input()) && _applicable(node->right_input());
    default:
      return false;
  }
}

}  // namespace opossum
