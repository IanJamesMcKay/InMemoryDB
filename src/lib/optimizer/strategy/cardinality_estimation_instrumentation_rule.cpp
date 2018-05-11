#include "cardinality_estimation_instrumentation_rule.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

std::string CardinalityEstimationInstrumentationRule::name() const {
  return "CardinalityEstimationInstrumentationRule";
}

bool CardinalityEstimationInstrumentationRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) {
  switch (node->type()) {
    case LQPNodeType::Predicate:
    case LQPNodeType::Join:

    default:
      ; // Do nothing
  }

  _apply_to_inputs(node);
}

}  // namespace opossum