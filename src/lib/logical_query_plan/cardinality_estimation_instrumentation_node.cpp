#include "cardinality_estimation_instrumentation_node.hpp"

namespace opossum {

CardinalityEstimationInstrumentationNode::CardinalityEstimationInstrumentationNode(const std::shared_ptr<CardinalityEstimationCache>& cardinality_estimation_cache):
  AbstractLQPNode(LQPNodeType::CardinalityEstimationInstrumentation), cardinality_estimation_cache(cardinality_estimation_cache) {}

std::string CardinalityEstimationInstrumentationNode::description() const {
  return "CardinalityEstimationInstrumentationNode";
}

bool CardinalityEstimationInstrumentationNode::shallow_equals(const AbstractLQPNode& rhs) const {
  return true;
}

std::shared_ptr<AbstractLQPNode> CardinalityEstimationInstrumentationNode::_deep_copy_impl(
const std::shared_ptr<AbstractLQPNode>& copied_left_input,
const std::shared_ptr<AbstractLQPNode>& copied_right_input) const {

}

}  // namespace opossum