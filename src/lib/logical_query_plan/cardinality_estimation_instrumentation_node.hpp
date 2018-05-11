#pragma once

#include "abstract_lqp_node.hpp"

namespace opossum {

class CardinalityEstimationCache;

class CardinalityEstimationInstrumentationNode : public AbstractLQPNode {
 public:
  CardinalityEstimationInstrumentationNode(const std::shared_ptr<CardinalityEstimationCache>& cardinality_estimation_cache);

  std::string description() const override;
  bool shallow_equals(const AbstractLQPNode& rhs) const override;

  std::shared_ptr<CardinalityEstimationCache> cardinality_estimation_cache;

 protected:
  std::shared_ptr<AbstractLQPNode> _deep_copy_impl(
    const std::shared_ptr<AbstractLQPNode>& copied_left_input,
    const std::shared_ptr<AbstractLQPNode>& copied_right_input) const override;
};

}  // namespace opossum
