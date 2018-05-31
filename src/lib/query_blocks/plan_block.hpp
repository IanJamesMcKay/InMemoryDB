#pragma once

#include "abstract_query_block.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

/**
 * Encapsulates a part of the Query that has already been planned as the Blocks are translated back to an LQP after
 * optimization
 *  OR
 * A chain of nodes (Aggregates, Projections, Sorts, Limits) that belong in no other Block type
 */
class PlanBlock : public AbstractQueryBlock {
 public:
  explicit PlanBlock(const std::shared_ptr<AbstractLQPNode>& lqp);
  PlanBlock(const std::shared_ptr<AbstractLQPNode>& lqp, const std::shared_ptr<AbstractQueryBlock>& input);

  std::shared_ptr<AbstractLQPNode> lqp;

 protected:
  size_t _shallow_hash_impl() const override;
  bool _shallow_deep_equals_impl(const AbstractQueryBlock& rhs) const override;
};

}  // namespace opossum
