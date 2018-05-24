#pragma once

#include "abstract_query_block.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

/**
 * Pseudo "Block" encapsulating a part of the Query that has already been planned.
 * The Optimizer will replace other Blocks with PlanBlocks when it is done optimizing them.
 */
class PlanBlock : public AbstractQueryBlock {
 public:
  PlanBlock(const std::shared_ptr<AbstractLQPNode>& lqp);

  std::shared_ptr<AbstractLQPNode> lqp;

 protected:
  size_t _shallow_hash_impl() const override;
  bool _shallow_deep_equals_impl(const AbstractQueryBlock& rhs) const override;
};

}  // namespace opossum
