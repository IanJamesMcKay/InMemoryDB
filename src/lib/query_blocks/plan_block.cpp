#include "plan_block.hpp"

namespace opossum {

PlanBlock::PlanBlock(const std::shared_ptr<AbstractLQPNode>& lqp):
  AbstractQueryBlock(QueryBlockType::Plan, {}) {}

PlanBlock::PlanBlock(const std::shared_ptr<AbstractLQPNode>& lqp, const std::shared_ptr<AbstractQueryBlock>& input):
  AbstractQueryBlock(QueryBlockType::Plan, {input}), lqp(lqp) {}

size_t PlanBlock::_shallow_hash_impl() const {
  return lqp->hash();
}

bool PlanBlock::_shallow_deep_equals_impl(const AbstractQueryBlock& rhs) const {
  const auto& plan_block_rhs = static_cast<const PlanBlock&>(rhs);
  return !lqp->find_first_subplan_mismatch(plan_block_rhs.lqp);
}

}  // namespace opossum
