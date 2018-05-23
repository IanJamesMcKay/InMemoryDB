#include "outer_join_block.hpp"

#include "utils/assert.hpp"

namespace opossum {

OuterJoinBlock::OuterJoinBlock(const JoinMode join_mode,
               const std::shared_ptr<const AbstractJoinPlanPredicate>& predicate,
               const std::shared_ptr<AbstractQueryBlock>& left_input,
               const std::shared_ptr<AbstractQueryBlock>& right_input):
  AbstractQueryBlock(QueryBlockType::OuterJoin, {left_input, right_input}), join_mode(join_mode), predicate(predicate) {

}

size_t OuterJoinBlock::_shallow_hash_impl() const {
  auto hash = boost::hash_value(static_cast<std::underlying_type_t<JoinMode>>(join_mode));
  boost::hash_combine(hash, predicate->hash());
  return hash;
}

bool OuterJoinBlock::_shallow_deep_equals_impl(const AbstractQueryBlock& rhs) const {
  const auto outer_join_block_rhs = static_cast<const OuterJoinBlock&>(rhs);

  // TODO(moritz) replace with proper equality check
  return join_mode == outer_join_block_rhs.join_mode && predicate->hash() == outer_join_block_rhs.predicate->hash();
}

}  // namespace opossum