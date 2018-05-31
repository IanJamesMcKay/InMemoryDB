#include "outer_join_block.hpp"

#include "logical_query_plan/join_node.hpp"
#include "utils/assert.hpp"

namespace opossum {

OuterJoinBlock::OuterJoinBlock(const std::shared_ptr<JoinNode>& outer_join_node,
               const std::shared_ptr<AbstractQueryBlock>& left_input,
               const std::shared_ptr<AbstractQueryBlock>& right_input):
  AbstractQueryBlock(QueryBlockType::OuterJoin, {left_input, right_input}), outer_join_node(outer_join_node) {
  Assert(outer_join_node->join_mode() == JoinMode::Outer || outer_join_node->join_mode() == JoinMode::Left || outer_join_node->join_mode() == JoinMode::Right, "Not an outer join");
}

size_t OuterJoinBlock::_shallow_hash_impl() const {
  return outer_join_node->hash();
}

bool OuterJoinBlock::_shallow_deep_equals_impl(const AbstractQueryBlock& rhs) const {
  const auto outer_join_block_rhs = static_cast<const OuterJoinBlock&>(rhs);
  return outer_join_node->shallow_equals(*outer_join_block_rhs.outer_join_node);
}

}  // namespace opossum