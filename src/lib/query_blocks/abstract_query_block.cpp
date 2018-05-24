#include "abstract_query_block.hpp"

#include "boost/functional/hash.hpp"

namespace opossum {

AbstractQueryBlock::AbstractQueryBlock(const QueryBlockType type, const std::vector<std::shared_ptr<AbstractQueryBlock>>& sub_blocks):
  type(type), sub_blocks(sub_blocks) {

  // We need those sub blocks sorted, so the hash becomes unique
  auto& mutable_sub_blocks = const_cast<std::vector<std::shared_ptr<AbstractQueryBlock>>&>(this->sub_blocks);
  std::sort(mutable_sub_blocks.begin(), mutable_sub_blocks.end(), [](const auto& lhs, const auto& rhs) {
    return lhs->hash() < rhs->hash();
  });

}

size_t AbstractQueryBlock::hash() const {
  auto hash = boost::hash_value(static_cast<std::underlying_type_t<QueryBlockType>>(type));
  boost::hash_combine(hash, _shallow_hash_impl());

  for (const auto& sub_block : sub_blocks) {
    boost::hash_combine(hash, sub_block->hash());
  }

  return hash;
}

bool AbstractQueryBlock::deep_equals(const AbstractQueryBlock& rhs) const {
  if (type != rhs.type) return false;
  if (sub_blocks.size() != rhs.sub_blocks.size()) return false;

  for (auto sub_block_idx = size_t{0}; sub_block_idx < sub_blocks.size(); ++sub_block_idx) {
    if (!sub_blocks[sub_block_idx]->deep_equals(*rhs.sub_blocks[sub_block_idx])) return false;
  }

  return true;
}

}  // namespace opossum