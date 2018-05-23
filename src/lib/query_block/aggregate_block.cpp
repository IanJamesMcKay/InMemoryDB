#include "aggregate_block.hpp"

#include "boost/functional/hash.hpp"

namespace opossum {

AggregateBlock::AggregateBlock(const std::vector<LQPColumnReference>& group_by_columns,
               const std::shared_ptr<AbstractQueryBlock>& input):
  AbstractQueryBlock(QueryBlockType::Aggregate, {input}), group_by_columns(group_by_columns) {}

size_t AggregateBlock::_shallow_hash_impl() const {
  auto hash = size_t{0};
  for (const auto& group_by_column : group_by_columns) {
    boost::hash_combine(hash, group_by_column.hash());
  }
  return hash;
}

bool AggregateBlock::_shallow_deep_equals_impl(const AbstractQueryBlock& rhs) const {
  const auto& aggregate_block_rhs = static_cast<const AggregateBlock&>(rhs);
  return group_by_columns == aggregate_block_rhs.group_by_columns;
}

}  // namespace opossum