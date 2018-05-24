#include "stored_table_block.hpp"

#include "utils/assert.hpp"

namespace opossum {

StoredTableBlock::StoredTableBlock(const std::shared_ptr<AbstractLQPNode>& node):
  AbstractQueryBlock(QueryBlockType::StoredTable, {}), node(node) {
  Assert(node->type() == LQPNodeType::StoredTable || node->type() == LQPNodeType::Mock, "Invalid node type");
}

size_t StoredTableBlock::_shallow_hash_impl() const {
  return node->hash();
}

bool StoredTableBlock::_shallow_deep_equals_impl(const AbstractQueryBlock& rhs) const {
  const auto& stored_table_block = static_cast<const StoredTableBlock&>(rhs);
  return !node->find_first_subplan_mismatch(stored_table_block.node);
}

}  // namespace opossum