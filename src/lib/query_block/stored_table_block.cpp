#include "stored_table_block.hpp"

namespace opossum {

StoredTableBlock::StoredTableBlock(const std::shared_ptr<StoredTableNode>& stored_table_node):
  AbstractQueryBlock(QueryBlockType::StoredTable, {}), stored_table_node(stored_table_node) {}

size_t StoredTableBlock::_shallow_hash_impl() const {
  return stored_table_node->hash();
}

bool StoredTableBlock::_shallow_deep_equals_impl(const AbstractQueryBlock& rhs) const {
  const auto& stored_table_block = static_cast<const StoredTableBlock&>(rhs);
  return !stored_table_node->find_first_subplan_mismatch(stored_table_block.stored_table_node);
}

}  // namespace opossum