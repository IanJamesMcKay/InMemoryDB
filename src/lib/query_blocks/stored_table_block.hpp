#pragma once

#include <memory>

#include "abstract_query_block.hpp"
#include "logical_query_plan/stored_table_node.hpp"

namespace opossum {

/**
 * Simply wraps a StoredTableNode
 */
class StoredTableBlock : public AbstractQueryBlock {
 public:
  explicit StoredTableBlock(const std::shared_ptr<AbstractLQPNode>& node);

  const std::shared_ptr<AbstractLQPNode> node;

 protected:
  size_t _shallow_hash_impl() const override;
  bool _shallow_deep_equals_impl(const AbstractQueryBlock& rhs) const override;
};

}  // namespace opossum
