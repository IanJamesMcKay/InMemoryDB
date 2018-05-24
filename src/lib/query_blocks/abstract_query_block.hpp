#pragma once

#include <memory>
#include <vector>

namespace opossum {

enum class QueryBlockType { Aggregate, Finalizing, OuterJoin, Plan, Predicates, StoredTable };

/**
 * While the LogicalQueryPLAN is a ordered representation of a query, a LogicalQueryBLOCK defines a part of the query
 * without imposing any order the operations it represents.
 * It can be seen as a more general representation of the query than the LQP
 */
class AbstractQueryBlock {
 public:
  AbstractQueryBlock(const QueryBlockType type, const std::vector<std::shared_ptr<AbstractQueryBlock>>& sub_blocks);

  virtual ~AbstractQueryBlock() = default;

  size_t deep_hash() const;
  bool deep_equals(const AbstractQueryBlock& rhs) const;

  const QueryBlockType type;
  const std::vector<std::shared_ptr<AbstractQueryBlock>> sub_blocks;

 protected:
  virtual size_t _shallow_hash_impl() const = 0;
  virtual bool _shallow_deep_equals_impl(const AbstractQueryBlock& rhs) const = 0;
};

}  // namespace opossum