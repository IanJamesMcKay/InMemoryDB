#pragma once

#include <memory>
#include <vector>

namespace opossum {

enum class QueryBlockType { OuterJoin, Plan, PredicateJoin };

/**
 * While the LogicalQuery**Plan** is a ordered representation of a query, a LogicalQuery**Block** defines a part of the
 * query without imposing any order the operations it represents.
 *
 * Typically QueryBlocks represent a part of a query that is optimised in isolation from the other blocks
 */
class AbstractQueryBlock {
 public:
  AbstractQueryBlock(const QueryBlockType type, const std::vector<std::shared_ptr<AbstractQueryBlock>>& sub_blocks);

  virtual ~AbstractQueryBlock() = default;

  size_t hash() const;
  bool deep_equals(const AbstractQueryBlock& rhs) const;

  const QueryBlockType type;
  const std::vector<std::shared_ptr<AbstractQueryBlock>> sub_blocks;

 protected:
  virtual size_t _shallow_hash_impl() const = 0;
  virtual bool _shallow_deep_equals_impl(const AbstractQueryBlock& rhs) const = 0;
};

}  // namespace opossum