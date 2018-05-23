#pragma once

#include "abstract_query_block.hpp"
#include "optimizer/join_ordering/join_plan_predicate.hpp"

namespace opossum {

/**
 * Represents a set of Predicates that operate on a set of SubBlocks. The predicates can be either local (only
 * referencing one SubBlock) or connect multiple SubBlocks with InnerJoin semantics.
 */
class PredicateBlock : public AbstractQueryBlock {
 public:
  PredicateBlock();
  PredicateBlock(const std::vector<std::shared_ptr<AbstractQueryBlock>>& sub_blocks,
                 const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates);

  const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates;

 protected:
  size_t _shallow_hash_impl() const override;
  bool _shallow_deep_equals_impl(const AbstractQueryBlock& rhs) const override;
};

}  // namespace opossum
