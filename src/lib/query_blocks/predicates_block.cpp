#include "predicates_block.hpp"

#include <algorithm>

namespace opossum {

PredicateBlock::PredicateBlock():
  PredicateBlock({}, {}) {}

PredicateBlock::PredicateBlock(const std::vector<std::shared_ptr<AbstractQueryBlock>>& sub_blocks,
               const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates):
  AbstractQueryBlock(QueryBlockType::Predicates, sub_blocks), predicates(predicates) {

}

size_t PredicateBlock::_shallow_hash_impl() const {
  auto hash = size_t{0};
  for (const auto& predicate : predicates) {
    boost::hash_combine(hash, predicate->hash());
  }
  return hash;
}

bool PredicateBlock::_shallow_deep_equals_impl(const AbstractQueryBlock& rhs) const {
  const auto& predicate_block_rhs = static_cast<const PredicateBlock&>(rhs);
  return std::equal(predicates.begin(), predicates.end(),
                    predicate_block_rhs.predicates.begin(), predicate_block_rhs.predicates.end(),
                    [](const auto& predicate_lhs, const auto& predicate_rhs) {
                      // TODO(moritz) Once on the new Expression systems, actually compare the expressions here.
                      return predicate_lhs->hash() == predicate_rhs->hash();
                    });
}

}  // namespace opossum
