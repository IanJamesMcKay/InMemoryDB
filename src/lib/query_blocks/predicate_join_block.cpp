#include "predicate_join_block.hpp"

#include <algorithm>

namespace opossum {

PredicateJoinBlock::PredicateJoinBlock():
  PredicateJoinBlock({}, {}) {}

PredicateJoinBlock::PredicateJoinBlock(const std::vector<std::shared_ptr<AbstractQueryBlock>>& sub_blocks,
               const std::vector<std::shared_ptr<AbstractJoinPlanPredicate>>& predicates):
  AbstractQueryBlock(QueryBlockType::Predicates, sub_blocks), predicates(predicates) {

  // We need those expressions sorted, so the hash becomes unique
  auto& mutable_column_expression = const_cast<std::vector<std::shared_ptr<AbstractJoinPlanPredicate>>&>(this->predicates);
  std::sort(mutable_column_expression.begin(), mutable_column_expression.end(), [](const auto& lhs, const auto& rhs) {
    return lhs->hash() < rhs->hash();
  });
}

size_t PredicateJoinBlock::_shallow_hash_impl() const {
  auto hash = size_t{0};
  for (const auto& predicate : predicates) {
    boost::hash_combine(hash, predicate->hash());
  }
  return hash;
}

bool PredicateJoinBlock::_shallow_deep_equals_impl(const AbstractQueryBlock& rhs) const {
  const auto& predicate_block_rhs = static_cast<const PredicateJoinBlock&>(rhs);
  return std::equal(predicates.begin(), predicates.end(),
                    predicate_block_rhs.predicates.begin(), predicate_block_rhs.predicates.end(),
                    [](const auto& predicate_lhs, const auto& predicate_rhs) {
                      // TODO(moritz) Once on the new Expression systems, actually compare the expressions here.
                      return predicate_lhs->hash() == predicate_rhs->hash();
                    });
}

}  // namespace opossum
