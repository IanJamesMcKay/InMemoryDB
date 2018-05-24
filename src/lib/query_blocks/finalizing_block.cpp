#include "finalizing_block.hpp"

#include "boost/functional/hash.hpp"

namespace opossum {

FinalizingBlock::FinalizingBlock(const std::vector<std::shared_ptr<LQPExpression>>& column_expressions,
                const OrderByDefinitions& order_by_definitions,
const std::optional<size_t>& limit,const std::shared_ptr<AbstractQueryBlock>& input):
  AbstractQueryBlock(QueryBlockType::Finalizing, {input}), column_expressions(column_expressions), order_by_definitions(order_by_definitions), limit(limit) {

  // We need thos expressions sorted, so the hash becomes unique
  auto& mutable_column_expression = const_cast<std::vector<std::shared_ptr<LQPExpression>>&>(column_expressions);
  std::sort(mutable_column_expression.begin(), mutable_column_expression.end(), [](const auto& lhs, const auto& rhs) {
    return lhs->hash() < rhs->hash();
  });
}

size_t FinalizingBlock::_shallow_hash_impl() const {
  auto hash = size_t{0};
  for (const auto& expression : column_expressions) {
    boost::hash_combine(hash, expression->hash());
  }
  for (const auto& order_by_definition : order_by_definitions) {
    boost::hash_combine(hash, order_by_definition.column_reference.hash());
    boost::hash_combine(hash, static_cast<std::underlying_type_t<OrderByMode>>(order_by_definition.order_by_mode));
  }
  boost::hash_combine(hash, limit.has_value());
  boost::hash_combine(hash, limit.value_or(0));

  return hash;
}

bool FinalizingBlock::_shallow_deep_equals_impl(const AbstractQueryBlock& rhs) const {
  const auto& finalizing_block_rhs = static_cast<const FinalizingBlock&>(rhs);

  if (!std::equal(column_expressions.begin(), column_expressions.end(),
                    finalizing_block_rhs.column_expressions.begin(), finalizing_block_rhs.column_expressions.end(),
                    [](const auto& expression_lhs, const auto& expression_rhs) {
                      // TODO(moritz) Once on the new Expression systems, actually compare the expressions here.
                      return expression_lhs->hash() == expression_rhs->hash();
                    })) {
    return false;
  }

  return order_by_definitions == finalizing_block_rhs.order_by_definitions;
}

}  // namespace opossum
