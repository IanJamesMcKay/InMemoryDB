#include "binary_predicate_expression.hpp"

#include <sstream>

#include "boost/functional/hash.hpp"

namespace opossum {

BinaryPredicateExpression::BinaryPredicateExpression(const PredicateCondition predicate_condition,
                                                         const std::shared_ptr<AbstractExpression2>& left_operand,
                                                         const std::shared_ptr<AbstractExpression2>& right_operand):
AbstractPredicateExpression(predicate_condition, {left_operand, right_operand}) {}

const std::shared_ptr<AbstractExpression2>& BinaryPredicateExpression::left_operand() const {
  return arguments[0];
}

const std::shared_ptr<AbstractExpression2>& BinaryPredicateExpression::right_operand() const {
  return arguments[1];
}

std::shared_ptr<AbstractExpression2> BinaryPredicateExpression::deep_copy() const {
  return std::make_shared<BinaryPredicateExpression>(predicate_condition, left_operand()->deep_copy(), left_operand()->deep_copy());
}

std::string BinaryPredicateExpression::as_column_name() const {
  std::stringstream stream;

  Fail("Todo");

  return stream.str();
}

bool BinaryPredicateExpression::_shallow_equals(const AbstractExpression2& expression) const {
  return predicate_condition == static_cast<const BinaryPredicateExpression&>(expression).predicate_condition;
}

}  // namespace opossum
