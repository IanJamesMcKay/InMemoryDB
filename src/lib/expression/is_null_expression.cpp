#include "is_null_expression.hpp"

#include "utils/assert.hpp"

namespace opossum {

IsNullExpression::IsNullExpression(const PredicateCondition predicate_condition, const std::shared_ptr<AbstractExpression2>& operand):
  AbstractPredicateExpression(predicate_condition, {operand}) {}

const std::shared_ptr<AbstractExpression2>& IsNullExpression::operand() const {
  return arguments[0];
}

std::shared_ptr<AbstractExpression2> IsNullExpression::deep_copy() const {
  return std::make_shared<IsNullExpression>(predicate_condition, operand()->deep_copy());
}

std::string IsNullExpression::as_column_name() const {
  Fail("Notyetimpleented");
  return "";
}

}  // namespace opossum
