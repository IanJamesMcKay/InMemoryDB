#include "not_expression.hpp"

#include "utils/assert.hpp"

namespace opossum {

NotExpression::NotExpression(const std::shared_ptr<AbstractExpression2>& operand):
  AbstractExpression2(ExpressionType2::Not, {operand}){

}

const std::shared_ptr<AbstractExpression2>& NotExpression::operand() const {
  return arguments[0];
}

std::shared_ptr<AbstractExpression2> NotExpression::deep_copy() const {
  return std::make_shared<NotExpression>(operand()->deep_copy());
}

std::string NotExpression::as_column_name() const {
  Fail("Notyetimplemented");
  return "";
}

}  // namespace opossum
