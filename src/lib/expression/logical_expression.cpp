#include "logical_expression.hpp"

#include <sstream>

#include "boost/functional/hash.hpp"

namespace opossum {

LogicalExpression::LogicalExpression(const LogicalOperator logical_operator,
                                           const std::shared_ptr<AbstractExpression2>& left_operand,
                                           const std::shared_ptr<AbstractExpression2>& right_operand):
AbstractExpression2(ExpressionType2::Logical, {left_operand, right_operand}), logical_operator(logical_operator) {}

const std::shared_ptr<AbstractExpression2>& LogicalExpression::left_operand() const {
  return arguments[0];
}

const std::shared_ptr<AbstractExpression2>& LogicalExpression::right_operand() const {
  return arguments[1];
}

std::shared_ptr<AbstractExpression2> LogicalExpression::deep_copy() const {
  return std::make_shared<LogicalExpression>(logical_operator, left_operand()->deep_copy(), left_operand()->deep_copy());
}

std::string LogicalExpression::as_column_name() const {
  std::stringstream stream;


  Fail("Todo");
  return stream.str();
}

bool LogicalExpression::_shallow_equals(const AbstractExpression2& expression) const {
  return logical_operator == static_cast<const LogicalExpression&>(expression).logical_operator;
}

size_t LogicalExpression::_on_hash() const {
  return boost::hash_value(static_cast<size_t>(logical_operator));
}

}  // namespace opossum
