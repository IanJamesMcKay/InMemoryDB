#include "between_expression.hpp"

#include "utils/assert.hpp"

namespace opossum {

BetweenExpression::BetweenExpression(const std::shared_ptr<AbstractExpression2>& value,
                  const std::shared_ptr<AbstractExpression2>& lower_bound,
                  const std::shared_ptr<AbstractExpression2>& upper_bound):
  AbstractPredicateExpression(PredicateCondition::Between, {value, upper_bound, lower_bound})
{

}

const std::shared_ptr<AbstractExpression2>& BetweenExpression::value() const {
  return arguments[0];
}

const std::shared_ptr<AbstractExpression2>& BetweenExpression::lower_bound() const{
  return arguments[1];
}

const std::shared_ptr<AbstractExpression2>& BetweenExpression::upper_bound() const {
  return arguments[2];
}

std::shared_ptr<AbstractExpression2> BetweenExpression::deep_copy() const {
  return std::make_shared<BetweenExpression>(value()->deep_copy(), lower_bound()->deep_copy(), upper_bound()->deep_copy());
}

std::string BetweenExpression::as_column_name() const {
  Fail("Notyetimplemented");
  return "";
};

}  // namespace opossum
