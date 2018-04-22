#include "abstract_expression2.hpp"

#include <queue>

#include "boost/functional/hash.hpp"

namespace opossum {

AbstractExpression2::AbstractExpression2(const ExpressionType2 type, const std::vector<std::shared_ptr<AbstractExpression2>>& arguments):
  type(type), arguments(arguments) {

}

bool AbstractExpression2::requires_calculation() const {
  return !arguments.empty();
}

bool AbstractExpression2::deep_equals(const AbstractExpression2& expression) const {
  if (type != expression.type) return false;
//  if (!deep_equals_expressions(arguments, expression.arguments)) return false;
  return _shallow_equals(expression);
}

size_t AbstractExpression2::hash() const {
  auto hash = boost::hash_value(static_cast<ExpressionType2>(type));
  for (const auto& argument : arguments) {
    boost::hash_combine(hash, argument->hash());
  }
  boost::hash_combine(hash, _on_hash());
  return hash;
}

bool AbstractExpression2::_shallow_equals(const AbstractExpression2& expression) const {
  return true;
}

size_t AbstractExpression2::_on_hash() const {
  return 0;
}

}  // namespace opoosum