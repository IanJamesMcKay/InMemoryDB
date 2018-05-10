#include "abstract_predicate_expression.hpp"

#include <sstream>

#include "boost/functional/hash.hpp"

namespace opossum {

AbstractPredicateExpression::AbstractPredicateExpression(const PredicateCondition predicate_condition, const std::vector<std::shared_ptr<AbstractExpression2>>& arguments):
AbstractExpression2(ExpressionType2::Predicate, arguments), predicate_condition(predicate_condition) {}

bool AbstractPredicateExpression::_shallow_equals(const AbstractExpression2& expression) const {
  return predicate_condition == static_cast<const AbstractPredicateExpression&>(expression).predicate_condition;
}

size_t AbstractPredicateExpression::_on_hash() const {
  return boost::hash_value(static_cast<size_t>(predicate_condition));
}
}  // namespace opossum
