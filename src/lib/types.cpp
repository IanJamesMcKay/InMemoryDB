#include "types.hpp"

namespace opossum {

PredicateCondition flip_predicate_condition(const PredicateCondition predicate_condition) {
  switch (predicate_condition) {
    case PredicateCondition::Equals:
      return PredicateCondition::Equals;
    case PredicateCondition::NotEquals:
      return PredicateCondition::NotEquals;
    case PredicateCondition::LessThan:
      return PredicateCondition::GreaterThan;
    case PredicateCondition::LessThanEquals:
      return PredicateCondition::GreaterThanEquals;
    case PredicateCondition::GreaterThan:
      return PredicateCondition::LessThan;
    case PredicateCondition::GreaterThanEquals:
      return PredicateCondition::LessThanEquals;
    default:
      Fail("Can't flip PredicateCondition");
  }
}

}  // namespace opossum
