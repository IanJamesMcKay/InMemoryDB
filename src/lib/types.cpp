#include "types.hpp"

namespace opossum {

PredicateCondition flip_predicate_condition(const PredicateCondition predicate_condition) {
  switch (predicate_condition) {
    case PredicateCondition::Equals:
      return PredicateCondition::Equals;
    case PredicateCondition::NotEquals:
      return PredicateCondition::NotEquals;
    case PredicateCondition::LessThan:
      return PredicateCondition::GreaterThanEquals;
    case PredicateCondition::LessThanEquals:
      return PredicateCondition::GreaterThan;
    case PredicateCondition::GreaterThan:
      return PredicateCondition::LessThanEquals;
    case PredicateCondition::GreaterThanEquals:
      return PredicateCondition::LessThan;
    case PredicateCondition::Like:
      return PredicateCondition::Like;
    case PredicateCondition::NotLike:
      return PredicateCondition::NotLike;
    case PredicateCondition::IsNull:
      return PredicateCondition::IsNull;
    case PredicateCondition::IsNotNull:
      return PredicateCondition::IsNotNull;
    default:
      Fail("Can't flip PredicateCondition");
  }
}

}  // namespace opossum
