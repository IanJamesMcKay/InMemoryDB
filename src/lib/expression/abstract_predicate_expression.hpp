#pragma once

#include "abstract_expression2.hpp"
#include "types.hpp"

namespace opossum {

class AbstractPredicateExpression : public AbstractExpression2 {
 public:
  AbstractPredicateExpression(const PredicateCondition predicate_condition,
                              const std::vector<std::shared_ptr<AbstractExpression2>>& arguments);

  PredicateCondition predicate_condition;

 protected:
  bool _shallow_equals(const AbstractExpression2& expression) const override;
  size_t _on_hash() const override;
};

}  // namespace opossum
