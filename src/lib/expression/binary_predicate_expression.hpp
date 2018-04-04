#pragma once

#include "abstract_expression.hpp"
#include "types.hpp"
#include "abstract_predicate_expression.hpp"

namespace opossum {

class BinaryPredicateExpression : public AbstractPredicateExpression {
 public:
  BinaryPredicateExpression(const PredicateCondition predicate_condition,
                            const std::shared_ptr<AbstractExpression>& left_operand,
                            const std::shared_ptr<AbstractExpression>& right_operand);

  const std::shared_ptr<AbstractExpression>& left_operand() const;
  const std::shared_ptr<AbstractExpression>& right_operand() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string description() const override;

  PredicateCondition predicate_condition;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
};

}  // namespace opossum
