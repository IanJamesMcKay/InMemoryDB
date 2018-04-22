#pragma once

#include "abstract_expression2.hpp"
#include "types.hpp"
#include "abstract_predicate_expression.hpp"

namespace opossum {

class BinaryPredicateExpression : public AbstractPredicateExpression {
 public:
  BinaryPredicateExpression(const PredicateCondition predicate_condition,
                            const std::shared_ptr<AbstractExpression2>& left_operand,
                            const std::shared_ptr<AbstractExpression2>& right_operand);

  const std::shared_ptr<AbstractExpression2>& left_operand() const;
  const std::shared_ptr<AbstractExpression2>& right_operand() const;

  std::shared_ptr<AbstractExpression2> deep_copy() const override;
  std::string as_column_name() const override;

 protected:
  bool _shallow_equals(const AbstractExpression2& expression) const override;
};

}  // namespace opossum
