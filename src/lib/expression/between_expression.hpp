#pragma once

#include "abstract_expression2.hpp"
#include "types.hpp"
#include "abstract_predicate_expression.hpp"

namespace opossum {

class BetweenExpression : public AbstractPredicateExpression {
 public:
  BetweenExpression(const std::shared_ptr<AbstractExpression2>& value,
                            const std::shared_ptr<AbstractExpression2>& lower_bound,
                            const std::shared_ptr<AbstractExpression2>& upper_bound);

  const std::shared_ptr<AbstractExpression2>& value() const;
  const std::shared_ptr<AbstractExpression2>& lower_bound() const;
  const std::shared_ptr<AbstractExpression2>& upper_bound() const;

  std::shared_ptr<AbstractExpression2> deep_copy() const override;
  std::string as_column_name() const override;
};

}  // namespace opossum
