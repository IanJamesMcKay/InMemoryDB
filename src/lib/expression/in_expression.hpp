#pragma once

#include "abstract_predicate_expression.hpp"

namespace opossum {

class InExpression : public AbstractPredicateExpression {
 public:
  InExpression(const std::shared_ptr<AbstractExpression2>& value, const std::shared_ptr<AbstractExpression2>& set);

  const std::shared_ptr<AbstractExpression2>& value() const;
  const std::shared_ptr<AbstractExpression2>& set() const;

  std::shared_ptr<AbstractExpression2> deep_copy() const override;
  std::string as_column_name() const override;
};

}  // namespace opossum
