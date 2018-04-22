#pragma once

#include "abstract_expression2.hpp"
#include "types.hpp"
#include "abstract_predicate_expression.hpp"

namespace opossum {

class NotExpression : public AbstractExpression2 {
 public:
  NotExpression(const std::shared_ptr<AbstractExpression2>& operand);

  const std::shared_ptr<AbstractExpression2>& operand() const;

  std::shared_ptr<AbstractExpression2> deep_copy() const override;
  std::string as_column_name() const override;
};

}  // namespace opossum
