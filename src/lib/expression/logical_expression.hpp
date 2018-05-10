#pragma once

#include "abstract_expression2.hpp"

namespace opossum {

enum class LogicalOperator {
  And,
  Or
};

class LogicalExpression : public AbstractExpression2 {
 public:
  LogicalExpression(const LogicalOperator logical_operator,
                    const std::shared_ptr<AbstractExpression2>& left_operand,
                    const std::shared_ptr<AbstractExpression2>& right_operand);

  const std::shared_ptr<AbstractExpression2>& left_operand() const;
  const std::shared_ptr<AbstractExpression2>& right_operand() const;

  std::shared_ptr<AbstractExpression2> deep_copy() const override;
  std::string as_column_name() const override;

  LogicalOperator logical_operator;

 protected:
  bool _shallow_equals(const AbstractExpression2& expression) const override;
  size_t _on_hash() const override;
};

}  // namespace opossum
