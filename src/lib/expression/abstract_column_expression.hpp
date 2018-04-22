#pragma once

#include "abstract_expression2.hpp"

namespace opossum {

class AbstractColumnExpression : public AbstractExpression2 {
 public:
  AbstractColumnExpression(): AbstractExpression2(ExpressionType2::Column, {}) {}
};

}  // namespace opossum
