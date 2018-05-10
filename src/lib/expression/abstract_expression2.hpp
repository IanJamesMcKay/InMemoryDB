#pragma once

#include <functional>
#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "types.hpp"

namespace opossum {

enum class ExpressionType2 {
  Aggregate, Arithmetic, Array, Case, Column, Exists, External, Function, In, Logical, Mock, Not, Predicate, Select, Value, ValuePlaceholder
};

class AbstractExpression2 : public std::enable_shared_from_this<AbstractExpression2> {
 public:
  explicit AbstractExpression2(const ExpressionType2 type, const std::vector<std::shared_ptr<AbstractExpression2>>& arguments);
  virtual ~AbstractExpression2() = default;

  bool deep_equals(const AbstractExpression2& expression) const;

  virtual bool requires_calculation() const;

  virtual std::shared_ptr<AbstractExpression2> deep_copy() const = 0;

  virtual std::string as_column_name() const = 0;

  size_t hash() const;

  const ExpressionType2 type;
  std::vector<std::shared_ptr<AbstractExpression2>> arguments;

 protected:
  virtual bool _shallow_equals(const AbstractExpression2& expression) const;
  virtual size_t _on_hash() const;
};

// Wrapper around expression->hash(), to enable hash based containers containing std::shared_ptr<AbstractExpression2>
struct ExpressionSharedPtrHash final {
  size_t operator()(const std::shared_ptr<AbstractExpression2>& expression) const {
    return expression->hash();
  }
};

// Wrapper around expression->deep_equals(), to enable hash based containers containing
// std::shared_ptr<AbstractExpression2>
struct ExpressionSharedPtrEqual final {
  size_t operator()(const std::shared_ptr<AbstractExpression2>& expression_a,
                    const std::shared_ptr<AbstractExpression2>& expression_b) const {
    return expression_a->deep_equals(*expression_b);
  }
};

template<typename Value> using ExpressionUnorderedMap = std::unordered_map<std::shared_ptr<AbstractExpression2>, Value, ExpressionSharedPtrHash, ExpressionSharedPtrEqual>;
using ExpressionUnorderedSet = std::unordered_set<std::shared_ptr<AbstractExpression2>, ExpressionSharedPtrHash, ExpressionSharedPtrEqual>;

}  // namespace opossum
