#include "jit_compute.hpp"

#include <stack>

namespace opossum {

JitCompute::JitCompute(const std::shared_ptr<const JitExpression>& expression) : _expression{expression} {}

std::string JitCompute::description() const {
  return "[Compute] x" + std::to_string(_expression->result().tuple_index()) + " = " + _expression->to_string();
}

std::shared_ptr<const JitExpression> JitCompute::expression() { return _expression; }

std::map<size_t, bool> JitCompute::accessed_column_ids() const {
  std::map<size_t, bool> column_ids;
  std::stack<const std::shared_ptr<const JitExpression>> stack;
  stack.push(_expression);
  while (!stack.empty()) {
    auto current = stack.top();
    stack.pop();
    if (auto right_child = current->right_child()) stack.push(right_child);
    if (auto left_child = current->left_child()) stack.push(left_child);
    if (current->expression_type() == ExpressionType::Column) {
      const auto tuple_index = current->result().tuple_index();
      column_ids[tuple_index] = !column_ids.count(tuple_index);
    }
  }
  return column_ids;
}

void JitCompute::set_load_column(const size_t tuple_id, const size_t input_column_index) const {
  std::stack<const std::shared_ptr<const JitExpression>> stack;
  stack.push(_expression);
  while (!stack.empty()) {
    auto current = stack.top();
    stack.pop();
    if (auto right_child = current->right_child()) stack.push(right_child);
    if (auto left_child = current->left_child()) stack.push(left_child);
    if (current->expression_type() == ExpressionType::Column) {
      const auto tuple_index = current->result().tuple_index();
      if (tuple_id == tuple_index) {
        current->set_load_column(input_column_index);
        return;
      }
    }
  }
}

void JitCompute::_consume(JitRuntimeContext& context) const {
  _expression->compute(context);
  _emit(context);
}

}  // namespace opossum
