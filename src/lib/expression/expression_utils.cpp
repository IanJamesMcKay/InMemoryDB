#include "expression_utils.hpp"

#include <algorithm>
#include <sstream>
#include <queue>

#include "lqp_column_expression.hpp"

namespace opossum {

bool expressions_equal(const std::vector<std::shared_ptr<AbstractExpression2>>& expressions_a,
                       const std::vector<std::shared_ptr<AbstractExpression2>>& expressions_b) {
  return std::equal(expressions_a.begin(), expressions_a.end(), expressions_b.begin(), expressions_b.end(),
                    [&] (const auto& expression_a, const auto& expression_b) { return expression_a->deep_equals(*expression_b);});
}
//
//bool expressions_equal_to_expressions_in_different_lqp(
//const std::vector<std::shared_ptr<AbstractExpression2>> &expressions_left,
//const std::vector<std::shared_ptr<AbstractExpression2>> &expressions_right,
//const LQPNodeMapping& node_mapping) {
//  if (expressions_left.size() != expressions_right.size()) return false;
//
//  for (auto expression_idx = size_t{0}; expression_idx < expressions_left.size(); ++expression_idx) {
//    const auto& expression_left = *expressions_left[expression_idx];
//    const auto& expression_right = *expressions_right[expression_idx];
//
//    if (!expression_equal_to_expression_in_different_lqp(expression_left, expression_right, node_mapping)) return false;
//  }
//
//  return true;
//}
//
//bool expression_equal_to_expression_in_different_lqp(const AbstractExpression2& expression_left,
//                                                     const AbstractExpression2& expression_right,
//                                                     const LQPNodeMapping& node_mapping) {
//  /**
//   * Compare expression_left to expression_right by creating a deep copy of expression_left and adapting it to the LQP
//   * of expression_right, then perform a normal comparison of two expressions in the same LQP.
//   */
//
//  auto copied_expression_left = expression_left.deep_copy();
//  expression_adapt_to_different_lqp(copied_expression_left, node_mapping);
//  return copied_expression_left->deep_equals(expression_right);
//}

std::vector<std::shared_ptr<AbstractExpression2>> expressions_copy(
const std::vector<std::shared_ptr<AbstractExpression2>>& expressions) {
  std::vector<std::shared_ptr<AbstractExpression2>> copied_expressions;
  copied_expressions.reserve(expressions.size());
  for (const auto& expression : expressions) {
    copied_expressions.emplace_back(expression->deep_copy());
  }
  return copied_expressions;
}

//std::vector<std::shared_ptr<AbstractExpression2>> expressions_copy_and_adapt_to_different_lqp(
//const std::vector<std::shared_ptr<AbstractExpression2>>& expressions,
//const LQPNodeMapping& node_mapping) {
//  std::vector<std::shared_ptr<AbstractExpression2>> copied_expressions;
//  copied_expressions.reserve(expressions.size());
//
//  for (const auto& expression : expressions) {
//    copied_expressions.emplace_back(expression_copy_and_adapt_to_different_lqp(*expression, node_mapping));
//  }
//
//  return copied_expressions;
//}
//
//std::shared_ptr<AbstractExpression2> expression_copy_and_adapt_to_different_lqp(
//const AbstractExpression2& expression,
//const LQPNodeMapping& node_mapping) {
//
//  auto copied_expression = expression.deep_copy();
//  expression_adapt_to_different_lqp(copied_expression, node_mapping);
//  return copied_expression;
//}

//void expression_adapt_to_different_lqp(
//  std::shared_ptr<AbstractExpression2>& expression,
//  const LQPNodeMapping& node_mapping){
//
//  visit_expression(expression, [&](auto& expression_ptr) {
//    if (expression_ptr->type != ExpressionType2::Column) return true;
//
//    const auto lqp_column_expression_ptr = std::dynamic_pointer_cast<LQPColumnExpression>(expression_ptr);
//    Assert(lqp_column_expression_ptr, "Asked to adapt expression in LQP, but encountered non-LQP ColumnExpression");
//
//    expression_ptr = expression_adapt_to_different_lqp(*lqp_column_expression_ptr, node_mapping);
//
//    return false;
//  });
//}
//
//std::shared_ptr<LQPColumnExpression> expression_adapt_to_different_lqp(
//const LQPColumnExpression& lqp_column_expression,
//const LQPNodeMapping& node_mapping) {
//  const auto node_mapping_iter = node_mapping.find(lqp_column_expression.column_reference.original_node());
//  Assert(node_mapping_iter != node_mapping.end(), "Couldn't find referenced node in NodeMapping");
//
//  LQPColumnReference adapted_column_reference{node_mapping_iter->second, lqp_column_expression.column_reference.original_column_id()};
//
//  return std::make_shared<LQPColumnExpression>(adapted_column_reference);
//}

std::string expression_column_names(const std::vector<std::shared_ptr<AbstractExpression2>> &expressions) {
  std::stringstream stream;
  for (auto expression_idx = size_t{0}; expression_idx < expressions.size(); ++expression_idx) {
    stream << expressions[expression_idx]->as_column_name();
    if (expression_idx + 1 < expressions.size()) {
      stream << ", ";
    }
  }
  return stream.str();
}


}  // namespace opossum
