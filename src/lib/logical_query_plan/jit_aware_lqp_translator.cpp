#include "jit_aware_lqp_translator.hpp"

#include <boost/range/adaptors.hpp>
#include <boost/range/combine.hpp>

#include <queue>
#include <unordered_set>

#include "aggregate_node.hpp"
#include "constant_mappings.hpp"
#include "jit_evaluation_helper.hpp"
#include "operators/jit_operator/operators/jit_aggregate.hpp"
#include "operators/jit_operator/operators/jit_filter.hpp"
#include "operators/jit_operator/operators/jit_read_tuple.hpp"
#include "operators/jit_operator/operators/jit_write_tuple.hpp"
#include "projection_node.hpp"
#include "storage/storage_manager.hpp"
#include "stored_table_node.hpp"

namespace opossum {

std::shared_ptr<AbstractOperator> JitAwareLQPTranslator::translate_node(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  if (JitEvaluationHelper::get().experiment().at("engine") == "opossum") {
    return LQPTranslator::translate_node(node);
  } else if (JitEvaluationHelper::get().experiment().at("engine") != "jit") {
    Fail("unknown query engine parameter");
  }

  uint32_t num_jittable_nodes{0};
  std::unordered_set<std::shared_ptr<AbstractLQPNode>> input_nodes;

  // Traverse query tree until a non-jitable nodes is found in each branch
  _breadth_first_search(node, [&](auto& current_node) {
    const auto is_root_node = current_node == node;
    if (_node_is_jittable(current_node, is_root_node)) {
      ++num_jittable_nodes;
      return true;
    } else {
      input_nodes.insert(current_node);
      return false;
    }
  });

  // It does not make sense to create a JitOperator for fewer than 2 LQP nodes,
  // but we may want a better heuristic here
  if (num_jittable_nodes < 2 || input_nodes.size() != 1) {
    return LQPTranslator::translate_node(node);
  }

  const auto input_node = *input_nodes.begin();

  auto jit_operator = std::make_shared<JitOperator>(translate_node(input_node));
  auto read_tuple = std::make_shared<JitReadTuple>();
  jit_operator->add_jit_operator(read_tuple);

  auto filter_node = node;
  while (filter_node != input_node && filter_node->type() != LQPNodeType::Predicate &&
         filter_node->type() != LQPNodeType::Union) {
    filter_node = filter_node->left_child();
  }

  // If we can reach the input node without encountering a UnionNode or PredicateNode,
  // there is no need to filter any tuples
  if (filter_node != input_node) {
    // However, if we do need to filter, we first convert the filter node to a JitExpression ...
    const auto expression = _translate_to_jit_expression(filter_node, *read_tuple, input_node);
    // make sure that expression get computed ...
    jit_operator->add_jit_operator(std::make_shared<JitCompute>(expression));
    // and then filter on the resulting boolean.
    jit_operator->add_jit_operator(std::make_shared<JitFilter>(expression->result()));
  }

  if (node->type() == LQPNodeType::Aggregate) {
    const auto aggregate_node = std::static_pointer_cast<AggregateNode>(node);

    auto aggregate = std::make_shared<JitAggregate>();
    const auto column_names = aggregate_node->output_column_names();
    const auto groupby_columns = aggregate_node->groupby_column_references();
    const auto aggregate_columns = aggregate_node->aggregate_expressions();
    const auto groupby_column_names = boost::adaptors::slice(column_names, 0, groupby_columns.size());
    const auto aggregate_column_names =
        boost::adaptors::slice(column_names, groupby_columns.size(), column_names.size());

    for (const auto& groupby_column : boost::combine(groupby_column_names, groupby_columns)) {
      const auto expression = _translate_to_jit_expression(groupby_column.get<1>(), *read_tuple, input_node);
      if (expression->expression_type() != ExpressionType::Column) {
        jit_operator->add_jit_operator(std::make_shared<JitCompute>(expression));
      }
      aggregate->add_groupby_column(groupby_column.get<0>(), expression->result());
    }

    for (const auto& aggregate_column : boost::combine(aggregate_column_names, aggregate_columns)) {
      const auto aggregate_expression = aggregate_column.get<1>();
      DebugAssert(aggregate_expression->type() == ExpressionType::Function, "Expression is not a function.");
      const auto expression = _translate_to_jit_expression(*aggregate_expression->aggregate_function_arguments()[0],
                                                           *read_tuple, input_node);
      if (expression->expression_type() != ExpressionType::Column) {
        jit_operator->add_jit_operator(std::make_shared<JitCompute>(expression));
      }
      aggregate->add_aggregate_column(aggregate_column.get<0>(), expression->result(),
                                      aggregate_expression->aggregate_function());
    }

    jit_operator->add_jit_operator(aggregate);
  } else {
    // Identify the top-most projection node to determine the output columns
    auto top_projection = node;
    while (top_projection != input_node && top_projection->type() != LQPNodeType::Projection) {
      top_projection = top_projection->left_child();
    }

    // Add a compute operator for each output column that computes the column value.
    auto write_tuple = std::make_shared<JitWriteTuple>();
    for (const auto& output_column :
         boost::combine(top_projection->output_column_names(), top_projection->output_column_references())) {
      const auto expression = _translate_to_jit_expression(output_column.get<1>(), *read_tuple, input_node);
      // It the JitExpression is of type ExpressionType::Column, there is no need to add a compute node, since it
      // would not compute anything anyway
      if (expression->expression_type() != ExpressionType::Column) {
        jit_operator->add_jit_operator(std::make_shared<JitCompute>(expression));
      }
      write_tuple->add_output_column(output_column.get<0>(), expression->result());
    }

    jit_operator->add_jit_operator(write_tuple);
  }

  return jit_operator;
}

std::shared_ptr<const JitExpression> JitAwareLQPTranslator::_translate_to_jit_expression(
    const std::shared_ptr<AbstractLQPNode>& node, JitReadTuple& jit_source,
    const std::shared_ptr<AbstractLQPNode>& input_node) const {
  std::shared_ptr<const JitExpression> left, right;
  switch (node->type()) {
    case LQPNodeType::Predicate:
      // If there are further conditions (either PredicateNodes of UnionNodes) down the line, we need a logical AND here
      if (_has_another_condition(node->left_child())) {
        left = _translate_to_jit_expression(std::dynamic_pointer_cast<PredicateNode>(node), jit_source, input_node);
        right = _translate_to_jit_expression(node->left_child(), jit_source, input_node);
        return std::make_shared<JitExpression>(left, ExpressionType::And, right, jit_source.add_temorary_value());
      } else {
        return _translate_to_jit_expression(std::dynamic_pointer_cast<PredicateNode>(node), jit_source, input_node);
      }

    case LQPNodeType::Union:
      left = _translate_to_jit_expression(node->left_child(), jit_source, input_node);
      right = _translate_to_jit_expression(node->right_child(), jit_source, input_node);
      return std::make_shared<JitExpression>(left, ExpressionType::Or, right, jit_source.add_temorary_value());

    case LQPNodeType::Projection:
      // We don't care about projection nodes here, since they do not perform any tuple filtering
      return _translate_to_jit_expression(node->left_child(), jit_source, input_node);

    default:
      Fail("unreachable");
  }
}

std::shared_ptr<const JitExpression> JitAwareLQPTranslator::_translate_to_jit_expression(
    const std::shared_ptr<PredicateNode>& node, JitReadTuple& jit_source,
    const std::shared_ptr<AbstractLQPNode>& input_node) const {
  auto condition = predicate_condition_to_expression_type.at(node->predicate_condition());
  auto left = _translate_to_jit_expression(node->column_reference(), jit_source, input_node);
  std::shared_ptr<const JitExpression> right;
  std::shared_ptr<const JitExpression> between_value2;
  std::shared_ptr<const JitExpression> between_greater_than_equals;
  std::shared_ptr<const JitExpression> between_less_than_equals;

  switch (condition) {
    /* Binary predicates */
    case ExpressionType::Equals:
    case ExpressionType::NotEquals:
    case ExpressionType::LessThan:
    case ExpressionType::LessThanEquals:
    case ExpressionType::GreaterThan:
    case ExpressionType::GreaterThanEquals:
    case ExpressionType::Like:
    case ExpressionType::NotLike:
      right = _translate_to_jit_expression(node->value(), jit_source, input_node);
      return std::make_shared<JitExpression>(left, condition, right, jit_source.add_temorary_value());
    /* Unary predicates */
    case ExpressionType::IsNull:
    case ExpressionType::IsNotNull:
      return std::make_shared<JitExpression>(left, condition, jit_source.add_temorary_value());
    /* Between */
    case ExpressionType::Between:
      right = _translate_to_jit_expression(node->value(), jit_source, input_node);
      between_value2 = _translate_to_jit_expression(node->value2().value(), jit_source, input_node);
      between_greater_than_equals = std::make_shared<JitExpression>(left, ExpressionType::GreaterThanEquals, right, jit_source.add_temorary_value());
      between_less_than_equals = std::make_shared<JitExpression>(left, ExpressionType::LessThanEquals, between_value2, jit_source.add_temorary_value());
      return std::make_shared<JitExpression>(between_greater_than_equals, ExpressionType::And, between_less_than_equals, jit_source.add_temorary_value());
    default:
      Fail("invalid expression type");
  }
}

std::shared_ptr<const JitExpression> JitAwareLQPTranslator::_translate_to_jit_expression(
    const LQPExpression& lqp_expression, JitReadTuple& jit_source,
    const std::shared_ptr<AbstractLQPNode>& input_node) const {
  std::shared_ptr<const JitExpression> left, right;
  switch (lqp_expression.type()) {
    case ExpressionType::Literal:
      return _translate_to_jit_expression(lqp_expression.value(), jit_source, input_node);

    case ExpressionType::Column:
      return _translate_to_jit_expression(lqp_expression.column_reference(), jit_source, input_node);

    /* Binary operators */
    case ExpressionType::Addition:
    case ExpressionType::Subtraction:
    case ExpressionType::Multiplication:
    case ExpressionType::Division:
    case ExpressionType::Modulo:
    case ExpressionType::Power:
    case ExpressionType::Equals:
    case ExpressionType::NotEquals:
    case ExpressionType::LessThan:
    case ExpressionType::LessThanEquals:
    case ExpressionType::GreaterThan:
    case ExpressionType::GreaterThanEquals:
    case ExpressionType::Like:
    case ExpressionType::NotLike:
    case ExpressionType::And:
    case ExpressionType::Or:
      left = _translate_to_jit_expression(*lqp_expression.left_child(), jit_source, input_node);
      right = _translate_to_jit_expression(*lqp_expression.right_child(), jit_source, input_node);
      return std::make_shared<JitExpression>(left, lqp_expression.type(), right, jit_source.add_temorary_value());

    /* Unary operators */
    case ExpressionType::Not:
    case ExpressionType::IsNull:
    case ExpressionType::IsNotNull:
      left = _translate_to_jit_expression(*lqp_expression.left_child(), jit_source, input_node);
      return std::make_shared<JitExpression>(left, lqp_expression.type(), jit_source.add_temorary_value());

    default:
      Fail("unexpected expression type");
  }
}

std::shared_ptr<const JitExpression> JitAwareLQPTranslator::_translate_to_jit_expression(
    const LQPColumnReference& lqp_column_reference, JitReadTuple& jit_source,
    const std::shared_ptr<AbstractLQPNode>& input_node) const {
  const auto column_id = input_node->find_output_column_id(lqp_column_reference);
  const auto stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(lqp_column_reference.original_node());
  const auto projection_node = std::dynamic_pointer_cast<const ProjectionNode>(lqp_column_reference.original_node());

  if (stored_table_node) {
    // we reached a "raw" data column and register it as an input column
    const auto table_name = stored_table_node->table_name();
    const auto table = StorageManager::get().get_table(table_name);
    const auto data_type = table->column_type(lqp_column_reference.original_column_id());
    const auto is_nullable = table->column_is_nullable(lqp_column_reference.original_column_id());
    const auto tuple_value = jit_source.add_input_column(data_type, is_nullable, column_id.value());
    return std::make_shared<JitExpression>(tuple_value);
  } else if (projection_node) {
    // if the LQPColumnReference references a computed column, we need to compute that expression as well
    const auto lqp_expression = projection_node->column_expressions()[lqp_column_reference.original_column_id()];
    return _translate_to_jit_expression(*lqp_expression, jit_source, input_node);
  } else {
    Fail("could not get column from anywhere");
  }
}

std::shared_ptr<const JitExpression> JitAwareLQPTranslator::_translate_to_jit_expression(
    const AllParameterVariant& value, JitReadTuple& jit_source,
    const std::shared_ptr<AbstractLQPNode>& input_node) const {
  if (is_lqp_column_reference(value)) {
    return _translate_to_jit_expression(boost::get<LQPColumnReference>(value), jit_source, input_node);
  } else if (is_variant(value)) {
    const auto variant = boost::get<AllTypeVariant>(value);
    const auto tuple_value = jit_source.add_literal_value(variant);
    return std::make_shared<JitExpression>(tuple_value);
  } else {
    Fail("unexpected parameter type");
  }
}

bool JitAwareLQPTranslator::_has_another_condition(const std::shared_ptr<AbstractLQPNode>& node) const {
  auto current_node = node;
  while (current_node->type() == LQPNodeType::Projection) {
    current_node = current_node->left_child();
  }
  return current_node->type() == LQPNodeType::Predicate || current_node->type() == LQPNodeType::Union;
}

bool JitAwareLQPTranslator::_node_is_jittable(const std::shared_ptr<AbstractLQPNode>& node,
                                              const bool allow_aggregate_node) const {
  switch (node->type()) {
    case LQPNodeType::Aggregate:
      return allow_aggregate_node;
    case LQPNodeType::Predicate:
      return std::dynamic_pointer_cast<PredicateNode>(node)->scan_type() == ScanType::TableScan &&
             std::dynamic_pointer_cast<PredicateNode>(node)->predicate_condition() != PredicateCondition::Between;
    case LQPNodeType::Projection:
    case LQPNodeType::Union:
      return true;
      break;
    default:
      return false;
      break;
  }
}

void JitAwareLQPTranslator::_breadth_first_search(
    const std::shared_ptr<AbstractLQPNode>& node,
    std::function<bool(const std::shared_ptr<AbstractLQPNode>&)> func) const {
  std::unordered_set<std::shared_ptr<const AbstractLQPNode>> visited;
  std::queue<std::shared_ptr<AbstractLQPNode>> queue({node});

  while (!queue.empty()) {
    auto current_node = queue.front();
    queue.pop();

    if (!current_node || visited.count(current_node)) {
      continue;
    }
    visited.insert(current_node);

    if (func(current_node)) {
      queue.push(current_node->left_child());
      queue.push(current_node->right_child());
    }
  }
}

}  // namespace opossum
