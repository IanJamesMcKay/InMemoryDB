#include "sql_translator.hpp"

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "abstract_expression.hpp"
#include "constant_mappings.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/create_view_node.hpp"
#include "logical_query_plan/delete_node.hpp"
#include "logical_query_plan/drop_view_node.hpp"
#include "logical_query_plan/dummy_table_node.hpp"
#include "logical_query_plan/insert_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/lqp_expression.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/show_columns_node.hpp"
#include "logical_query_plan/show_tables_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/update_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "sql/hsql_expr_translator.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "util/sqlhelper.h"
#include "utils/assert.hpp"

#include "SQLParser.h"

namespace opossum {

PredicateCondition translate_operator_type_to_predicate_condition(const hsql::OperatorType operator_type) {
  static const std::unordered_map<const hsql::OperatorType, const PredicateCondition> operator_to_predicate_condition =
      {{hsql::kOpEquals, PredicateCondition::Equals},
       {hsql::kOpNotEquals, PredicateCondition::NotEquals},
       {hsql::kOpGreater, PredicateCondition::GreaterThan},
       {hsql::kOpGreaterEq, PredicateCondition::GreaterThanEquals},
       {hsql::kOpLess, PredicateCondition::LessThan},
       {hsql::kOpLessEq, PredicateCondition::LessThanEquals},
       {hsql::kOpBetween, PredicateCondition::Between},
       {hsql::kOpLike, PredicateCondition::Like},
       {hsql::kOpNotLike, PredicateCondition::NotLike},
       {hsql::kOpIsNull, PredicateCondition::IsNull},
       {hsql::kOpIn, PredicateCondition::In}};

  auto it = operator_to_predicate_condition.find(operator_type);
  DebugAssert(it != operator_to_predicate_condition.end(), "Filter expression clause operator is not yet supported.");
  return it->second;
}

PredicateCondition get_predicate_condition_for_reverse_order(const PredicateCondition predicate_condition) {
  /**
   * If we switch the sides for the expressions, we might have to change the operator that is used for the predicate.
   * This function returns the respective PredicateCondition.
   *
   * Example:
   *     SELECT * FROM t WHERE 1 > a
   *  -> SELECT * FROM t WHERE a < 1
   *
   *    but:
   *     SELECT * FROM t WHERE 1 = a
   *  -> SELECT * FROM t WHERE a = 1
   */
  static const std::unordered_map<const PredicateCondition, const PredicateCondition>
      predicate_condition_for_reverse_order = {
          {PredicateCondition::GreaterThan, PredicateCondition::LessThan},
          {PredicateCondition::LessThan, PredicateCondition::GreaterThan},
          {PredicateCondition::GreaterThanEquals, PredicateCondition::LessThanEquals},
          {PredicateCondition::LessThanEquals, PredicateCondition::GreaterThanEquals}};

  auto it = predicate_condition_for_reverse_order.find(predicate_condition);
  if (it != predicate_condition_for_reverse_order.end()) {
    return it->second;
  }

  return predicate_condition;
}

JoinMode translate_join_type_to_join_mode(const hsql::JoinType join_type) {
  static const std::unordered_map<const hsql::JoinType, const JoinMode> join_type_to_mode = {
      {hsql::kJoinInner, JoinMode::Inner}, {hsql::kJoinFull, JoinMode::Outer},      {hsql::kJoinLeft, JoinMode::Left},
      {hsql::kJoinRight, JoinMode::Right}, {hsql::kJoinNatural, JoinMode::Natural}, {hsql::kJoinCross, JoinMode::Cross},
  };

  auto it = join_type_to_mode.find(join_type);
  DebugAssert(it != join_type_to_mode.end(), "Unable to handle join type.");
  return it->second;
}

std::vector<std::shared_ptr<AbstractLQPNode>> SQLTranslator::translate_parse_result(
    const hsql::SQLParserResult& result) {
  std::vector<std::shared_ptr<AbstractLQPNode>> result_nodes;
  const std::vector<hsql::SQLStatement*>& statements = result.getStatements();

  for (const hsql::SQLStatement* stmt : statements) {
    auto result_node = translate_statement(*stmt);
    result_nodes.push_back(result_node);
  }

  return result_nodes;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::translate_statement(const hsql::SQLStatement& statement) {
  switch (statement.type()) {
    case hsql::kStmtSelect:
      return _translate_select(static_cast<const hsql::SelectStatement&>(statement));
    case hsql::kStmtInsert:
      return _translate_insert(static_cast<const hsql::InsertStatement&>(statement));
    case hsql::kStmtDelete:
      return _translate_delete(static_cast<const hsql::DeleteStatement&>(statement));
    case hsql::kStmtUpdate:
      return _translate_update(static_cast<const hsql::UpdateStatement&>(statement));
    case hsql::kStmtShow:
      return _translate_show(static_cast<const hsql::ShowStatement&>(statement));
    case hsql::kStmtCreate:
      return _translate_create(static_cast<const hsql::CreateStatement&>(statement));
    case hsql::kStmtDrop:
      return _translate_drop(static_cast<const hsql::DropStatement&>(statement));
    default:
      Fail("SQL statement type not supported");
  }
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_insert(const hsql::InsertStatement& insert) {
  const std::string table_name{insert.tableName};

  AssertInput(StorageManager::get().has_table(table_name), "Insert: Invalid table name: " + table_name);

  auto target_table = StorageManager::get().get_table(table_name);


  std::shared_ptr<AbstractLQPNode> current_result_node;

  // Check for SELECT ... INTO .. query
  if (insert.type == hsql::kInsertSelect) {
    AssertInput(insert.select != nullptr, "Insert: no select statement given");
    current_result_node = _translate_select(*insert.select);
  } else {
    current_result_node = DummyTableNode::make();
  }

  // Lambda to compare a DataType to the type of an hqsl::Expr
  auto literal_matches_data_type = [](const hsql::Expr& expr, const DataType& column_type) {
    switch (column_type) {
      case DataType::Int:
        return expr.isType(hsql::kExprLiteralInt);
      case DataType::Long:
        return expr.isType(hsql::kExprLiteralInt);
      case DataType::Float:
        return expr.isType(hsql::kExprLiteralFloat);
      case DataType::Double:
        return expr.isType(hsql::kExprLiteralFloat);
      case DataType::String:
        return expr.isType(hsql::kExprLiteralString);
      case DataType::Null:
        return expr.isType(hsql::kExprLiteralNull);
      default:
        return false;
    }
  };

  // Lambda to compare a vector of DataType to the types of a vector of hqsl::Expr
  auto data_types_match_expr_types = [&](const std::vector<DataType>& data_types,
                                         const std::vector<hsql::Expr*>& expressions) {
    auto data_types_it = data_types.begin();
    auto expressions_it = expressions.begin();

    while (data_types_it != data_types.end() && expressions_it != expressions.end()) {
      if (!literal_matches_data_type(*(*expressions_it), *data_types_it) &&
          !(*expressions_it)->isType(hsql::kExprParameter)) {
        // if this is a PreparedStatement, we don't have a mismatch
        return false;
      }
      data_types_it++;
      expressions_it++;
    }

    return true;
  };

  if (!insert.columns) {
    // No column order given. Assuming all columns in regular order.
    // For SELECT ... INTO we are basically done because can use the above node as input.

    if (insert.type == hsql::kInsertValues) {
      AssertInput(insert.values != nullptr, "Insert: no values given");

      Assert(data_types_match_expr_types(target_table->column_data_types(), *insert.values),
             "Insert: Column type mismatch");

      // In the case of INSERT ... VALUES (...), simply create a
      current_result_node = _translate_projection(*insert.values, current_result_node);
    }

    Assert(current_result_node->output_column_count() == target_table->column_count(), "Insert: Column count mismatch");
  } else {
    // Certain columns have been specified. In this case we create a new expression list
    // for the Projection, so that it contains as many columns as the target table.

    // pre-fill new projection list with NULLs
    std::vector<std::shared_ptr<LQPExpression>> projections(target_table->column_count(),
                                                            LQPExpression::create_literal(NULL_VALUE));

    ColumnID insert_column_index{0};
    for (const auto& column_name : *insert.columns) {
      // retrieve correct ColumnID from the target table
      auto column_id = target_table->column_id_by_name(column_name);

      if (insert.type == hsql::kInsertValues) {
        // when inserting values, simply translate the literal expression
        const auto& hsql_expr = *(*insert.values)[insert_column_index];

        Assert(literal_matches_data_type(hsql_expr, target_table->column_data_types()[column_id]),
               "Insert: Column type mismatch");

        projections[column_id] = HSQLExprTranslator::to_lqp_expression(hsql_expr, nullptr);
      } else {
        DebugAssert(insert.type == hsql::kInsertSelect, "Unexpected Insert type");
        DebugAssert(insert_column_index < current_result_node->output_column_count(), "ColumnID out of range");
        // when projecting from another table, create a column reference expression
        projections[column_id] =
            LQPExpression::create_column(current_result_node->output_column_references()[insert_column_index]);
      }

      ++insert_column_index;
    }

    // create projection and add to the node chain
    auto projection_node = ProjectionNode::make(projections);
    projection_node->set_left_input(current_result_node);

    current_result_node = projection_node;
  }

  auto insert_node = InsertNode::make(table_name);
  insert_node->set_left_input(current_result_node);

  return insert_node;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_delete(const hsql::DeleteStatement& del) {
  std::shared_ptr<AbstractLQPNode> current_result_node = StoredTableNode::make(del.tableName);
  current_result_node = _validate_if_active(current_result_node);
  if (del.expr) {
    current_result_node = _translate_where(*del.expr, current_result_node);
  }

  auto delete_node = DeleteNode::make(del.tableName);
  delete_node->set_left_input(current_result_node);

  return delete_node;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_update(const hsql::UpdateStatement& update) {
  std::shared_ptr<AbstractLQPNode> current_values_node = _translate_table_ref(*update.table);
  if (update.where) {
    current_values_node = _translate_where(*update.where, current_values_node);
  }

  // The update operator wants ReferenceColumns on its left side
  // TODO(anyone): fix this
  Assert(!std::dynamic_pointer_cast<StoredTableNode>(current_values_node),
         "Unconditional updates are currently not supported");

  std::vector<std::shared_ptr<LQPExpression>> update_expressions;
  update_expressions.reserve(current_values_node->output_column_count());

  // pre-fill with regular column references
  for (ColumnID column_idx{0}; column_idx < current_values_node->output_column_count(); ++column_idx) {
    update_expressions.emplace_back(
        LQPExpression::create_column(current_values_node->output_column_references()[column_idx]));
  }

  // now update with new values
  for (auto& sql_expr : *update.updates) {
    const auto named_column_ref = QualifiedColumnName{sql_expr->column, std::nullopt};
    const auto column_reference = current_values_node->get_column(named_column_ref);
    const auto column_id = current_values_node->get_output_column_id(column_reference);

    auto expr = HSQLExprTranslator::to_lqp_expression(*sql_expr->value, current_values_node);
    update_expressions[column_id] = expr;
  }

  std::shared_ptr<AbstractLQPNode> update_node = UpdateNode::make((update.table)->name, update_expressions);
  update_node->set_left_input(current_values_node);

  return update_node;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_select(const hsql::SelectStatement& select) {
  // SQL Orders of Operations: http://www.bennadel.com/blog/70-sql-query-order-of-operations.htm
  // 1. FROM clause (incl. JOINs and subselects that are part of this)
  // 2. WHERE clause
  // 3. GROUP BY clause
  // 4. HAVING clause
  // 5. SELECT clause
  // 6. UNION clause
  // 7. ORDER BY clause
  // 8. LIMIT clause

  auto current_result_node = _translate_table_ref(*select.fromTable);

  if (select.whereClause != nullptr) {
    current_result_node = _translate_where(*select.whereClause, current_result_node);
  }

  AssertInput(select.selectList != nullptr, "SELECT list needs to exist");
  DebugAssert(!select.selectList->empty(), "SELECT list needs to have entries");

  Assert(!select.selectDistinct, "DISTINCT is not yet supported");

  // If the query has a GROUP BY clause or if it has aggregates, we do not need a top-level projection
  // because all elements must either be aggregate functions or columns of the GROUP BY clause,
  // so the Aggregate operator will handle them.
  auto is_aggregate = select.groupBy != nullptr;
  if (!is_aggregate) {
    for (auto* expr : *select.selectList) {
      // TODO(anybody): Only consider aggregate functions here (i.e., SUM, COUNT, etc. - but not CONCAT, ...).
      if (expr->isType(hsql::kExprFunctionRef)) {
        is_aggregate = true;
        break;
      }
    }
  }

  if (is_aggregate) {
    current_result_node = _translate_aggregate(select, current_result_node);
  } else {
    current_result_node = _translate_projection(*select.selectList, current_result_node);
  }

  Assert(select.unionSelect == nullptr, "Set operations (UNION/INTERSECT/...) are not supported yet");

  if (select.order != nullptr) {
    current_result_node = _translate_order_by(*select.order, current_result_node);
  }

  // TODO(anybody): Translate TOP.
  if (select.limit != nullptr) {
    current_result_node = _translate_limit(*select.limit, current_result_node);
  }

  return current_result_node;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_join(const hsql::JoinDefinition& join) {
  const auto join_mode = translate_join_type_to_join_mode(join.type);

  if (join_mode == JoinMode::Natural) {
    return _translate_natural_join(join);
  }

  auto left_node = _translate_table_ref(*join.left);
  auto right_node = _translate_table_ref(*join.right);

  /* In general there is currently no support for multiple join conditions using AND and OR.
   * The current implementation expects a single join condition in a set of conjunctive
   * clauses. The remaining clauses are expected to be relevant for only one of
   * the join partners and are therefore converted into predicates inserted in between the
   * source relations and the actual join node.
   * See TPC-H 13 for an example query.
   */
  std::vector<const hsql::Expr*> condition_list;
  _flatten_conjunctive_expression_tree(join.condition, condition_list);

  std::vector<const hsql::Expr *> join_conditions, left_conditions, right_conditions;
  _get_sides_from_operator_expressions(condition_list, left_node, right_node, join_conditions, left_conditions,
                                       right_conditions);

  Assert(join_conditions.size() == 1,
         "No or more than one possible join condition (= condition involving columns of different origin) supplied. "
         "The join operator does currently not support this.");
  const auto join_condition = join_conditions[0];

  // The Join operators only support simple comparisons for now.
  switch (join_condition->opType) {
    case hsql::kOpEquals:
    case hsql::kOpNotEquals:
    case hsql::kOpLess:
    case hsql::kOpLessEq:
    case hsql::kOpGreater:
    case hsql::kOpGreaterEq:
      break;
    default:
      Fail("Join condition must be a simple comparison operator.");
  }

  auto left_qualified_column_name = HSQLExprTranslator::to_qualified_column_name(*join_condition->expr);
  auto right_qualified_column_name = HSQLExprTranslator::to_qualified_column_name(*join_condition->expr2);
  auto predicate_condition = translate_operator_type_to_predicate_condition(join_condition->opType);

  const auto left_in_left_node = _get_side(left_node, right_node, join_condition->expr) == LQPInputSide::Left;
  if (!left_in_left_node) {
    std::swap(left_qualified_column_name, right_qualified_column_name);
    predicate_condition = get_predicate_condition_for_reverse_order(predicate_condition);
  }

  const auto column_references = std::make_pair(*left_node->find_column(left_qualified_column_name),
                                                *right_node->find_column(right_qualified_column_name));
  auto join_node = JoinNode::make(join_mode, column_references, predicate_condition);
  join_node->set_left_input(left_node);
  join_node->set_right_input(right_node);
  _insert_nonjoin_predicates(join_node, left_conditions, right_conditions);
  return join_node;
}

void SQLTranslator::_insert_nonjoin_predicates(const std::shared_ptr<JoinNode>& join_node,
                                               const std::vector<const hsql::Expr*>& left_conditions,
                                               const std::vector<const hsql::Expr*>& right_conditions) {
  /*
   * Concerning the inner join, no restrictions apply to non-join-predicates.
   * Both a push-down or push-up (placing predicate nodes in between one of the
   * join input nodes or placing them after the join, respectively) would be
   * legit.
   *
   * For left outer and right outer join types, the situation is different.
   * In a left outer join the left side is known as "preserve-side" and the
   * right side as "null-supplying side" (vice versa for right outer join).
   *
   * Filtering on the preserve-side cannot be external to the join since all
   * tuples of the preserve-side are part of the outer-join's ouptut relation.
   * All predicates in the join condition simply state if the RHS tuple
   * components are padded with NULLs or with matching tuples from RHS.
   *
   * For instance, the query "select * from A left join B on a = 404;"
   * contains all tuples of A padded with NULLs representing the tuple
   * components of B, even if the value '404' is not present.
   */
  if (join_node->join_mode() == JoinMode::Inner) {
    _insert_predicates_before(join_node, left_conditions);
    _insert_predicates_before(join_node, right_conditions, LQPInputSide::Right);
  } else if (join_node->join_mode() == JoinMode::Left) {
    Assert(left_conditions.empty(), "Multiple join conditions not supported (preserve side)");
    _insert_predicates_before(join_node, right_conditions, LQPInputSide::Right);
  } else if (join_node->join_mode() == JoinMode::Right) {
    _insert_predicates_before(join_node, left_conditions);
    Assert(right_conditions.empty(), "Multiple join conditions not supported (preserve side)");
  } else {
    Assert(left_conditions.empty() && right_conditions.empty(),
           "Cannot translate nonjoin conditions for this join mode");
  }
}

LQPInputSide SQLTranslator::_get_side(const std::shared_ptr<AbstractLQPNode>& left_node,
                                      const std::shared_ptr<AbstractLQPNode>& right_node, const hsql::Expr* expr) {
  DebugAssert(expr && expr->type == hsql::kExprColumnRef, "only applicable to columnar expressions");
  const auto qualified = HSQLExprTranslator::to_qualified_column_name(*expr);
  const auto left = left_node->find_column(qualified);
  const auto right = right_node->find_column(qualified);
  AssertInput(left.has_value() ^ right.has_value(),
         "Column '" + qualified.as_string() + "' was not found or is not unique");
  return left ? LQPInputSide::Left : LQPInputSide::Right;
}

void SQLTranslator::_get_sides_from_operator_expressions(const std::vector<const hsql::Expr*>& operators,
                                                         const std::shared_ptr<AbstractLQPNode>& left_node,
                                                         const std::shared_ptr<AbstractLQPNode>& right_node,
                                                         std::vector<const hsql::Expr*>& both,
                                                         std::vector<const hsql::Expr*>& left,
                                                         std::vector<const hsql::Expr*>& right) {
  for (const auto* op : operators) {
    const bool left_is_column = op->expr && op->expr->type == hsql::kExprColumnRef;
    const bool right_is_column = op->expr2 && op->expr2->type == hsql::kExprColumnRef;

    if (left_is_column && right_is_column &&
        _get_side(left_node, right_node, op->expr) != _get_side(left_node, right_node, op->expr2)) {
      both.push_back(op);
      continue;
    }

    if (left_is_column) {
      auto& side = (_get_side(left_node, right_node, op->expr) == LQPInputSide::Left) ? left : right;
      side.push_back(op);
    } else if (right_is_column) {
      auto& side = (_get_side(left_node, right_node, op->expr2) == LQPInputSide::Left) ? left : right;
      side.push_back(op);
    } else {
      Fail("Cannot determine a side for an expression which does not contain any columns");
    }
  }
}

void SQLTranslator::_insert_predicates_before(const std::shared_ptr<AbstractLQPNode>& node,
                                              const std::vector<const hsql::Expr*>& conditions,
                                              const LQPInputSide side) {
  if (conditions.empty()) {
    return;
  }

  auto last_node = node->input(side);
  for (const auto* condition : conditions) {
    last_node = _translate_predicate(
        *condition, false, [&node](const auto& expr) { return HSQLExprTranslator::to_column_reference(expr, node); },
        last_node);
  }

  DebugAssert(node->input(side),
              "Unable to insert predicates in between since the node does not have an input on that side."
              "Is the node's input side and type correct?");
  node->set_input(side, last_node);
}

void SQLTranslator::_flatten_conjunctive_expression_tree(const hsql::Expr* expression,
                                                         std::vector<const hsql::Expr*>& expressions) {
  if (expression->opType == hsql::kOpAnd) {
    Assert(expression->expr && expression->expr2, "kOpAnd should have two children expressions");
    _flatten_conjunctive_expression_tree(expression->expr, expressions);
    _flatten_conjunctive_expression_tree(expression->expr2, expressions);
    return;
  }

  Assert(expression->opType != hsql::kOpOr, "Disjunction is not supported.");
  expressions.push_back(expression);
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_natural_join(const hsql::JoinDefinition& join) {
  DebugAssert(translate_join_type_to_join_mode(join.type) == JoinMode::Natural, "join must be a natural join");

  const auto& left_node = _translate_table_ref(*join.left);
  const auto& right_node = _translate_table_ref(*join.right);

  // we need copies that we can sort on.
  auto left_column_names = left_node->output_column_names();
  auto right_column_names = right_node->output_column_names();

  std::sort(left_column_names.begin(), left_column_names.end());
  std::sort(right_column_names.begin(), right_column_names.end());

  std::vector<std::string> join_column_names;
  std::set_intersection(left_column_names.begin(), left_column_names.end(), right_column_names.begin(),
                        right_column_names.end(), std::back_inserter(join_column_names));

  Assert(!join_column_names.empty(), "No matching columns for natural join found");

  std::shared_ptr<AbstractLQPNode> return_node = JoinNode::make(JoinMode::Cross);
  return_node->set_left_input(left_node);
  return_node->set_right_input(right_node);

  for (const auto& join_column_name : join_column_names) {
    auto left_column_reference = left_node->get_column({join_column_name});
    auto right_column_reference = right_node->get_column({join_column_name});
    auto predicate = PredicateNode::make(left_column_reference, PredicateCondition::Equals, right_column_reference);
    predicate->set_left_input(return_node);
    return_node = predicate;
  }

  // We need to collect the column origins so that we can remove the duplicate columns used in the join condition
  std::vector<LQPColumnReference> column_references;
  for (auto column_id = ColumnID{0u}; column_id < return_node->output_column_count(); ++column_id) {
    const auto& column_name = return_node->output_column_names()[column_id];

    if (static_cast<size_t>(column_id) >= left_node->output_column_count() &&
        std::find(join_column_names.begin(), join_column_names.end(), column_name) != join_column_names.end()) {
      continue;
    }

    const auto& column_reference = return_node->output_column_references()[column_id];
    column_references.emplace_back(column_reference);
  }

  const auto column_expressions = LQPExpression::create_columns(column_references);

  auto projection = ProjectionNode::make(column_expressions);
  projection->set_left_input(return_node);

  return projection;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_cross_product(const std::vector<hsql::TableRef*>& tables) {
  DebugAssert(!tables.empty(), "Cannot translate cross product without tables");
  auto product = _translate_table_ref(*tables.front());

  for (size_t i = 1; i < tables.size(); i++) {
    auto next_node = _translate_table_ref(*tables[i]);

    auto new_product = JoinNode::make(JoinMode::Cross);
    new_product->set_left_input(product);
    new_product->set_right_input(next_node);

    product = new_product;
  }

  return product;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_table_ref_alias(const std::shared_ptr<AbstractLQPNode>& node,
                                                                           const hsql::TableRef& table) {
  // Add a new projection node for table alias with column alias declarations
  // e.g. select * from foo as bar(a, b)
  if (!table.alias || !table.alias->columns) {
    return node;
  }

  DebugAssert(table.type == hsql::kTableName || table.type == hsql::kTableSelect,
              "Aliases are only applicable to table names and subselects");

  // To stick to the sql standard there must be an alias for every column of the renamed table
  // https://www.contrib.andrew.cmu.edu/~shadow/sql/sql1992.txt 6.3
  AssertInput(table.alias->columns->size() == node->output_column_count(),
         "The number of column aliases must match the number of columns");

  auto& column_references = node->output_column_references();
  std::vector<std::shared_ptr<LQPExpression>> projections;
  projections.reserve(table.alias->columns->size());
  size_t column_id = 0;
  for (const char* column : *(table.alias->columns)) {
    projections.push_back(LQPExpression::create_column(column_references.at(column_id), std::string(column)));
    ++column_id;
  }
  auto projection_node = ProjectionNode::make(projections);
  projection_node->set_left_input(node);
  return projection_node;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_table_ref(const hsql::TableRef& table) {
  auto alias = table.alias ? std::optional<std::string>(table.alias->name) : std::nullopt;
  std::shared_ptr<AbstractLQPNode> node;
  switch (table.type) {
    case hsql::kTableName:
      if (StorageManager::get().has_table(table.name)) {
        /**
         * Make sure the ALIAS is applied to the StoredTableNode and not the ValidateNode
         */
        auto stored_table_node = StoredTableNode::make(table.name);
        stored_table_node->set_alias(alias);
        return _translate_table_ref_alias(_validate_if_active(stored_table_node), table);
      } else if (StorageManager::get().has_view(table.name)) {
        node = StorageManager::get().get_view(table.name);
        Assert(!_validate || node->subplan_is_validated(), "Trying to add non-validated view to validated query");
      } else {
        throw InvalidInputException(std::string("Did not find a table or view with name ") + table.name);
      }
      break;
    case hsql::kTableSelect:
      node = _translate_select(*table.select);
      Assert(alias, "Every derived table must have its own alias");
      node = _translate_table_ref_alias(node, table);
      break;
    case hsql::kTableJoin:
      node = _translate_join(*table.join);
      break;
    case hsql::kTableCrossProduct:
      node = _translate_cross_product(*table.list);
      break;
    default:
      Fail("Unable to translate source table.");
  }

  node->set_alias(alias);
  return node;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_where(const hsql::Expr& expr,
                                                                 const std::shared_ptr<AbstractLQPNode>& input_node) {
  DebugAssert(expr.isType(hsql::kExprOperator), "Filter expression clause has to be of type operator!");

  /**
   * If the expression is a nested expression, recursively resolve
   */
  if (expr.opType == hsql::kOpOr) {
    auto union_unique_node = UnionNode::make(UnionMode::Positions);
    union_unique_node->set_left_input(_translate_where(*expr.expr, input_node));
    union_unique_node->set_right_input(_translate_where(*expr.expr2, input_node));
    return union_unique_node;
  }

  if (expr.opType == hsql::kOpAnd) {
    auto filter_node = _translate_where(*expr.expr, input_node);
    return _translate_where(*expr.expr2, filter_node);
  }

  return _translate_predicate(
      expr, false,
      [&](const hsql::Expr& hsql_expr) { return HSQLExprTranslator::to_column_reference(hsql_expr, input_node); },
      input_node);
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_having(const hsql::Expr& expr,
                                                                  const std::shared_ptr<AggregateNode>& aggregate_node,
                                                                  const std::shared_ptr<AbstractLQPNode>& input_node) {
  DebugAssert(expr.isType(hsql::kExprOperator), "Filter expression clause has to be of type operator!");

  if (expr.opType == hsql::kOpOr) {
    auto union_unique_node = UnionNode::make(UnionMode::Positions);
    union_unique_node->set_left_input(_translate_having(*expr.expr, aggregate_node, input_node));
    union_unique_node->set_right_input(_translate_having(*expr.expr2, aggregate_node, input_node));
    return union_unique_node;
  }

  if (expr.opType == hsql::kOpAnd) {
    auto filter_node = _translate_having(*expr.expr, aggregate_node, input_node);
    return _translate_having(*expr.expr2, aggregate_node, filter_node);
  }

  return _translate_predicate(expr, true,
                              [&](const hsql::Expr& hsql_expr) {
                                const auto column_operand_expression =
                                    HSQLExprTranslator::to_lqp_expression(hsql_expr, aggregate_node->left_input());
                                return aggregate_node->get_column_by_expression(column_operand_expression);
                              },
                              input_node);
}

/**
 * Retrieves all aggregate functions used by the HAVING clause.
 * This is use by _translate_having to add missing aggregations to the Aggregate operator.
 */
std::vector<std::shared_ptr<LQPExpression>> SQLTranslator::_retrieve_having_aggregates(
    const hsql::Expr& expr, const std::shared_ptr<AbstractLQPNode>& input_node) {
  std::vector<std::shared_ptr<LQPExpression>> expressions;

  if (expr.type == hsql::kExprFunctionRef) {
    // We found an aggregate function. Translate and add to the list
    auto translated = HSQLExprTranslator::to_lqp_expression(expr, input_node);

    if (translated->type() == ExpressionType::Function) {
      expressions.emplace_back(translated);
    }

    return expressions;
  }

  // Check for more aggregate functions recursively
  if (expr.expr) {
    auto left_expressions = _retrieve_having_aggregates(*expr.expr, input_node);
    expressions.insert(expressions.end(), left_expressions.begin(), left_expressions.end());
  }

  if (expr.expr2) {
    auto right_expressions = _retrieve_having_aggregates(*expr.expr2, input_node);
    expressions.insert(expressions.end(), right_expressions.begin(), right_expressions.end());
  }

  return expressions;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_aggregate(
    const hsql::SelectStatement& select, const std::shared_ptr<AbstractLQPNode>& input_node) {
  /**
   * This function creates the following node structure:
   *
   * input_node -> [groupby_aliasing_node] -> aggregate_node -> {having_node}* -> projection_node
   *
   * - the aggregate_node creates aggregate and groupby columns.
   * - the groupby_aliasing_node is temporary and allows for resolving GroupByColumns that were assigned an ALIAS in
   *        the SELECT list. It will be removed again after the GroupByColumns have been resolved
   * - the having_nodes apply the predicates in the optional HAVING clause (might be multiple to support AND, OR, ...)
   * - the projection_node establishes the correct column order as requested by the SELECT list (since AggregateNode
   *        outputs all groupby columns first and then all aggregate columns) and assigns ALIASes
   */

  const auto& select_list = *select.selectList;
  const auto* group_by = select.groupBy;
  const auto has_having = (group_by && group_by->having);

  /**
   * Output columns of the aggregate_node actually to be output, excluding those that are just used for HAVING
   * and their optional ALIAS
   */
  std::vector<std::pair<ColumnID, std::optional<std::string>>> output_columns;

  /**
   * Build the groupby_aliasing_node
   */
  std::vector<std::shared_ptr<LQPExpression>> groupby_aliasing_expressions;
  groupby_aliasing_expressions.reserve(input_node->output_column_count());
  for (auto input_column_id = ColumnID{0}; input_column_id < input_node->output_column_count(); ++input_column_id) {
    groupby_aliasing_expressions.emplace_back(
        LQPExpression::create_column(input_node->output_column_references()[input_column_id]));
  }
  // Set aliases for columns that receive one by the select list
  for (const auto* select_column_hsql_expr : select_list) {
    if (!select_column_hsql_expr->isType(hsql::kExprColumnRef)) {
      continue;
    }
    if (!select_column_hsql_expr->alias) {
      continue;
    }

    const auto qualified_column_name = HSQLExprTranslator::to_qualified_column_name(*select_column_hsql_expr);
    const auto column_reference = input_node->get_column(qualified_column_name);
    const auto column_id = input_node->get_output_column_id(column_reference);

    groupby_aliasing_expressions[column_id]->set_alias(select_column_hsql_expr->alias);
  }
  auto groupby_aliasing_node = ProjectionNode::make(groupby_aliasing_expressions);
  groupby_aliasing_node->set_left_input(input_node);

  /**
   * Collect the ColumnReferences of the GroupByColumns
   */
  std::vector<LQPColumnReference> groupby_column_references;
  if (group_by) {
    groupby_column_references.reserve(group_by->columns->size());
    for (const auto* groupby_hsql_expr : *group_by->columns) {
      Assert(groupby_hsql_expr->isType(hsql::kExprColumnRef), "Grouping on complex expressions is not yet supported.");

      const auto qualified_column_name = HSQLExprTranslator::to_qualified_column_name(*groupby_hsql_expr);
      const auto column_reference = groupby_aliasing_node->find_column(qualified_column_name);
      AssertInput(column_reference, "Couldn't resolve groupby column.");

      groupby_column_references.emplace_back(*column_reference);
    }
  }

  /**
   * The Aggregate Operator outputs all groupby columns first, and then all aggregates.
   * Therefore use this offset when setting up the ColumnIDs for the Projection that puts the columns in the right order.
   */
  auto current_aggregate_column_id =
      group_by ? ColumnID{static_cast<uint16_t>(group_by->columns->size())} : ColumnID{0};

  /**
   * Parse the SELECT list for aggregates and remember the order of the output_columns
   */
  std::vector<std::shared_ptr<LQPExpression>> aggregate_expressions;
  aggregate_expressions.reserve(select_list.size());

  for (const auto* select_column_hsql_expr : select_list) {
    std::optional<std::string> alias;
    if (select_column_hsql_expr->alias) {
      alias = std::string(select_column_hsql_expr->alias);
    }

    if (select_column_hsql_expr->isType(hsql::kExprFunctionRef)) {
      const auto aggregate_expression = HSQLExprTranslator::to_lqp_expression(*select_column_hsql_expr, input_node);
      aggregate_expressions.emplace_back(aggregate_expression);

      output_columns.emplace_back(current_aggregate_column_id, alias);
      current_aggregate_column_id++;
    } else if (select_column_hsql_expr->isType(hsql::kExprColumnRef)) {
      /**
       * This if block is mostly used to conduct an SQL conformity check, whether column references in the SELECT list of
       * aggregates appear in the GROUP BY clause.
       */
      AssertInput(group_by != nullptr,
             "SELECT list of aggregate contains a column, but the query does not have a GROUP BY clause.");

      const auto qualified_column_name = HSQLExprTranslator::to_qualified_column_name(*select_column_hsql_expr);
      const auto column_reference = groupby_aliasing_node->find_column(qualified_column_name);
      AssertInput(column_reference, "Couldn't resolve groupby column.");

      const auto iter =
          std::find(groupby_column_references.begin(), groupby_column_references.end(), *column_reference);

      AssertInput(iter != groupby_column_references.end(), "Column '" + select_column_hsql_expr->getName()
                                                        + "' is specified in SELECT list, but not in GROUP BY clause.");

      const auto column_id = static_cast<ColumnID>(std::distance(groupby_column_references.begin(), iter));
      output_columns.emplace_back(column_id, alias);
    } else {
      Fail("Unsupported item in projection list for AggregateOperator.");
    }
  }

  /**
   * The SELECT-list has been resolved, so now we can (and have to!) remove the groupby_aliasing_node from the LQP
   */
  groupby_aliasing_node->remove_from_tree();

  /**
   * Check for HAVING now, because it might contain more aggregations
   */
  if (has_having) {
    // retrieve all aggregates in the having clause
    auto having_expressions = _retrieve_having_aggregates(*group_by->having, input_node);

    for (const auto& having_expr : having_expressions) {
      // see if the having expression is included in the aggregation
      auto result = std::find_if(aggregate_expressions.begin(), aggregate_expressions.end(),
                                 [having_expr](const auto& expr) { return *expr == *having_expr; });

      if (result == aggregate_expressions.end()) {
        // expression not found! add to the other aggregations
        aggregate_expressions.emplace_back(having_expr);
      }
    }
  }

  /**
   * Create the AggregateNode, optionally add the PredicateNodes for the HAVING clause and finally add a ProjectionNode
   */
  auto aggregate_node = AggregateNode::make(aggregate_expressions, groupby_column_references);
  aggregate_node->set_left_input(input_node);

  /**
   * Create the ProjectionNode
   */
  std::vector<std::shared_ptr<LQPExpression>> projection_expressions;
  for (const auto& output_column : output_columns) {
    DebugAssert(output_column.first < aggregate_node->output_column_count(), "ColumnID out of range");
    const auto column_reference = aggregate_node->output_column_references()[output_column.first];
    projection_expressions.emplace_back(LQPExpression::create_column(column_reference, output_column.second));
  }
  auto projection_node = ProjectionNode::make(projection_expressions);

  /**
   * If there is a HAVING, insert it between AggregateNode and ProjectionNode, otherwise just tie the ProjectionNode
   * to the AggregateNode
   */
  if (has_having) {
    auto having_node = _translate_having(*group_by->having, aggregate_node, aggregate_node);
    projection_node->set_left_input(having_node);
  } else {
    projection_node->set_left_input(aggregate_node);
  }

  return projection_node;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_projection(
    const std::vector<hsql::Expr*>& select_list, const std::shared_ptr<AbstractLQPNode>& input_node) {
  std::vector<std::shared_ptr<LQPExpression>> select_column_expressions;

  for (const auto* select_column_hsql_expr : select_list) {
    std::shared_ptr<LQPExpression> expr;

    // For subselects, recursively translate the term to an LQPNode and add its root to an LQPExpression
    if (select_column_hsql_expr->select) {
      auto alias = select_column_hsql_expr->alias != nullptr
                       ? std::optional<std::string>(select_column_hsql_expr->alias)
                       : std::nullopt;
      auto subselect_node = _translate_select(*select_column_hsql_expr->select);
      expr = LQPExpression::create_subselect(subselect_node, alias);
    } else {
      expr = HSQLExprTranslator::to_lqp_expression(*select_column_hsql_expr, input_node);
    }

    DebugAssert(expr->type() == ExpressionType::Star || expr->type() == ExpressionType::Column ||
                    expr->is_arithmetic_operator() || expr->type() == ExpressionType::Literal ||
                    expr->type() == ExpressionType::Subselect || expr->type() == ExpressionType::Placeholder,
                "Only column references, star-selects, subselects and arithmetic expressions supported for now.");

    if (expr->type() == ExpressionType::Star) {
      // Resolve `SELECT *` or `SELECT prefix.*` to columns.
      std::vector<LQPColumnReference> column_references;

      if (!expr->table_name()) {
        // If there is no table qualifier take all columns from the input.
        for (ColumnID column_idx{0}; column_idx < input_node->output_column_count(); ++column_idx) {
          column_references.emplace_back(input_node->output_column_references()[column_idx]);
        }
      } else {
        /**
         * Otherwise only take columns that belong to that qualifier.
         *
         * Consider `SELECT t1.* FROM (SELECT a,b FROM t) AS t1`
         *
         * First, we retrieve the node (`origin_node`) that "creates" "t1". Then, in the for loop, for every Column that
         * `origin_node` outputs, we check whether it "reaches" the input_node
         * (it may get discarded by a Projection/Aggregate along the way). If it is still contained in the input_node
         * it gets added to the list of Columns that the Projection outputs.
         */
        auto origin_node = input_node->find_table_name_origin(*expr->table_name());
        AssertInput(origin_node, "Couldn't resolve '" + *expr->table_name() + "'.*");

        for (auto origin_node_column_id = ColumnID{0}; origin_node_column_id < origin_node->output_column_count();
             ++origin_node_column_id) {
          const auto column_reference = LQPColumnReference{origin_node, origin_node_column_id};
          const auto input_node_column_id = input_node->find_output_column_id({origin_node, origin_node_column_id});
          if (input_node_column_id) {
            column_references.emplace_back(column_reference);
          }
        }
      }

      const auto column_expressions = LQPExpression::create_columns(column_references);
      select_column_expressions.insert(select_column_expressions.end(), column_expressions.cbegin(),
                                       column_expressions.cend());
    } else {
      select_column_expressions.emplace_back(expr);
    }
  }

  auto projection_node = ProjectionNode::make(select_column_expressions);
  projection_node->set_left_input(input_node);

  return projection_node;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_order_by(
    const std::vector<hsql::OrderDescription*>& order_list, const std::shared_ptr<AbstractLQPNode>& input_node) {
  if (order_list.empty()) {
    return input_node;
  }

  std::vector<OrderByDefinition> order_by_definitions;
  order_by_definitions.reserve(order_list.size());

  for (const auto& order_description : order_list) {
    const auto& order_expr = *order_description->expr;

    // TODO(anybody): handle non-column refs
    DebugAssert(order_expr.isType(hsql::kExprColumnRef), "Can only order by columns for now.");

    const auto column_reference = HSQLExprTranslator::to_column_reference(order_expr, input_node);
    const auto order_by_mode = order_type_to_order_by_mode.at(order_description->type);

    order_by_definitions.emplace_back(column_reference, order_by_mode);
  }

  auto sort_node = SortNode::make(order_by_definitions);
  sort_node->set_left_input(input_node);

  return sort_node;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_limit(const hsql::LimitDescription& limit,
                                                                 const std::shared_ptr<AbstractLQPNode>& input_node) {
  auto limit_node = LimitNode::make(limit.limit);
  limit_node->set_left_input(input_node);
  return limit_node;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_predicate(
    const hsql::Expr& hsql_expr, bool allow_function_columns,
    const std::function<LQPColumnReference(const hsql::Expr&)>& resolve_column,
    const std::shared_ptr<AbstractLQPNode>& input_node) {
  DebugAssert(hsql_expr.expr != nullptr, "hsql malformed");

  /**
   * From the hsql-expr describing the scan condition, construct the parameters for a PredicateNode
   * (resulting in e.g. a TableScan). allow_function_columns and resolve_column are helper params making
   * _resolve_predicate_params() usable for both WHERE and HAVING.
   *
   * TODO(anybody): think about how this can be supported as well.
   *
   * * Example:
   *     SELECT * FROM t WHERE 1 BETWEEN a AND 3
   *  -> SELECT * FROM t WHERE a <= 1
   *
   *     SELECT * FROM t WHERE 3 BETWEEN 1 AND a
   *  -> SELECT * FROM t WHERE a >= 3
   *
   *  The biggest question is how to introduce this in the code nicely.
   *
   *
   * # Supported:
   * SELECT a, SUM(B) FROM t GROUP BY a HAVING SUM(B) > 0
   * This query is fine because the expression used in the HAVING clause is part of the SELECT list.
   * We first translate the SELECT list, which will result in an Aggregate operator that creates a column for the sum.
   * We can subsequently access that column when we translate the HAVING expression here.
   *
   * # Unsupported:
   * SELECT a, SUM(B) FROM t GROUP BY a HAVING AVG(B) > 0
   * This query cannot be translated at the moment because the Aggregate does not produce an output column for the AVG.
   * Therefore, the filter expression cannot be translated, because the TableScan operator is not able to compute
   * aggregates on its own.
   *
   * TODO(anybody): extend support for those HAVING clauses.
   * One option is to add them to the Aggregate and then use a Projection to remove them from the result.
   */

  const auto refers_to_column = [allow_function_columns](const hsql::Expr& hsql_expr) {
    return hsql_expr.isType(hsql::kExprColumnRef) ||
           (allow_function_columns && hsql_expr.isType(hsql::kExprFunctionRef));
  };

  auto predicate_negated = (hsql_expr.opType == hsql::kOpNot);

  const auto* column_ref_hsql_expr = hsql_expr.expr;
  PredicateCondition predicate_condition;

  if (predicate_negated) {
    Assert(hsql_expr.expr != nullptr, "NOT operator without further expressions");
    predicate_condition = translate_operator_type_to_predicate_condition(hsql_expr.expr->opType);

    /**
     * It should be possible for any predicate to be negated with "NOT",
     * e.g., WHERE NOT a > 5. However, this is currently not supported.
     * Right now we only use `kOpNot` to detect and set the `OpIsNotNull` predicate condition.
     */
    Assert(predicate_condition == PredicateCondition::IsNull, "Only IS NULL can be negated");

    if (predicate_condition == PredicateCondition::IsNull) {
      predicate_condition = PredicateCondition::IsNotNull;
    }

    // change column reference to the correct expression
    column_ref_hsql_expr = hsql_expr.expr->expr;
  } else {
    predicate_condition = translate_operator_type_to_predicate_condition(hsql_expr.opType);
  }

  // Indicates whether to use expr.expr or expr.expr2 as the main column to reference
  auto operands_switched = false;

  /**
   * value_ref_hsql_expr = the expr referring to the value of the scan, e.g. the 5 in `WHERE 5 > p_income`, but also
   * the secondary column p_b in a scan like `WHERE p_a > p_b`
   */
  const hsql::Expr* value_ref_hsql_expr = nullptr;

  std::optional<AllTypeVariant> value2;  // Left uninitialized for predicates that are not BETWEEN

  if (predicate_condition == PredicateCondition::Between) {
    /**
     * Translate expressions of the form `column_or_aggregate BETWEEN value AND value2`.
     * Both value and value2 can be any kind of literal, while value might also be a column or a placeholder.
     * As per the TODO below, value2 cannot be neither of those, YET
     */

    Assert(hsql_expr.exprList->size() == 2, "Need two arguments for BETWEEEN");

    const auto* expr0 = (*hsql_expr.exprList)[0];
    const auto* expr1 = (*hsql_expr.exprList)[1];
    DebugAssert(expr0 != nullptr && expr1 != nullptr, "hsql malformed");

    value_ref_hsql_expr = expr0;

    // TODO(anybody): TableScan does not support AllParameterVariant as second value.
    // This would be required to use BETWEEN in a prepared statement,
    // or to do a BETWEEN scan for three columns (a BETWEEN b and c).
    const auto value2_all_parameter_variant = HSQLExprTranslator::to_all_parameter_variant(*expr1);
    Assert(is_variant(value2_all_parameter_variant), "Value2 of a Predicate has to be AllTypeVariant");
    value2 = boost::get<AllTypeVariant>(value2_all_parameter_variant);

    Assert(refers_to_column(*column_ref_hsql_expr), "For BETWEENS, hsql_expr.expr has to refer to a column");
  } else if (predicate_condition == PredicateCondition::In) {
    // Handle IN by using a semi join
    Assert(hsql_expr.select, "The IN operand only supports subqueries so far");
    // TODO(anybody): Also support lists of literals
    auto subselect_node = _translate_select(*hsql_expr.select);

    const auto left_column = resolve_column(*column_ref_hsql_expr);
    Assert(subselect_node->output_column_references().size() == 1, "You can only check IN on one column");
    auto right_column = subselect_node->output_column_references()[0];
    const auto column_references = std::make_pair(left_column, right_column);

    auto join_node = std::make_shared<JoinNode>(JoinMode::Semi, column_references, PredicateCondition::Equals);
    join_node->set_left_input(input_node);
    join_node->set_right_input(subselect_node);

    return join_node;
  } else if (predicate_condition != PredicateCondition::IsNull &&
             predicate_condition != PredicateCondition::IsNotNull) {
    /**
     * For logical operators (>, >=, <, ...), thanks to the strict interface of PredicateNode/TableScan, we have to
     * determine whether the left (expr.expr) or the right (expr.expr2) expr refers to the Column/AggregateFunction
     * or the other one.
     */
    DebugAssert(hsql_expr.expr2 != nullptr, "hsql malformed");

    if (!refers_to_column(*hsql_expr.expr)) {
      Assert(refers_to_column(*hsql_expr.expr2), "One side of the expression has to refer to a column.");
      operands_switched = true;
      predicate_condition = get_predicate_condition_for_reverse_order(predicate_condition);
    }

    value_ref_hsql_expr = operands_switched ? hsql_expr.expr : hsql_expr.expr2;
    column_ref_hsql_expr = operands_switched ? hsql_expr.expr2 : hsql_expr.expr;
  }

  /**
   * the argument passed to resolve_column() here:
   * the expr referring to the main column to be scanned, e.g. "p_income" in `WHERE 5 > p_income`
   * or "p_a" in `WHERE p_a > p_b`
   */
  const auto column_id = resolve_column(*column_ref_hsql_expr);

  auto current_node = input_node;
  auto has_nested_expression = false;

  AllParameterVariant value;
  if (predicate_condition == PredicateCondition::IsNull || predicate_condition == PredicateCondition::IsNotNull) {
    value = NULL_VALUE;
  } else if (refers_to_column(*value_ref_hsql_expr)) {
    value = resolve_column(*value_ref_hsql_expr);
  } else if (value_ref_hsql_expr->select) {
    // If a subselect is present, add the result of the subselect to the table using a projection
    const auto column_references = input_node->output_column_references();
    const auto original_column_expressions = LQPExpression::create_columns(column_references);

    auto subselect_node = _translate_select(*value_ref_hsql_expr->select);
    auto subselect_expression = LQPExpression::create_subselect(subselect_node);

    auto column_expressions = original_column_expressions;
    column_expressions.push_back(subselect_expression);

    auto expand_projection_node = std::make_shared<ProjectionNode>(column_expressions);
    expand_projection_node->set_left_input(input_node);

    // Compare against the column containing the subselect result
    auto subselect_column_id = ColumnID(column_expressions.size() - 1);
    auto predicate_node =
        std::make_shared<PredicateNode>(column_id, predicate_condition, subselect_column_id, std::nullopt);
    predicate_node->set_left_input(expand_projection_node);

    // Remove the column containing the subselect result
    auto reduce_projection_node = std::make_shared<ProjectionNode>(original_column_expressions);
    reduce_projection_node->set_left_input(predicate_node);

    return reduce_projection_node;
  } else if (value_ref_hsql_expr->type == hsql::kExprOperator) {
    /**
     * If there is a nested expression (e.g. 1233 + 1) instead of a column reference or literal,
     * we need to add a Projection node that handles this before adding the PredicateNode.
     */
    auto column_expressions = LQPExpression::create_columns(current_node->output_column_references());
    column_expressions.push_back(HSQLExprTranslator::to_lqp_expression(*value_ref_hsql_expr, current_node));

    auto projection_node = std::make_shared<ProjectionNode>(column_expressions);
    projection_node->set_left_input(current_node);
    current_node = projection_node;
    has_nested_expression = true;

    DebugAssert(column_expressions.size() <= std::numeric_limits<uint16_t>::max(),
                "Number of column expressions cannot exceed maximum value of ColumnID.");
    value = LQPColumnReference(current_node, ColumnID{static_cast<uint16_t>(column_expressions.size() - 1)});
  } else {
    value = HSQLExprTranslator::to_all_parameter_variant(*value_ref_hsql_expr);
  }

  auto predicate_node = PredicateNode::make(column_id, predicate_condition, value, value2);
  predicate_node->set_left_input(current_node);

  current_node = predicate_node;

  /**
   * The ProjectionNode we added previously (if we have a nested expression)
   * added a column expression for that expression, which we need to remove here.
   */
  if (has_nested_expression) {
    auto column_expressions = LQPExpression::create_columns(current_node->output_column_references());
    column_expressions.pop_back();

    auto projection_node = std::make_shared<ProjectionNode>(column_expressions);
    projection_node->set_left_input(current_node);
    current_node = projection_node;
  }

  return current_node;
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_show(const hsql::ShowStatement& show_statement) {
  switch (show_statement.type) {
    case hsql::ShowType::kShowTables:
      return ShowTablesNode::make();
    case hsql::ShowType::kShowColumns:
      return ShowColumnsNode::make(std::string(show_statement.name));
    default:
      Fail("hsql::ShowType is not supported.");
  }
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_create(const hsql::CreateStatement& create_statement) {
  switch (create_statement.type) {
    case hsql::CreateType::kCreateView: {
      auto view = _translate_select((const hsql::SelectStatement&)*create_statement.select);

      if (create_statement.viewColumns) {
        // The CREATE VIEW statement has renamed the columns: CREATE VIEW myview (foo, bar) AS SELECT ...
        AssertInput(create_statement.viewColumns->size() == view->output_column_count(),
               "Number of Columns in CREATE VIEW does not match SELECT statement");

        // Create a list of renamed column expressions
        std::vector<std::shared_ptr<LQPExpression>> projections;
        ColumnID column_id{0};
        for (const auto& alias : *create_statement.viewColumns) {
          const auto column_reference = view->output_column_references()[column_id];
          // rename columns so they match the view definition
          projections.push_back(LQPExpression::create_column(column_reference, alias));
          ++column_id;
        }

        // Create a projection node for this renaming
        auto projection_node = ProjectionNode::make(projections);
        projection_node->set_left_input(view);
        view = projection_node;
      }

      return std::make_shared<CreateViewNode>(create_statement.tableName, view);
    }
    default:
      Fail("hsql::CreateType is not supported.");
  }
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_translate_drop(const hsql::DropStatement& drop_statement) {
  switch (drop_statement.type) {
    case hsql::DropType::kDropView: {
      return DropViewNode::make(drop_statement.name);
    }
    default:
      Fail("hsql::DropType is not supported.");
  }
}

std::shared_ptr<AbstractLQPNode> SQLTranslator::_validate_if_active(
    const std::shared_ptr<AbstractLQPNode>& input_node) {
  if (!_validate) return input_node;

  auto validate_node = ValidateNode::make();
  validate_node->set_left_input(input_node);
  return validate_node;
}

}  // namespace opossum
