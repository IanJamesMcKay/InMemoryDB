#pragma once

#include <memory>
#include <unordered_map>

#include "abstract_lqp_node.hpp"
#include "all_type_variant.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/post_operator_callback.hpp"
#include "predicate_node.hpp"

namespace opossum {

class AbstractOperator;
class TransactionContext;
class LQPExpression;
class PQPExpression;
enum class OperatorType;

/**
 * Translates an LQP (Logical Query Plan), represented by its root node, into an Operator tree for the execution
 * engine, which in return is represented by its root Operator.
 */
class LQPTranslator final : private Noncopyable {
 public:
  using SubPQPCache = std::unordered_map<std::shared_ptr<const AbstractLQPNode>, std::shared_ptr<AbstractOperator>>;

  std::shared_ptr<AbstractOperator> translate_node(const std::shared_ptr<AbstractLQPNode>& node) const;

  const SubPQPCache& sub_pqp_cache() const;

  void add_post_operator_callback(const PostOperatorCallback& callback);

//  static OperatorType operator_type(const AbstractLQPNode& lqp_node);

 private:
  std::shared_ptr<AbstractOperator> _translate_by_node_type(LQPNodeType type,
                                                            const std::shared_ptr<AbstractLQPNode>& node) const;

  // SQL operators
  std::shared_ptr<AbstractOperator> _translate_stored_table_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_predicate_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_predicate_node_to_index_scan(
      const std::shared_ptr<PredicateNode>& node, const AllParameterVariant& value, const ColumnID column_id,
      const std::shared_ptr<AbstractOperator> input_operator) const;
  std::shared_ptr<AbstractOperator> _translate_projection_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_sort_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_join_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_aggregate_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_limit_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_insert_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_delete_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_dummy_table_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_update_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_union_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_validate_node(const std::shared_ptr<AbstractLQPNode>& node) const;

  // Maintenance operators
  std::shared_ptr<AbstractOperator> _translate_show_tables_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_show_columns_node(const std::shared_ptr<AbstractLQPNode>& node) const;

  // Translate LQP- to PQPExpressions
  std::vector<std::shared_ptr<PQPExpression>> _translate_expressions(
      const std::vector<std::shared_ptr<LQPExpression>>& lqp_expressions,
      const std::shared_ptr<AbstractLQPNode>& node) const;

  std::shared_ptr<AbstractOperator> _translate_create_view_node(const std::shared_ptr<AbstractLQPNode>& node) const;
  std::shared_ptr<AbstractOperator> _translate_drop_view_node(const std::shared_ptr<AbstractLQPNode>& node) const;

  // Cache operator subtrees by LQP node to avoid executing operators below a diamond shape multiple times
  mutable SubPQPCache _sub_pqp_cache;

  std::vector<PostOperatorCallback> _post_operator_callbacks;
};

}  // namespace opossum
