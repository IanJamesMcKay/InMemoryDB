#include "cardinality_estimator_statistics.hpp"

#include <algorithm>
#include <unordered_set>
#include <optimizer/join_ordering/join_plan_predicate.hpp>

#include "optimizer/join_ordering/join_plan_predicate.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/logical_expression.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "utils/assert.hpp"
#include "storage/storage_manager.hpp"
#include "statistics/generate_table_statistics.hpp"
#include "statistics/column_statistics.hpp"
#include "statistics/table_statistics.hpp"

namespace opossum {

Cardinality CardinalityEstimatorStatistics::estimate(const std::shared_ptr<AbstractLQPNode>& lqp,
                                                     const std::shared_ptr<AbstractQueryBlock>& query_blocks_root) const {
  return _compute_node_statistics(lqp)->row_count();
}

std::shared_ptr<TableStatistics> CardinalityEstimatorStatistics::_compute_node_statistics(
const std::shared_ptr<AbstractLQPNode> &node) {
  Assert(node, "Unexpected LQP leaf");

  switch (node->type()) {
    case LQPNodeType::Join:
      return _compute_join_node_statistics(std::static_pointer_cast<JoinNode>(node));

    case LQPNodeType::Predicate:
      return _compute_predicate_node_statistics(std::static_pointer_cast<PredicateNode>(node));

    case LQPNodeType::Sort:
      return _compute_node_statistics(node->left_input());

    case LQPNodeType::StoredTable:
      return _compute_stored_table_node_statistics(std::static_pointer_cast<StoredTableNode>(node));

    case LQPNodeType::Union:
      return _compute_union_node_statistics(std::static_pointer_cast<UnionNode>(node));

    case LQPNodeType::Mock:
      return _compute_mock_node_statistics(std::static_pointer_cast<MockNode>(node));

    // TODO(anybody) actually implement these
    case LQPNodeType::Validate:
    case LQPNodeType::Projection:
    case LQPNodeType::Aggregate:
    case LQPNodeType::Limit:
      return _compute_node_statistics(node->left_input());

    case LQPNodeType::Update:
    case LQPNodeType::Root:
    case LQPNodeType::Delete:
    case LQPNodeType::DropView:
    case LQPNodeType::DummyTable:
    case LQPNodeType::Insert:
    case LQPNodeType::CreateView:
    case LQPNodeType::ShowColumns:
    case LQPNodeType::ShowTables:
      Fail("Can't estimate the selectivity of non-DQL nodes");
  }
}

std::shared_ptr<TableStatistics> CardinalityEstimatorStatistics::_compute_join_node_statistics(const std::shared_ptr<JoinNode> &node) {

  if (node->join_mode() == JoinMode::Cross) {
    return std::make_shared<TableStatistics>(node->left_input()->get_statistics()->estimate_cross_join(*node->right_input()->get_statistics()));
  } else {
    Assert(node->join_column_references(),
           "Only cross joins and joins with join column ids supported for generating join statistics");
    Assert(node->predicate_condition(),
           "Only cross joins and joins with predicate condition supported for generating join statistics");

    ColumnIDPair join_colum_ids{node->left_input()->get_output_column_id(node->join_column_references()->first),
                                node->right_input()->get_output_column_id(node->join_column_references()->second)};

    const auto left_input_statistics = _compute_node_statistics(node->left_input());
    const auto right_input_statistics = _compute_node_statistics(node->right_input());

    return std::make_shared<TableStatistics>(left_input_statistics->estimate_predicated_join(*right_input_statistics, node->join_mode(),
                                                                                                    join_colum_ids, *node->predicate_condition()));
  }
}

std::shared_ptr<TableStatistics> CardinalityEstimatorStatistics::_compute_predicate_node_statistics(const std::shared_ptr<PredicateNode> &node) {
  // If value references a Column, we have to resolve its ColumnID (same as for _column_reference below)
  auto value = node->value();
  if (is_lqp_column_reference(value)) {
    value = node->get_output_column_id(boost::get<LQPColumnReference>(value));
  }

  const auto left_input_statistics = _compute_node_statistics(node->left_input());

  return std::make_shared<TableStatistics>(left_input_statistics->estimate_predicate(node->left_input()->get_output_column_id(node->column_reference()),
                                                                                            node->predicate_condition(), value, node->value2()));
}

std::shared_ptr<TableStatistics> CardinalityEstimatorStatistics::_compute_union_node_statistics(const std::shared_ptr<UnionNode> &node) {
  const auto left_input_statistics = _compute_node_statistics(node->left_input());
  const auto right_input_statistics = _compute_node_statistics(node->right_input());

  return std::make_shared<TableStatistics>(left_input_statistics->estimate_disjunction(*right_input_statistics));
}

std::shared_ptr<TableStatistics> CardinalityEstimatorStatistics::_compute_mock_node_statistics(const std::shared_ptr<MockNode> &node) {
  if (node->constructor_arguments().type() != typeid(std::shared_ptr<TableStatistics>)) {
    return boost::get<std::shared_ptr<TableStatistics>>(node->constructor_arguments());
  }
  Fail("Mock nodes need statistics in order to be used in optimization");
}

std::shared_ptr<TableStatistics> CardinalityEstimatorStatistics::_compute_stored_table_node_statistics(const std::shared_ptr<StoredTableNode> &node) {
  if (node->get_statistics()) {
    return node->get_statistics();
  } else {
    return std::make_shared<TableStatistics>(generate_table_statistics(*StorageManager::get().get_table(node->table_name())));
  }
}

}  // namespace opossum
