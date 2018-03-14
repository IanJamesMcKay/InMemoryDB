#include "abstract_cost_model.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "operators/table_scan.hpp"

namespace opossum {

std::optional<Cost> AbstractCostModel::cost_table_scan_op(const TableScan& table_scan) const {
  return std::nullopt;
}

std::optional<Cost> AbstractCostModel::get_node_cost(const AbstractLQPNode& node) const {
  switch (node.type()) {
    case LQPNodeType::Predicate: {
      const auto& predicate_node = static_cast<const PredicateNode&>(node);
      const auto left_column_id = predicate_node.get_output_column_id(predicate_node.column_reference());

      auto value = predicate_node.value();
      if (value.type() == typeid(LQPColumnReference)) {
        value = predicate_node.get_output_column_id(boost::get<LQPColumnReference>(value));
      }

      return cost_table_scan(node.left_input()->get_statistics(), left_column_id, predicate_node.predicate_condition(), value);
    }

    case LQPNodeType::Join: {
      const auto& join_node = static_cast<const JoinNode&>(node);

      const auto left_statistics = node.left_input()->get_statistics();
      const auto right_statistics = node.right_input()->get_statistics();

      if (join_node.join_mode() == JoinMode::Cross) {
        return cost_product(left_statistics, right_statistics);
      }

      const auto left_column_id = join_node.left_input()->get_output_column_id(join_node.join_column_references()->first);
      const auto right_column_id = join_node.right_input()->get_output_column_id(join_node.join_column_references()->second);

      if (join_node.join_mode() == JoinMode::Inner && join_node.predicate_condition() == PredicateCondition::Equals) {
        return cost_join_hash(left_statistics, right_statistics, join_node.join_mode(), {left_column_id, right_column_id}, *join_node.predicate_condition());
      }

      return cost_join_sort_merge(left_statistics, right_statistics, join_node.join_mode(), {left_column_id, right_column_id}, *join_node.predicate_condition());
    }

    default:
      return std::nullopt;
  }
}

std::optional<Cost> AbstractCostModel::get_operator_cost(const AbstractOperator& op) const {
  switch (op.type()) {
    case OperatorType::TableScan:
      return cost_table_scan_op(static_cast<const TableScan&>(op));

    default:
      return std::nullopt;
  }
}

}