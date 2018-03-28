#pragma once

#include <memory>

#include "all_parameter_variant.hpp"
#include "cost.hpp"
#include "types.hpp"

namespace opossum {

class AbstractLQPNode;
class AbstractOperator;
class JoinHash;
class UnionPositions;
class Product;
class TableStatistics;
class TableScan;

enum class OperatorCostMode {
  TargetCost, PredictedCost
};

class AbstractCostModel {
 public:
  virtual ~AbstractCostModel() = default;

  virtual Cost cost_join_hash(const std::shared_ptr<TableStatistics>& table_statistics_left,
                              const std::shared_ptr<TableStatistics>& table_statistics_right, const JoinMode join_mode,
                              const ColumnIDPair& join_column_ids,
                              const PredicateCondition predicate_condition) const = 0;

  virtual Cost cost_table_scan(const std::shared_ptr<TableStatistics>& table_statistics, const ColumnID column,
                               const PredicateCondition predicate_condition, const AllParameterVariant& value) const = 0;

  virtual Cost cost_join_sort_merge(const std::shared_ptr<TableStatistics>& table_statistics_left,
                                    const std::shared_ptr<TableStatistics>& table_statistics_right,
                                    const JoinMode join_mode, const ColumnIDPair& join_column_ids,
                                    const PredicateCondition predicate_condition) const = 0;

  virtual Cost cost_product(const std::shared_ptr<TableStatistics>& table_statistics_left,
                            const std::shared_ptr<TableStatistics>& table_statistics_right) const = 0;

  virtual Cost cost_union_positions(const std::shared_ptr<TableStatistics>& table_statistics_left,
                                    const std::shared_ptr<TableStatistics>& table_statistics_right) const = 0;

  /**
   * @defgroup Operator Costing
   * Deriving Cost Models don't necessarily need to implement these, they are used for debugging/visualiuation
   *
   * @{
   */

  virtual std::optional<Cost> cost_table_scan_op(const TableScan& table_scan, const OperatorCostMode operator_cost_mode) const;
  virtual std::optional<Cost> cost_join_hash_op(const JoinHash& join_hash, const OperatorCostMode operator_cost_mode) const;
  virtual std::optional<Cost> cost_product_op(const Product& product, const OperatorCostMode operator_cost_mode) const;
  virtual std::optional<Cost> cost_union_positions_op(const UnionPositions& union_positions, const OperatorCostMode operator_cost_mode) const;

  /**@}*/

  std::optional<Cost> get_node_cost(const AbstractLQPNode& node) const;
  std::optional<Cost> get_operator_cost(const AbstractOperator& op, const OperatorCostMode operator_cost_mode) const;
};

}  // namespace opossum
