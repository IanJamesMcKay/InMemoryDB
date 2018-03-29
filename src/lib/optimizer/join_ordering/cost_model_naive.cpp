#include "cost_model_naive.hpp"

#include <cmath>

#include "operators/table_scan.hpp"
#include "operators/product.hpp"
#include "operators/join_hash.hpp"
#include "operators/union_positions.hpp"
#include "optimizer/table_statistics.hpp"

namespace opossum {

Cost CostModelNaive::cost_join_hash(const std::shared_ptr<TableStatistics>& table_statistics_left,
                                    const std::shared_ptr<TableStatistics>& table_statistics_right,
                                    const JoinMode join_mode, const ColumnIDPair& join_column_ids,
                                    const PredicateCondition predicate_condition) const {
  return 1.2f * std::min(table_statistics_left->row_count(), table_statistics_right->row_count());
}

Cost CostModelNaive::cost_table_scan(const std::shared_ptr<TableStatistics>& table_statistics, const ColumnID column,
                                     const PredicateCondition predicate_condition, const AllParameterVariant& value) const {
  return table_statistics->row_count();
}

Cost CostModelNaive::cost_join_sort_merge(const std::shared_ptr<TableStatistics>& table_statistics_left,
                                          const std::shared_ptr<TableStatistics>& table_statistics_right,
                                          const JoinMode join_mode, const ColumnIDPair& join_column_ids,
                                          const PredicateCondition predicate_condition) const {
  const auto row_count_left = table_statistics_left->row_count();
  const auto row_count_right = table_statistics_right->row_count();

  return row_count_left * std::log(row_count_left) + row_count_right * std::log(row_count_right);
}

Cost CostModelNaive::cost_product(const std::shared_ptr<TableStatistics>& table_statistics_left,
                                  const std::shared_ptr<TableStatistics>& table_statistics_right) const {
  return table_statistics_left->row_count() * table_statistics_right->row_count();
}

Cost CostModelNaive::cost_union_positions(const std::shared_ptr<TableStatistics>& table_statistics_left,
                          const std::shared_ptr<TableStatistics>& table_statistics_right) const {
  return 0.0f; // TODO(moritz)
}

std::optional<Cost> CostModelNaive::cost_table_scan_op(const TableScan& table_scan, const OperatorCostMode operator_cost_mode) const {
  switch (operator_cost_mode) {
    case OperatorCostMode::PredictedCost: return table_scan.input_table_left()->row_count();
    case OperatorCostMode::TargetCost: return std::nullopt;
  }
}

std::optional<Cost> CostModelNaive::cost_join_hash_op(const JoinHash& join_hash, const OperatorCostMode operator_cost_mode) const {
  switch (operator_cost_mode) {
    case OperatorCostMode::PredictedCost:
      return 1.2f * std::min(join_hash.input_table_left()->row_count(), join_hash.input_table_right()->row_count());
    case OperatorCostMode::TargetCost:
      return std::nullopt;
  }
}

std::optional<Cost> CostModelNaive::cost_product_op(const Product& product, const OperatorCostMode operator_cost_mode) const {
  switch (operator_cost_mode) {
    case OperatorCostMode::PredictedCost: return product.input_table_left()->row_count() * product.input_table_right()->row_count();
    case OperatorCostMode::TargetCost: return std::nullopt;
  }
}

std::optional<Cost> CostModelNaive::cost_union_positions_op(const UnionPositions& union_positions, const OperatorCostMode operator_cost_mode) const  {
  switch (operator_cost_mode) {
    case OperatorCostMode::PredictedCost: return 0.0f;
    case OperatorCostMode::TargetCost:  return std::nullopt;
  }

}

}  // namespace opossum
