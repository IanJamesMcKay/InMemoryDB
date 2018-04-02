#pragma once

#include "abstract_cost_model.hpp"

namespace opossum {

class CostModelNaive : public AbstractCostModel {
 public:
  std::string name() const override;

  Cost cost_join_hash(const std::shared_ptr<TableStatistics>& table_statistics_left,
                      const std::shared_ptr<TableStatistics>& table_statistics_right, const JoinMode join_mode,
                      const ColumnIDPair& join_column_ids, const PredicateCondition predicate_condition) const override;

  Cost cost_table_scan(const std::shared_ptr<TableStatistics>& table_statistics, const ColumnID column,
                       const PredicateCondition predicate_condition, const AllParameterVariant& value) const override;

  Cost cost_join_sort_merge(const std::shared_ptr<TableStatistics>& table_statistics_left,
                            const std::shared_ptr<TableStatistics>& table_statistics_right, const JoinMode join_mode,
                            const ColumnIDPair& join_column_ids,
                            const PredicateCondition predicate_condition) const override;

  Cost cost_product(const std::shared_ptr<TableStatistics>& table_statistics_left,
                    const std::shared_ptr<TableStatistics>& table_statistics_right) const override;

  Cost cost_union_positions(const std::shared_ptr<TableStatistics>& table_statistics_left,
                                    const std::shared_ptr<TableStatistics>& table_statistics_right) const override;


  std::optional<Cost> cost_table_scan_op(const TableScan& table_scan, const OperatorCostMode operator_cost_mode) const override;
  std::optional<Cost> cost_join_hash_op(const JoinHash& join_hash, const OperatorCostMode operator_cost_mode) const override;
  std::optional<Cost> cost_product_op(const Product& product, const OperatorCostMode operator_cost_mode) const override;
  std::optional<Cost> cost_union_positions_op(const UnionPositions& union_positions, const OperatorCostMode operator_cost_mode) const override;
};

}  // namespace opossum
