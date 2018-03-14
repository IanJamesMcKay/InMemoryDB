#pragma once

#include "boost/numeric/ublas/matrix.hpp"

#include "abstract_cost_model.hpp"

namespace opossum {

class CostModelSegmented : public AbstractCostModel {
 public:
  /**
   * JoinHashCoefficientMatrix 5x4
   * x Axis: LeftInputRowCount, RightInputRowCount, OutputRowCount, Intercept
   * y Axis: materialization, partitioning, build, probe, output
   */
  using JoinHashCoefficientMatrix = std::array<std::array<float, 4>, 5>;

  /**
   * ProductCoefficientMatrix 1x1
   * x Axis: RowCountProduct
   * y Axis: Total
   */
  using ProductCoefficientMatrix = std::array<std::array<float, 1>, 1>;

  /**
   * TableScanCoefficientMatrix 1x3
   * Features: InputRowCount, InputReferenceCount, OutputRowCount
   * Targets: Total
   */
  using TableScanCoefficientMatrix = std::array<std::array<float, 3>, 1>;

  /**
   * JoinSortMergeCoefficientMatrix 1x3
   * x Axis: LeftInputRowCount, OutputRowCount, Intercept
   * y Axis: Total
   */
  using JoinSortMergeCoefficientMatrix = std::array<std::array<float, 3>, 1>;

  CostModelSegmented();

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

  std::optional<Cost> cost_table_scan_op(const TableScan& table_scan) const override;

 private:
  JoinHashCoefficientMatrix _join_hash_coefficients;
  TableScanCoefficientMatrix _table_scan_column_value_numeric;
  TableScanCoefficientMatrix _table_scan_column_column_numeric;
  TableScanCoefficientMatrix _table_scan_column_value_string;
  TableScanCoefficientMatrix _table_scan_uncategorized;
  JoinSortMergeCoefficientMatrix _join_sort_merge_coefficients;
  ProductCoefficientMatrix _product_coefficients;
};

}  // namespace opossum
