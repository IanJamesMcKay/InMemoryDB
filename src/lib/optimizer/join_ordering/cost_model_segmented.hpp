#pragma once

#include "boost/numeric/ublas/matrix.hpp"

#include "abstract_cost_model.hpp"

namespace opossum {

class TableScan;

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

  /**
   * @defgroup TableScanModel
   * @{
   */
  struct TableScanFeatures final {
    // Determine submodel
    DataType left_data_type{DataType::Int};
    DataType right_data_type{DataType::Int};
    PredicateCondition predicate_condition{PredicateCondition::Equals};
    bool right_operand_is_column{false};

    // Scalar features
    size_t input_reference_count{0};
    size_t input_row_count{0};
    size_t output_row_count{0};
    size_t output_reference_count{0};
  };

  struct TableScanTargets final {
    Cost scan_cost{0};
    Cost output_cost{0};
    Cost total{0};
  };

  enum class TableScanSubModel {
    ColumnValueNumeric, ColumnColumnNumeric, ColumnValueString, ColumnColumnString, Like, Uncategorized
  };

  static TableScanSubModel table_scan_sub_model(const TableScanFeatures& features);
  static TableScanFeatures table_scan_features(const TableScan& table_scan);
  static TableScanTargets table_scan_targets(const TableScan& table_scan);
  /**@}*/

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

  /**
   * @defgroup Actual costing function backends
   * @{
   */
  Cost cost_table_scan_impl(const TableScanFeatures& features) const;
  /**@}*/

 private:
  JoinHashCoefficientMatrix _join_hash_coefficients;
  JoinSortMergeCoefficientMatrix _join_sort_merge_coefficients;
  std::unordered_map<TableScanSubModel, TableScanCoefficientMatrix> _table_scan_sub_model_coefficients;
  ProductCoefficientMatrix _product_coefficients;
};

}  // namespace opossum
