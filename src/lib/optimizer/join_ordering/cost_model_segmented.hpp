#pragma once

#include "boost/numeric/ublas/matrix.hpp"

#include "abstract_cost_model.hpp"

namespace opossum {

class Product;
class TableScan;
class JoinHash;

class CostModelSegmented : public AbstractCostModel {
 public:
  /**
   * JoinHashCoefficientMatrix
   * x Axis: LeftInputRowCount, RightInputRowCount, OutputRowCount, Intercept
   * y Axis: materialization, partitioning, build, probe, output
   */
  using JoinHashCoefficientMatrix = std::array<std::array<float, 5>, 6>;

  /**
   * ProductCoefficientMatrix
   * x Axis: RowCountProduct, ColumnCountProduct
   * y Axis: Total
   */
  using ProductCoefficientMatrix = std::array<std::array<float, 1>, 1>;

  /**
   * UnionPositionsCoefficientMatrix
   * x Axis: RowCountLeft, RowCountRight, OutputValueCount
   * y Axis: Total
   */
  using UnionPositionsCoefficientMatrix = std::array<std::array<float, 3>, 3>;

  /**
   * TableScanCoefficientMatrix
   * Features: InputRowCount, InputReferenceCount, OutputRowCount
   * Targets: Total
   */
  using TableScanCoefficientMatrix = std::array<std::array<float, 4>, 1>;

  /**
   * JoinSortMergeCoefficientMatrix
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

  Cost cost_table_scan(const std::shared_ptr<TableStatistics>& table_statistics, const ColumnID column,
                       const PredicateCondition predicate_condition, const AllParameterVariant& value) const override;
  Cost cost_table_scan_impl(const TableScanFeatures& features) const;

  std::optional<Cost> cost_table_scan_op(const TableScan& table_scan, const OperatorCostMode operator_cost_mode) const override;
  /**@}*/

  /**
   * @defgroup JoinHash
   * @{
   */
  struct JoinHashFeatures final {
    // Scalar features
    size_t major_input_row_count{0};
    size_t major_input_reference_count{0};
    size_t minor_input_row_count{0};
    size_t minor_input_reference_count{0};
    size_t output_row_count{0};
  };

  struct JoinHashTargets final {
    Cost materialization{0};
    Cost partitioning{0};
    Cost build{0};
    Cost probe{0};
    Cost output{0};
    Cost total{0};
  };

  enum class JoinHashSubModel {
    Standard
  };

  static JoinHashSubModel join_hash_sub_model(const JoinHashFeatures& features);
  static JoinHashFeatures join_hash_features(const JoinHash& join_hash);
  static JoinHashTargets join_hash_targets(const JoinHash& join_hash);

  Cost cost_join_hash(const std::shared_ptr<TableStatistics>& table_statistics_left,
                      const std::shared_ptr<TableStatistics>& table_statistics_right, const JoinMode join_mode,
                      const ColumnIDPair& join_column_ids, const PredicateCondition predicate_condition) const override;
  Cost cost_join_hash_impl(const JoinHashFeatures& features) const;

  std::optional<Cost> cost_join_hash_op(const JoinHash& join_hash, const OperatorCostMode operator_cost_mode) const override;
  /**@}*/

  /**
   * @defgroup Product
   * @{
   */
  struct ProductFeatures final {
    size_t output_value_count{0};
  };

  struct ProductTargets final {
    Cost total{0};
  };

  enum class ProductSubModel {
    Standard
  };

  static ProductSubModel product_sub_model(const ProductFeatures& features);
  static ProductFeatures product_features(const Product& product);
  static ProductTargets product_targets(const Product& product);

  Cost cost_product(const std::shared_ptr<TableStatistics>& table_statistics_left,
                      const std::shared_ptr<TableStatistics>& table_statistics_right) const override;
  Cost cost_product_impl(const ProductFeatures& features) const;

  std::optional<Cost> cost_product_op(const Product& product, const OperatorCostMode operator_cost_mode) const override;
  /**@}*/


  /**
   * @defgroup UnionPositions
   * @{
   */
  struct UnionPositionsFeatures final {
    size_t n_log_n_row_count_left{0};
    size_t n_log_n_row_count_right{0};
    size_t output_value_count{0};
  };

  struct UnionPositionsTargets final {
    Cost init{0};
    Cost sort{0};
    Cost output{0};
    Cost total{0};
  };

  enum class UnionPositionsSubModel {
    Standard
  };

  static UnionPositionsSubModel union_positions_sub_model(const UnionPositionsFeatures& features);
  static UnionPositionsFeatures union_positions_features(const UnionPositions& union_positions);
  static UnionPositionsTargets union_positions_targets(const UnionPositions& union_positions);

  Cost cost_union_positions(const std::shared_ptr<TableStatistics>& table_statistics_left,
                      const std::shared_ptr<TableStatistics>& table_statistics_right) const override;
  Cost cost_union_positions_impl(const UnionPositionsFeatures& features) const;

  std::optional<Cost> cost_union_positions_op(const UnionPositions& union_positions, const OperatorCostMode operator_cost_mode) const override;
  /**@}*/


  CostModelSegmented();

  Cost cost_join_sort_merge(const std::shared_ptr<TableStatistics>& table_statistics_left,
                            const std::shared_ptr<TableStatistics>& table_statistics_right, const JoinMode join_mode,
                            const ColumnIDPair& join_column_ids,
                            const PredicateCondition predicate_condition) const override;


 private:
  JoinSortMergeCoefficientMatrix _join_sort_merge_coefficients;

  std::unordered_map<JoinHashSubModel, JoinHashCoefficientMatrix> _join_hash_sub_model_coefficients;
  std::unordered_map<TableScanSubModel, TableScanCoefficientMatrix> _table_scan_sub_model_coefficients;
  std::unordered_map<ProductSubModel, ProductCoefficientMatrix> _product_sub_model_coefficients;
  std::unordered_map<UnionPositionsSubModel, UnionPositionsCoefficientMatrix> _union_positions_sub_model_coefficients;
};

}  // namespace opossum
