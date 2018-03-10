#include "cost_model_segmented.hpp"

#include "optimizer/table_statistics.hpp"

namespace opossum {

CostModelSegmented::CostModelSegmented() {
  _join_hash_coefficients =
      JoinHashCoefficientMatrix{{{{0.5134497330789046f, 0.2855347312271709f, 0.03562409664220728f, 0.0f}},
                                 {{-3.568099221199f, 2.163215873463416f, 3.5492830521878886f, 0.0f}},
                                 {{0.16607179650238305f, 0.26172171946741085f, 0.08765293032708743f, 0.0f}},
                                 {{0.3460139111375513f, 0.030189513343270802f, 0.07910391143651066f, 0.0f}},
                                 {{0.5765816664702633f, 0.08757680844338467f, 1.4843440266345889f, 0.0f}}}};

  _table_scan_coefficients = TableScanCoefficientMatrix{{{{0.4391338428178249f, 0.09476596343484817f, 0.0f}}}};

  _join_sort_merge_coefficients = JoinSortMergeCoefficientMatrix{{{{0.4391338428178249f, 0.09476596343484817f, 0.0f}}}};

  _product_coefficients = ProductCoefficientMatrix{{{{1.0960099722478653f}}}};
}

Cost CostModelSegmented::cost_join_hash(const std::shared_ptr<TableStatistics>& table_statistics_left,
                                        const std::shared_ptr<TableStatistics>& table_statistics_right,
                                        const JoinMode join_mode, const ColumnIDPair& join_column_ids,
                                        const PredicateCondition predicate_condition) const {
  const auto left_input_row_count = table_statistics_left->row_count();
  const auto right_input_row_count = table_statistics_right->row_count();
  const auto output_table_statistics = table_statistics_left->generate_predicated_join_statistics(
      table_statistics_right, join_mode, join_column_ids, predicate_condition);
  const auto output_row_count = output_table_statistics->row_count();

  const auto& m = _join_hash_coefficients;

  // clang-format off
  const auto materialization = left_input_row_count * m[0][0] + right_input_row_count * m[0][1] + output_row_count * m[0][2] + m[0][3];  // NOLINT
  //const auto partitioning =    left_input_row_count * m[1][0] + right_input_row_count * m[1][1] + output_row_count * m[1][2] + m[1][3];  // NOLINT
  const auto build =           left_input_row_count * m[2][0] + right_input_row_count * m[2][1] + output_row_count * m[2][2] + m[2][3];  // NOLINT
  const auto probe =           left_input_row_count * m[3][0] + right_input_row_count * m[3][1] + output_row_count * m[3][2] + m[3][3];  // NOLINT
  //const auto output =          left_input_row_count * m[4][0] + right_input_row_count * m[4][1] + output_row_count * m[4][2] + m[4][3];  // NOLINT

  // clang-format on

  return materialization + build + probe;
}

Cost CostModelSegmented::cost_table_scan(const std::shared_ptr<TableStatistics>& table_statistics,
                                         const ColumnID column, const PredicateCondition predicate_condition,
                                         const AllTypeVariant value) const {
  const auto left_input_row_count = table_statistics->row_count();
  const auto output_table_statistics = table_statistics->predicate_statistics(column, predicate_condition, value);
  const auto output_row_count = output_table_statistics->row_count();

  const auto& m = _table_scan_coefficients;

  // clang-format off
  const auto total = left_input_row_count * m[0][0] + output_row_count * m[0][1] + m[0][2];
  // clang-format on

  return total;
}

Cost CostModelSegmented::cost_join_sort_merge(const std::shared_ptr<TableStatistics>& table_statistics_left,
                                              const std::shared_ptr<TableStatistics>& table_statistics_right,
                                              const JoinMode join_mode, const ColumnIDPair& join_column_ids,
                                              const PredicateCondition predicate_condition) const {
  return 0.0f;
}

Cost CostModelSegmented::cost_product(const std::shared_ptr<TableStatistics>& table_statistics_left,
                                      const std::shared_ptr<TableStatistics>& table_statistics_right) const {
  const auto product_row_count = table_statistics_left->row_count() * table_statistics_right->row_count();

  const auto& m = _product_coefficients;

  // clang-format off
  const auto total = product_row_count * m[0][0];
  // clang-format on

  return total;
}

}  // namespace opossum
