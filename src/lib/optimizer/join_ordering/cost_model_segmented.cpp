#include "cost_model_segmented.hpp"

#include "operators/table_scan.hpp"
#include "optimizer/table_statistics.hpp"

namespace opossum {

CostModelSegmented::CostModelSegmented() {
  _join_hash_coefficients =
      JoinHashCoefficientMatrix{{{{0.5134497330789046f, 0.2855347312271709f, 0.03562409664220728f, 0.0f}},
                                 {{-3.568099221199f, 2.163215873463416f, 3.5492830521878886f, 0.0f}},
                                 {{0.16607179650238305f, 0.26172171946741085f, 0.08765293032708743f, 0.0f}},
                                 {{0.3460139111375513f, 0.030189513343270802f, 0.07910391143651066f, 0.0f}},
                                 {{0.5765816664702633f, 0.08757680844338467f, 1.4843440266345889f, 0.0f}}}};

  _table_scan_column_value_numeric = TableScanCoefficientMatrix{{{{0.143470961207f,0.212471537509f,0.107160515551f}}}};
  _table_scan_column_column_numeric = TableScanCoefficientMatrix{{{{0.30762516144f,0.61525032288f,-1.0277178954f}}}};
  _table_scan_column_value_string = TableScanCoefficientMatrix{{{{0.37348066254f,0.18510574601f,0.132287638358f}}}};
  _table_scan_uncategorized = TableScanCoefficientMatrix{{{{0.143470961207f,0.212471537509f,0.107160515551f}}}};

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
                                         const AllParameterVariant& value) const {
  const auto input_row_count = table_statistics->row_count();
  auto input_reference_count = 0.0f;
  const auto output_table_statistics = table_statistics->predicate_statistics(column, predicate_condition, value);
  const auto output_row_count = output_table_statistics->row_count();
  const auto value_is_column = value.type() == typeid(LQPColumnReference);
  const auto data_type = table_statistics->column_statistics()[column]->data_type();

  const TableScanCoefficientMatrix * m = {nullptr};

  if (table_statistics->table_type() == TableType::References) {
      input_reference_count = value_is_column ? 2 * input_row_count : input_row_count;
  }

  if (data_type == DataType::String) {
    m = &_table_scan_column_value_string; // TODO(moritz) different model for column-column-string
  } else {
    m = value_is_column ? &_table_scan_column_column_numeric : &_table_scan_column_value_numeric;
  }


  // clang-format off
  const auto cost = input_row_count * (*m)[0][0] + input_reference_count * (*m)[0][1] + output_row_count * (*m)[0][2];
  // clang-format on

  return cost;
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

std::optional<Cost> CostModelSegmented::cost_table_scan_op(const TableScan& table_scan) {
  DebugAssert(table_scan.has_finished_execution(), "Operator needs to have finished");

  const auto input_table = table_scan.input_table_left()
  const auto input_row_count = input_table->row_count();
  const auto output_row_count = output_table_statistics->row_count();

}

}  // namespace opossum
