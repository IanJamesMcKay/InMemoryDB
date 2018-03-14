#include "cost_model_segmented.hpp"

#include "operators/table_scan.hpp"
#include "optimizer/table_statistics.hpp"
#include "types.hpp"
#include "resolve_type.hpp"

namespace opossum {

CostModelSegmented::TableScanSubModel CostModelSegmented::table_scan_sub_model(const CostModelSegmented::TableScanFeatures& features) {
  if (features.predicate_condition == PredicateCondition::Like || features.predicate_condition == PredicateCondition::NotLike) {
    DebugAssert(features.left_data_type == DataType::String && features.right_data_type == DataType::String, "Expected string");
    return TableScanSubModel::Like;
  } else {
    if (features.left_data_type == DataType::String) {
      if (features.right_operand_is_column) {
        return TableScanSubModel::ColumnColumnString;
      } else {
        return TableScanSubModel::ColumnValueString;
      }
    } else {
      if (features.right_operand_is_column) {
        return TableScanSubModel::ColumnColumnNumeric;
      } else {
        return TableScanSubModel::ColumnValueNumeric;
      }
    }
  }
}

CostModelSegmented::TableScanFeatures CostModelSegmented::table_scan_features(const TableScan& table_scan) {
  DebugAssert(table_scan.input_table_left(), "Operator needs input");
  DebugAssert(table_scan.get_output(), "Operator needs to be executed first");

  TableScanFeatures features;

  features.left_data_type = table_scan.get_output()->column_data_type(table_scan.left_column_id());
  features.input_row_count = table_scan.input_table_left()->row_count();

  features.right_operand_is_column = table_scan.right_parameter().type() == typeid(ColumnID);

  if (features.right_operand_is_column) {
    features.right_data_type = table_scan.get_output()->column_data_type(boost::get<ColumnID>(table_scan.right_parameter()));

    if (table_scan.input_table_left()->type() == TableType::References) {
      features.input_reference_count = features.input_row_count * 2;
    }
  } else {
    DebugAssert(table_scan.right_parameter().type() == typeid(AllTypeVariant), "Unexpected right parameter");
    features.right_data_type = data_type_from_all_type_variant(boost::get<AllTypeVariant>(table_scan.right_parameter()));

    if (table_scan.input_table_left()->type() == TableType::References) {
      features.input_reference_count = features.input_row_count;
    }
  }

  features.predicate_condition = table_scan.predicate_condition();

  features.output_row_count = table_scan.get_output()->row_count();

  if (table_scan.input_table_left()->type() == TableType::References) {
    features.output_reference_count = features.output_row_count;
  } else {
    features.output_reference_count = 0;
  }

  return features;
}

CostModelSegmented::TableScanTargets CostModelSegmented::table_scan_targets(const TableScan& table_scan) {
  DebugAssert(table_scan.input_table_left(), "Operator needs input");
  DebugAssert(table_scan.get_output(), "Operator needs to be executed first");

  TableScanTargets targets;

  targets.scan_cost = static_cast<float>(table_scan.performance_data().scan.count());
  targets.output_cost = static_cast<float>(table_scan.performance_data().output.count());
  targets.total = static_cast<float>(table_scan.performance_data().total.count());

  return targets;
}


CostModelSegmented::CostModelSegmented() {
  // clang-format off
  _join_hash_coefficients =
      JoinHashCoefficientMatrix{{{{0.5134497330789046f, 0.2855347312271709f, 0.03562409664220728f, 0.0f}},
                                 {{-3.568099221199f, 2.163215873463416f, 3.5492830521878886f, 0.0f}},
                                 {{0.16607179650238305f, 0.26172171946741085f, 0.08765293032708743f, 0.0f}},
                                 {{0.3460139111375513f, 0.030189513343270802f, 0.07910391143651066f, 0.0f}},
                                 {{0.5765816664702633f, 0.08757680844338467f, 1.4843440266345889f, 0.0f}}}};

  auto& m = _table_scan_sub_model_coefficients;

  m[TableScanSubModel::Uncategorized] = TableScanCoefficientMatrix{{{{0.143470961207f,0.212471537509f,0.107160515551f}}}};
  m[TableScanSubModel::ColumnValueNumeric] = TableScanCoefficientMatrix{{{{0.143470961207f,0.212471537509f,0.107160515551f}}}};
  m[TableScanSubModel::ColumnColumnNumeric] = TableScanCoefficientMatrix{{{{0.30762516144f,0.61525032288f,-1.0277178954f}}}};
  m[TableScanSubModel::ColumnValueString] = TableScanCoefficientMatrix{{{{0.143470961207f,0.212471537509f,0.107160515551f}}}};
  m[TableScanSubModel::ColumnValueNumeric] = TableScanCoefficientMatrix{{{{0.143470961207f,0.212471537509f,0.107160515551f}}}};
  m[TableScanSubModel::Like] = TableScanCoefficientMatrix{{{{0.143470961207f,0.212471537509f,0.107160515551f}}}};

  _join_sort_merge_coefficients = JoinSortMergeCoefficientMatrix{{{{0.4391338428178249f, 0.09476596343484817f, 0.0f}}}};

  _product_coefficients = ProductCoefficientMatrix{{{{1.0960099722478653f}}}};
  // clang-format on
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
  const auto output_table_statistics = table_statistics->predicate_statistics(column, predicate_condition, value);

  TableScanFeatures features;
  features.left_data_type = table_statistics->column_statistics()[column]->data_type();

  features.right_operand_is_column = value.type() == typeid(ColumnID);

  if (features.right_operand_is_column) {
    features.right_data_type = table_statistics->column_statistics()[boost::get<ColumnID>(value)]->data_type();
  } else {
    features.right_data_type = data_type_from_all_type_variant(boost::get<AllTypeVariant>(value));
  }

  features.predicate_condition = predicate_condition;
  features.input_row_count = table_statistics->row_count();
  features.input_reference_count = 0;
  features.output_row_count = output_table_statistics->row_count();

  if (table_statistics->table_type() == TableType::References) {
    features.input_reference_count = features.right_operand_is_column ? 2 * features.input_row_count : features.input_row_count;
    features.output_reference_count = features.output_row_count;
  } else {
    features.input_reference_count = 0;
    features.output_reference_count = 0;
  }

  return cost_table_scan_impl(features);
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

std::optional<Cost> CostModelSegmented::cost_table_scan_op(const TableScan& table_scan) const {
  return cost_table_scan_impl(table_scan_features(table_scan));
}

Cost CostModelSegmented::cost_table_scan_impl(const TableScanFeatures& features) const {
  const auto submodel = table_scan_sub_model(features);

  const auto coefficients_iter = _table_scan_sub_model_coefficients.find(submodel);
  DebugAssert(coefficients_iter != _table_scan_sub_model_coefficients.end(), "No coefficients for submodel");

  const TableScanCoefficientMatrix& m = coefficients_iter->second;

  // clang-format off
  const auto cost = features.input_row_count * m[0][0] + features.input_reference_count * m[0][1] + features.output_row_count * m[0][2];
  // clang-format on

  return cost;
}

}  // namespace opossum
