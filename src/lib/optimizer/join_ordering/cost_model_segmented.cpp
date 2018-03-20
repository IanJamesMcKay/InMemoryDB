#include "cost_model_segmented.hpp"

#include "operators/table_scan.hpp"
#include "operators/join_hash.hpp"
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

Cost CostModelSegmented::cost_table_scan_impl(const TableScanFeatures& features) const {
  const auto submodel = table_scan_sub_model(features);

  const auto coefficients_iter = _table_scan_sub_model_coefficients.find(submodel);
  DebugAssert(coefficients_iter != _table_scan_sub_model_coefficients.end(), "No coefficients for submodel");

  const TableScanCoefficientMatrix& m = coefficients_iter->second;

  // clang-format off
  const auto cost = features.input_row_count * m[0][0] +
                    features.input_reference_count * m[0][1] +
                    features.output_row_count * m[0][2] +
                    features.output_reference_count * m[0][3];
  // clang-format on

  return cost;
}

std::optional<Cost> CostModelSegmented::cost_table_scan_op(const TableScan& table_scan, const OperatorCostMode operator_cost_mode) const {
  switch (operator_cost_mode) {
    case OperatorCostMode::PredictedCost: return cost_table_scan_impl(table_scan_features(table_scan));
    case OperatorCostMode::TargetCost: return static_cast<Cost>(table_scan.performance_data().total.count());
  }

  Fail("Unreachable");
}

CostModelSegmented::JoinHashSubModel CostModelSegmented::join_hash_sub_model(const JoinHashFeatures& features) {
  return JoinHashSubModel::Standard;
}

CostModelSegmented::JoinHashFeatures CostModelSegmented::join_hash_features(const JoinHash& join_hash) {
  DebugAssert(join_hash.input_table_left(), "Operator needs input");
  DebugAssert(join_hash.get_output(), "Operator needs to be executed first");

  const auto inputs_swapped = join_hash.input_table_left()->row_count() < join_hash.input_table_right()->row_count();
  const auto major_input_table = inputs_swapped ? join_hash.input_table_right() : join_hash.input_table_left();
  const auto minor_input_table = inputs_swapped ? join_hash.input_table_left() : join_hash.input_table_right();

  JoinHashFeatures features;

  features.major_input_row_count = major_input_table->row_count();
  features.major_input_reference_count = 0;
  if (major_input_table->type() == TableType::References) {
    features.major_input_reference_count = features.major_input_row_count;
  }

  features.minor_input_row_count = minor_input_table->row_count();
  features.minor_input_reference_count = 0;
  if (minor_input_table->type() == TableType::References) {
    features.minor_input_reference_count = features.major_input_row_count;
  }

  features.output_row_count = join_hash.get_output()->row_count();

  return features;
}

CostModelSegmented::JoinHashTargets CostModelSegmented::join_hash_targets(const JoinHash& join_hash) {
  DebugAssert(join_hash.input_table_left(), "Operator needs input");
  DebugAssert(join_hash.get_output(), "Operator needs to be executed first");

  JoinHashTargets targets;

  targets.materialization = static_cast<float>(join_hash.performance_data().materialization.count());
  targets.partitioning = static_cast<float>(join_hash.performance_data().partitioning.count());
  targets.build = static_cast<float>(join_hash.performance_data().build.count());
  targets.probe = static_cast<float>(join_hash.performance_data().probe.count());
  targets.output = static_cast<float>(join_hash.performance_data().output.count());
  targets.total = static_cast<float>(join_hash.performance_data().total.count());

  return targets;
}

Cost CostModelSegmented::cost_join_hash(const std::shared_ptr<TableStatistics>& table_statistics_left,
                                        const std::shared_ptr<TableStatistics>& table_statistics_right,
                                        const JoinMode join_mode, const ColumnIDPair& join_column_ids,
                                        const PredicateCondition predicate_condition) const {

  const auto inputs_swapped = table_statistics_left->row_count() < table_statistics_right->row_count();
  const auto major_input_statistics = inputs_swapped ? table_statistics_right : table_statistics_left;
  const auto minor_input_statistics = inputs_swapped ? table_statistics_left : table_statistics_right;

  JoinHashFeatures features;

  features.major_input_row_count = major_input_statistics->row_count();
  features.major_input_reference_count = 0;
  if (major_input_statistics->table_type() == TableType::References) {
    features.major_input_reference_count = features.major_input_row_count;
  }

  features.minor_input_row_count = minor_input_statistics->row_count();
  features.minor_input_reference_count = 0;
  if (minor_input_statistics->table_type() == TableType::References) {
    features.minor_input_reference_count = features.major_input_row_count;
  }

  const auto output_statistics = table_statistics_left->generate_predicated_join_statistics(
    table_statistics_right, join_mode, join_column_ids, predicate_condition
  );

  features.output_row_count = output_statistics->row_count();

  return cost_join_hash_impl(features);
}

Cost CostModelSegmented::cost_join_hash_impl(const JoinHashFeatures& features) const {
  const auto sub_model = join_hash_sub_model(features);

  const auto coefficients_iter = _join_hash_sub_model_coefficients.find(sub_model);
  Assert(coefficients_iter != _join_hash_sub_model_coefficients.end(), "No coefficients registered");
  const auto& m = coefficients_iter->second;

  const auto apply_coefficients = [&](const auto& coefs) {
    return
      features.major_input_row_count * coefs[0] +
      features.major_input_reference_count * coefs[1] +
      features.minor_input_row_count * coefs[2] +
      features.minor_input_reference_count * coefs[3] +
      features.output_row_count * coefs[4];
  };

  // clang-format off
  const auto materialization = apply_coefficients(m[0]);
  const auto partitioning = apply_coefficients(m[1]);
  const auto build = apply_coefficients(m[2]);
  const auto probe = apply_coefficients(m[3]);
  //const auto output = apply_coefficients(m[4]);

  return materialization + partitioning + build + probe;
}

std::optional<Cost> CostModelSegmented::cost_join_hash_op(const JoinHash& join_hash, const OperatorCostMode operator_cost_mode) const {
  switch (operator_cost_mode) {
    case OperatorCostMode::PredictedCost: return cost_join_hash_impl(join_hash_features(join_hash));
    case OperatorCostMode::TargetCost:
      return static_cast<Cost>(
        join_hash.performance_data().materialization.count() +
        join_hash.performance_data().partitioning.count() +
        join_hash.performance_data().build.count() +
        join_hash.performance_data().probe.count());
  }

  Fail("Unreachable");
}

CostModelSegmented::CostModelSegmented() {
  // clang-format off
#if IS_DEBUG
  _table_scan_sub_model_coefficients[TableScanSubModel::ColumnValueNumeric] = TableScanCoefficientMatrix{{{{0.153536888722f,0.171785002473f,0.123031861379f,0.00575314830326f}}}};
  _table_scan_sub_model_coefficients[TableScanSubModel::ColumnColumnNumeric] = TableScanCoefficientMatrix{{{{0.0f,0.240739447571f,0.114426983752f,0.0f}}}};
  _table_scan_sub_model_coefficients[TableScanSubModel::ColumnValueString] = TableScanCoefficientMatrix{{{{0.402630625817f,0.142415145234f,0.103431554751f,0.060795953428f}}}};
  _table_scan_sub_model_coefficients[TableScanSubModel::Like] = TableScanCoefficientMatrix{{{{5.49454424291f,0.0f,39.4321564737f,0.0f}}}};
  _join_hash_sub_model_coefficients[JoinHashSubModel::Standard] = JoinHashCoefficientMatrix{{
                                                                                            {{0.234863571949f,0.143030348877f,0.184927415472f,0.0494761506483f,0.0165322914404f}},
                                                                                            {{0.0290688797497f,-0.00129454807982f,0.0306660287466f,-0.00195512735435f,0.00240595418867f}},
                                                                                            {{0.0133633015818f,-0.0058356843756f,0.996310673253f,0.0115764473203f,-0.00592612412216f}},
                                                                                            {{0.16548266813f,-0.0182245939098f,0.183679336787f,-0.0169706211479f,0.221258012519f}},
                                                                                            {{-0.17359429104f,1.81469002739f,-3.86058445369f,1.83166903226f,1.05222272565f}},
                                                                                            {{0.577078248674f,2.10035578547f,12.3853602915f,-0.952023529569f,1.69834517341f}}}};

#else
  _table_scan_sub_model_coefficients[TableScanSubModel::ColumnValueNumeric] = TableScanCoefficientMatrix{{{{0.00273675608795f,0.0198991965615f,0.00588407162284f,0.0f}}}};
_table_scan_sub_model_coefficients[TableScanSubModel::ColumnColumnNumeric] = TableScanCoefficientMatrix{{{{1.48812511808e-14f,0.0261512533175f,0.0f,0.0f}}}};
_table_scan_sub_model_coefficients[TableScanSubModel::ColumnValueString] = TableScanCoefficientMatrix{{{{0.0126773171369f,0.0344768850053f,0.00314812762259f,0.0f}}}};
_table_scan_sub_model_coefficients[TableScanSubModel::Like] = TableScanCoefficientMatrix{{{{2.13261518175f,0.0f,7.88869062342f,0.0f}}}};
_join_hash_sub_model_coefficients[JoinHashSubModel::Standard] = JoinHashCoefficientMatrix{{
  {{0.0141496060631f,0.020746798901f,0.0175540404666f,0.00239816700595f,0.00307865903684f}},
  {{0.00415135443286f,-0.000164957945402f,0.00986727846429f,-0.0013993019131f,0.000701864876809f}},
  {{0.00550650524567f,-0.00249623157084f,0.215998489697f,0.00186924552861f,-0.00239109476619f}},
  {{0.0368550457481f,-0.00215788907073f,0.180075667108f,-0.0157358394594f,0.00206040725543f}},
  {{-0.0553951891686f,0.341781322995f,-0.0446918905048f,0.149867893184f,0.0841159858309f}},
  {{0.0288343862905f,0.380103861638f,1.86860303586f,-0.0798231970635f,0.110383537425f}}}};

#endif

  _join_sort_merge_coefficients = JoinSortMergeCoefficientMatrix{{{{0.4391338428178249f, 0.09476596343484817f, 0.0f}}}};

  _product_coefficients = ProductCoefficientMatrix{{{{1.0960099722478653f}}}};
  // clang-format on
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
