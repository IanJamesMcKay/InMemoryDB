#include "cost_model_segmented.hpp"

#include "operators/product.hpp"
#include "operators/table_scan.hpp"
#include "operators/union_positions.hpp"
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

CostModelSegmented::ProductSubModel CostModelSegmented::product_sub_model(const ProductFeatures& features) {
  return ProductSubModel::Standard;
}

CostModelSegmented::ProductFeatures CostModelSegmented::product_features(const Product& product) {
  ProductFeatures features;
  features.output_value_count = (product.input_table_left()->row_count() * product.input_table_right()->row_count()) * (product.input_table_left()->column_count() + product.input_table_right()->column_count());
  return features;
}

CostModelSegmented::ProductTargets CostModelSegmented::product_targets(const Product& product) {
  ProductTargets targets;
  targets.total = static_cast<Cost>(product.performance_data().total.count());
  return targets;
}

Cost CostModelSegmented::cost_product(const std::shared_ptr<TableStatistics>& table_statistics_left,
                                      const std::shared_ptr<TableStatistics>& table_statistics_right) const  {
  ProductFeatures features;
  features.output_value_count = (table_statistics_left->row_count() * table_statistics_right->row_count()) * (table_statistics_left->column_statistics().size() + table_statistics_right->column_statistics().size());
  return cost_product_impl(features);
}

Cost CostModelSegmented::cost_product_impl(const ProductFeatures& features) const {
  const auto submodel = product_sub_model(features);

  const auto coefficients_iter = _product_sub_model_coefficients.find(submodel);
  DebugAssert(coefficients_iter != _product_sub_model_coefficients.end(), "No coefficients for submodel");

  const ProductCoefficientMatrix & m = coefficients_iter->second;

  // clang-format off
  const auto cost = features.output_value_count * m[0][0];
  // clang-format on

  return cost;
}

std::optional<Cost> CostModelSegmented::cost_product_op(const Product& product, const OperatorCostMode operator_cost_mode) const  {
  switch (operator_cost_mode) {
    case OperatorCostMode::PredictedCost: return cost_product_impl(product_features(product));
    case OperatorCostMode::TargetCost:
      return static_cast<Cost>(
      product.performance_data().total.count());
  }

  Fail("Unreachable");
}

CostModelSegmented::UnionPositionsSubModel CostModelSegmented::union_positions_sub_model(const UnionPositionsFeatures& features) {
  return UnionPositionsSubModel::Standard;
}

CostModelSegmented::UnionPositionsFeatures CostModelSegmented::union_positions_features(const UnionPositions& union_positions) {
  UnionPositionsFeatures features;
  const auto row_count_left = union_positions.input_table_left()->row_count();
  const auto row_count_right = union_positions.input_table_right()->row_count();
  features.n_log_n_row_count_left = row_count_left > 0.0f ? row_count_left * std::log(row_count_left) : 0.0f;
  features.n_log_n_row_count_right = row_count_right > 0.0f ? row_count_right * std::log(row_count_right) : 0.0f;
  features.output_value_count = union_positions.get_output()->row_count();
  return features;
}

CostModelSegmented::UnionPositionsTargets CostModelSegmented::union_positions_targets(const UnionPositions& union_positions) {
  UnionPositionsTargets targets;
  targets.init = static_cast<Cost>(union_positions.performance_data().init.count());
  targets.sort = static_cast<Cost>(union_positions.performance_data().sort.count());
  targets.output = static_cast<Cost>(union_positions.performance_data().output.count());
  targets.total = static_cast<Cost>(union_positions.performance_data().total.count());
  return targets;
}

Cost CostModelSegmented::cost_union_positions(const std::shared_ptr<TableStatistics>& table_statistics_left,
                          const std::shared_ptr<TableStatistics>& table_statistics_right) const {
  UnionPositionsFeatures features;
  const auto row_count_left = table_statistics_left->row_count();
  const auto row_count_right = table_statistics_right->row_count();
  features.n_log_n_row_count_left = row_count_left > 0.0f ? row_count_left * std::log(row_count_left) : 0.0f;
  features.n_log_n_row_count_right = row_count_right > 0.0f ? row_count_right * std::log(row_count_right) : 0.0f;
  features.output_value_count = table_statistics_left->generate_disjunction_statistics(table_statistics_right)->row_count() * table_statistics_left->column_statistics().size();
  return cost_union_positions_impl(features);
}

Cost CostModelSegmented::cost_union_positions_impl(const UnionPositionsFeatures& features) const {
  const auto submodel = union_positions_sub_model(features);

  const auto coefficients_iter = _union_positions_sub_model_coefficients.find(submodel);
  DebugAssert(coefficients_iter != _union_positions_sub_model_coefficients.end(), "No coefficients for submodel");

  const UnionPositionsCoefficientMatrix & m = coefficients_iter->second;

  // clang-format off
  const auto init = features.n_log_n_row_count_left * m[0][0] +
                    features.n_log_n_row_count_right * m[0][1] +
                    features.output_value_count * m[0][2];
  const auto sort = features.n_log_n_row_count_left * m[1][0] +
                    features.n_log_n_row_count_right * m[1][1] +
                    features.output_value_count * m[1][2];
  const auto output = features.n_log_n_row_count_left * m[2][0] +
                    features.n_log_n_row_count_right * m[2][1] +
                    features.output_value_count * m[2][2];
  // clang-format on

  return init + sort + output;
}

std::optional<Cost> CostModelSegmented::cost_union_positions_op(const UnionPositions& union_positions, const OperatorCostMode operator_cost_mode) const {
  switch (operator_cost_mode) {
    case OperatorCostMode::PredictedCost: return cost_union_positions_impl(union_positions_features(union_positions));
    case OperatorCostMode::TargetCost:
      return static_cast<Cost>(
      union_positions.performance_data().total.count());
  }

  Fail("Unreachable");
}

CostModelSegmented::CostModelSegmented() {
  // clang-format off
#if IS_DEBUG
  _table_scan_sub_model_coefficients[TableScanSubModel::ColumnValueNumeric] = TableScanCoefficientMatrix{{{{0.151131599208f,0.15565695035f,0.112082904016f,0.0248249284018f}}}};
  _table_scan_sub_model_coefficients[TableScanSubModel::ColumnColumnNumeric] = TableScanCoefficientMatrix{{{{4.36373080703e-13f,0.265919959986f,0.0f,0.0f}}}};
  _table_scan_sub_model_coefficients[TableScanSubModel::ColumnValueString] = TableScanCoefficientMatrix{{{{0.389646092813f,0.150529434248f,0.140562838787f,0.0576460191188f}}}};
  _table_scan_sub_model_coefficients[TableScanSubModel::Like] = TableScanCoefficientMatrix{{{{8.0548057056f,0.0f,0.0f,0.0f}}}};
  _join_hash_sub_model_coefficients[JoinHashSubModel::Standard] = JoinHashCoefficientMatrix{{
                                                                                            {{0.236218521822f,0.159532133258f,0.335379374947f,0.0112769794937f,0.0110738685879f}},
                                                                                            {{0.0276370834782f,0.0012261035992f,0.042467333437f,-0.00343449419895f,0.00204183088907f}},
                                                                                            {{0.0124246779124f,-0.00289462813578f,1.06357321626f,-0.00464682141054f,-0.00778809474253f}},
                                                                                            {{0.174016568831f,-0.00136720303356f,0.204186038348f,-0.0182493905997f,0.215731049808f}},
                                                                                            {{-0.11659053695f,1.79166876186f,0.155007198049f,0.921496783153f,0.954651366687f}},
                                                                                            {{0.664478195252f,2.08830878681f,9.93761939556f,-0.42970446016f,1.73536148128f}}}};

  _product_sub_model_coefficients[ProductSubModel::Standard] = ProductCoefficientMatrix{{{{0.0493615676317f}}}};
  _union_positions_sub_model_coefficients[UnionPositionsSubModel::Standard] = UnionPositionsCoefficientMatrix{{
                                                                                                      {{0.00361414884244f,0.00366948928481f,0.0f}},
                                                                                                      {{0.17550233854f,0.173866054327f,0.236154285219f}},
                                                                                                      {{0.0176795823411f,0.00400452890858f,0.166886875874f}}}};

#else
  _table_scan_sub_model_coefficients[TableScanSubModel::ColumnValueNumeric] = TableScanCoefficientMatrix{{{{0.00357555320143f,0.0189155666155f,0.00185955501541f,0.0f}}}};
  _table_scan_sub_model_coefficients[TableScanSubModel::ColumnColumnNumeric] = TableScanCoefficientMatrix{{{{2.30772081893e-15f,0.0262691992411f,0.0f,0.0f}}}};

  _table_scan_sub_model_coefficients[TableScanSubModel::ColumnValueString] = TableScanCoefficientMatrix{{{{0.0122686866294f,0.0183285792719f,0.00602195854223f,0.0325447573152f}}}};
  _table_scan_sub_model_coefficients[TableScanSubModel::Like] = TableScanCoefficientMatrix{{{{1.96376974778f,0.0f,11.2450352145f,0.0f}}}};
  _join_hash_sub_model_coefficients[JoinHashSubModel::Standard] = JoinHashCoefficientMatrix{{
    {{0.0103009953807f,0.0229193016843f,0.0207545157144f,0.00649976207153f,0.00349376958911f}},
    {{0.00418425435483f,-0.000142189284019f,0.00755619815574f,-0.000414539011173f,0.000766298349721f}},
    {{0.00482515927315f,-0.0018375842829f,0.229990179639f,0.00506143915308f,-0.00242361359393f}},
    {{0.0359717530683f,-0.00227851496324f,0.154465463074f,-0.00436081773056f,0.0035045973279f}},
    {{-0.0643601748759f,0.328974298062f,-0.211668488646f,0.231701599006f,0.100841779697f}},
    {{0.0226343617879f,0.356091866462f,1.16167842564f,0.149531354133f,0.143323050762f}}}};

  _product_sub_model_coefficients[ProductSubModel::Standard] = ProductCoefficientMatrix{{{{0.0136870174701f}}}};
  _union_positions_sub_model_coefficients[UnionPositionsSubModel::Standard] = UnionPositionsCoefficientMatrix{{
    {{0.000238876475788f,0.000247689759188f,0.0f}},
    {{0.00178812744648f,0.00182670264568f,0.000323476902142f}},
    {{0.000732239993574f,0.000390231877899f,0.0021468294427f}}}};
#endif

  // clang-format on

  _join_sort_merge_coefficients = JoinSortMergeCoefficientMatrix{{{{0.4391338428178249f, 0.09476596343484817f, 0.0f}}}};
}

Cost CostModelSegmented::cost_join_sort_merge(const std::shared_ptr<TableStatistics>& table_statistics_left,
                                              const std::shared_ptr<TableStatistics>& table_statistics_right,
                                              const JoinMode join_mode, const ColumnIDPair& join_column_ids,
                                              const PredicateCondition predicate_condition) const {
  return 0.0f;
}

}  // namespace opossum
