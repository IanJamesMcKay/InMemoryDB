#include "cost_model_segmented_sampler.hpp"

#include <fstream>

#include "operators/utils/flatten_pqp.hpp"
#include "operators/join_hash.hpp"
#include "operators/table_scan.hpp"
#include "sql/sql.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_query_plan.hpp"
#include "resolve_type.hpp"

namespace opossum {

std::ostream& operator<<(std::ostream& stream, const CostModelSegmentedSampler::TableScanSample& sample) {
  stream << sample.features.input_row_count << ",";
  stream << sample.features.input_reference_count << ",";
  stream << sample.features.output_row_count << ",";
  stream << sample.features.output_reference_count << ",";
  stream << sample.targets.scan_cost << ",";
  stream << sample.targets.output_cost << ",";
  stream << sample.targets.total << ",";

  stream << std::endl;

  return stream;
}

std::ostream& operator<<(std::ostream& stream, const CostModelSegmentedSampler::JoinHashSample& sample) {
  stream << sample.features.major_input_row_count << ",";
  stream << sample.features.major_input_reference_count << ",";
  stream << sample.features.minor_input_row_count << ",";
  stream << sample.features.minor_input_reference_count << ",";
  stream << sample.features.output_row_count << ",";

  stream << sample.targets.materialization << ",";
  stream << sample.targets.partitioning << ",";
  stream << sample.targets.build << ",";
  stream << sample.targets.probe << ",";
  stream << sample.targets.output << ",";
  stream << sample.targets.total << ",";

  stream << std::endl;

  return stream;
}

std::ostream& operator<<(std::ostream& stream, const CostModelSegmentedSampler::ProductSample& sample) {
  stream << sample.features.output_row_count << ",";
  stream << sample.features.output_value_count << ",";

  stream << sample.targets.total << ",";

  stream << std::endl;

  return stream;
}

std::ostream& operator<<(std::ostream& stream, const CostModelSegmentedSampler::UnionPositionsSample& sample) {
  stream << sample.features.row_count_left << ",";
  stream << sample.features.row_count_right << ",";
  stream << sample.features.output_value_count << ",";

  stream << sample.targets.total << ",";

  stream << std::endl;

  return stream;
}

void CostModelSegmentedSampler::write_samples() const {
  auto prefix = std::string{"samples/"};

  prefix += IS_DEBUG ? "debug_" : "release_";
  
  auto column_value_numeric = std::ofstream(prefix + "table_scan_column_value_numeric.csv");
  auto column_column_numeric = std::ofstream(prefix + "table_scan_column_column_numeric.csv");
  auto column_value_string = std::ofstream(prefix + "table_scan_column_value_string.csv");
  auto column_column_string = std::ofstream(prefix + "table_scan_column_column_string.csv");
  auto like = std::ofstream(prefix + "table_scan_like.csv");
  auto uncategorized_scan = std::ofstream(prefix + "table_scan_uncategorized.csv");

  for (const auto& sample : _table_scan_samples) {
    // clang-format off
    switch (CostModelSegmented::table_scan_sub_model(sample.features)) {
      case CostModelSegmented::TableScanSubModel::ColumnValueString: column_value_string << sample; break;
      case CostModelSegmented::TableScanSubModel::ColumnColumnString: column_column_string << sample; break;
      case CostModelSegmented::TableScanSubModel::ColumnValueNumeric: column_value_numeric << sample; break;
      case CostModelSegmented::TableScanSubModel::ColumnColumnNumeric: column_column_numeric << sample; break;
      case CostModelSegmented::TableScanSubModel::Like: like << sample; break;
      case CostModelSegmented::TableScanSubModel::Uncategorized: uncategorized_scan << sample; break;
    }
    // clang-format on
  }


  auto join_hash_standard = std::ofstream(prefix + "join_hash_standard.csv");

  for (const auto& sample : _join_hash_samples) {
    switch(CostModelSegmented::join_hash_sub_model(sample.features)) {
      case CostModelSegmented::JoinHashSubModel::Standard: join_hash_standard << sample; break;
    }
  }

  auto product_standard = std::ofstream(prefix + "product_standard.csv");

  for (const auto& sample : _product_samples) {
    switch(CostModelSegmented::product_sub_model(sample.features)) {
      case CostModelSegmented::ProductCostModel::Standard: product_standard << sample; break;
    }
  }

  auto union_positions_standard = std::ofstream(prefix + "union_positions_standard.csv");

  for (const auto& sample : _union_positions_samples) {
    switch(CostModelSegmented::union_positions_sub_model(sample.features)) {
      case CostModelSegmented::UnionPositionsCostModel::Standard: union_positions_standard << sample; break;
    }
  }
}

void CostModelSegmentedSampler::_sample_table_scan(const TableScan& table_scan) {
  TableScanSample sample{
    CostModelSegmented::table_scan_features(table_scan),
    CostModelSegmented::table_scan_targets(table_scan)
  };
  
  _table_scan_samples.emplace_back(sample);
}

void CostModelSegmentedSampler::_sample_join_hash(const JoinHash& join_hash) {
  JoinHashSample sample{
    CostModelSegmented::join_hash_features(join_hash),
    CostModelSegmented::join_hash_targets(join_hash)
  };

  _join_hash_samples.emplace_back(sample);
}

void CostModelSegmentedSampler::_sample_product(const Product& product) {
  ProductSample sample{
    CostModelSegmented::product_features(product),
    CostModelSegmented::product_targets(product)
  };

  _product_samples.emplace_back(sample);
}

void CostModelSegmentedSampler::_sample_union_positions(const UnionPositions& union_positions) {
  UnionPositionsSample sample{
    CostModelSegmented::union_positions_features(union_positions),
    CostModelSegmented::union_positions_targets(union_positions)
  };

  _union_positions_samples.emplace_back(sample);
}

}  // namespace opossum
