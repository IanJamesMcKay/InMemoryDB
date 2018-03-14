#include "cost_model_segmented_sampler.hpp"

#include <fstream>

#include "operators/utils/flatten_pqp.hpp"
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

void CostModelSegmentedSampler::write_samples() const {
  auto prefix = std::string{"samples/"};

  prefix += IS_DEBUG ? "debug_" : "release_";
  
  auto column_value_numeric = std::ofstream(prefix + "table_scan_column_value_numeric.csv");
  auto column_column_numeric = std::ofstream(prefix + "table_scan_column_column_numeric.csv");
  auto column_value_string = std::ofstream(prefix + "table_scan_column_value_string.csv");
  auto column_column_string = std::ofstream(prefix + "table_scan_column_column_string.csv");
  auto like = std::ofstream(prefix + "table_scan_like.csv");
  auto uncategorized = std::ofstream(prefix + "table_scan_uncategorized.csv");

  for (const auto& sample : _table_scan_samples) {
    // clang-format off
    switch (CostModelSegmented::table_scan_sub_model(sample.features)) {
      case CostModelSegmented::TableScanSubModel::ColumnValueString: column_value_string << sample; break;
      case CostModelSegmented::TableScanSubModel::ColumnColumnString: column_column_string << sample; break;
      case CostModelSegmented::TableScanSubModel::ColumnValueNumeric: column_value_numeric << sample; break;
      case CostModelSegmented::TableScanSubModel::ColumnColumnNumeric: column_column_numeric << sample; break;
      case CostModelSegmented::TableScanSubModel::Like: like << sample; break;
      case CostModelSegmented::TableScanSubModel::Uncategorized: uncategorized << sample; break;
      default:
        Fail("Unhandled sub model");
    }
    // clang-format on
  }
}

void CostModelSegmentedSampler::_sample_table_scan(const TableScan& table_scan) {
  TableScanSample sample{
    CostModelSegmented::table_scan_features(table_scan),
    CostModelSegmented::table_scan_targets(table_scan)
  };
  
  _table_scan_samples.emplace_back(sample);
}

}  // namespace opossum
