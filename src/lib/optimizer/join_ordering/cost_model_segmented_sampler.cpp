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
  stream << sample.input_row_count << ",";
  stream << sample.input_reference_count << ",";
  stream << sample.output_row_count << ",";
  stream << sample.scan_cost << ",";
  stream << sample.output_cost << ",";
  stream << sample.total << ",";

  stream << std::endl;

  return stream;
}

void CostModelSegmentedSampler::write_samples() const {
  auto prefix = std::string{"samples/"};

  prefix += IS_DEBUG ? "debug_" : "release_";

  auto column_value_numeric_scan_samples = std::ofstream(prefix + "table_scan_column_value_numeric.csv");
  auto column_column_numeric_scan_samples = std::ofstream(prefix + "table_scan_column_column_numeric.csv");
  auto column_value_string_scan_samples = std::ofstream(prefix + "table_scan_column_value_string.csv");
  auto uncategorized_scan_samples = std::ofstream(prefix + "table_scan_uncategorized.csv");

  for (const auto& sample : _table_scan_samples) {
    if (sample.left_data_type == DataType::String) {
      if (sample.right_operand_is_column) {
        uncategorized_scan_samples << sample;
      } else {
        column_value_string_scan_samples << sample;
      }
    } else {
      if (sample.right_operand_is_column) {
        column_column_numeric_scan_samples << sample;
      } else {
        column_value_numeric_scan_samples << sample;
      }
    }
  }
}

void CostModelSegmentedSampler::_sample_table_scan(const TableScan& table_scan) {
  TableScanSample sample;

  sample.left_data_type = table_scan.get_output()->column_data_type(table_scan.left_column_id());
  sample.input_row_count = table_scan.input_table_left()->row_count();

  sample.right_operand_is_column = table_scan.right_parameter().type() == typeid(ColumnID);

  if (sample.right_operand_is_column) {
    sample.right_data_type = table_scan.get_output()->column_data_type(boost::get<ColumnID>(table_scan.right_parameter()));

    if (table_scan.input_table_left()->type() == TableType::References) {
      sample.input_reference_count = sample.input_row_count * 2;
    }
  } else {
    DebugAssert(table_scan.right_parameter().type() == typeid(AllTypeVariant), "Unexpected right parameter");
    sample.right_data_type = data_type_from_all_type_variant(boost::get<AllTypeVariant>(table_scan.right_parameter()));

    if (table_scan.input_table_left()->type() == TableType::References) {
      sample.input_reference_count = sample.input_row_count;
    }
  }

  sample.output_row_count = table_scan.get_output()->row_count();

  sample.scan_cost = static_cast<float>(table_scan.performance_data().scan.count());
  sample.output_cost = static_cast<float>(table_scan.performance_data().output.count());
  sample.total = static_cast<float>(table_scan.performance_data().total.count());

  _table_scan_samples.emplace_back(sample);
}

}  // namespace opossum
