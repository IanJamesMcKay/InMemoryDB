#include "cost_model_segmented_sampler.hpp"

#include "operators/utils/flatten_pqp.hpp"
#include "operators/table_scan.hpp"
#include "sql/sql.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_query_plan.hpp"
#include "resolve_type.hpp"

namespace opossum {

void CostModelSegmentedSampler::write_samples() const {

}

void CostModelSegmentedSampler::_sample_table_scan(const TableScan& table_scan) {
  TableScanSample sample;

  sample.table_type = table_scan.input_table_left()->type();
  sample.left_data_type = table_scan.get_output()->column_data_type(table_scan.left_column_id());

  if (table_scan.right_parameter().type() == typeid(ColumnID)) {
    sample.right_data_type = table_scan.get_output()->column_data_type(boost::get<ColumnID>(table_scan.right_parameter()));
    sample.access_count = table_scan.input_table_left()->row_count() * 2;
  } else {
    DebugAssert(table_scan.right_parameter().type() == typeid(AllTypeVariant), "Unexpected right parameter");

    sample.right_data_type = data_type_from_all_type_variant(boost::get<AllTypeVariant>(table_scan.right_parameter()));
    sample.access_count = table_scan.input_table_left()->row_count();
  }

  sample.output_row_count = table_scan.get_output()->row_count();

  sample.scan_cost = static_cast<float>(table_scan.performance_data().scan.count());
  sample.output_cost = static_cast<float>(table_scan.performance_data().output.count());

  _table_scan_samples.emplace_back(sample);
}

}  // namespace opossum
