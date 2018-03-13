#pragma once

#include "abstract_cost_model_sampler.hpp
#include "cost.hpp
#include "all_type_variant.hpp
#include "types.hpp

namespace opossum {

class TableScan;

class CostModelSegmentedSampler : public AbstractCostModelSampler {
 public:
  struct TableScanSample {
    TableType table_type{TableType::Data};

    DataType left_data_type{DataType::Int};
    DataType right_data_type{DataType::Int};

    size_t access_count{0}; // Combines number of data accesses for left and right operand
    size_t output_row_count{0};

    // Target variables
    Cost scan_cost{0};
    Cost output_cost{0};
  };

  void write_samples() const override;

 protected:
  void _sample_table_scan(const TableScan& table_scan) override;

 private:
  std::vector<TableScanSample> _table_scan_samples;
};

}  // namespace opossum
