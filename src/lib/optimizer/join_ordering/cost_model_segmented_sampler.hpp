#pragma once

#include "abstract_cost_model_sampler.hpp"
#include "cost.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

class TableScan;

class CostModelSegmentedSampler : public AbstractCostModelSampler {
 public:
  struct TableScanSample {
    DataType left_data_type{DataType::Int};
    DataType right_data_type{DataType::Int};

    bool right_operand_is_column{false};

    size_t input_reference_count{0};
    size_t input_row_count{0};
    size_t output_row_count{0};

    // Target variables
    Cost scan_cost{0};
    Cost output_cost{0};
    Cost total{0};

    friend std::ostream& operator<<(std::ostream& stream, const TableScanSample& sample);
  };

  void write_samples() const override;

 protected:
  void _sample_table_scan(const TableScan& table_scan) override;

 private:
  std::vector<TableScanSample> _table_scan_samples;
};

}  // namespace opossum
