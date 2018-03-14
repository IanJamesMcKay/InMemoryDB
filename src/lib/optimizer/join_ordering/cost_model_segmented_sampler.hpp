#pragma once

#include "abstract_cost_model_sampler.hpp"
#include "cost_model_segmented.hpp"
#include "cost.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

class TableScan;

class CostModelSegmentedSampler : public AbstractCostModelSampler {
 public:
  struct TableScanSample {
    CostModelSegmented::TableScanFeatures features;
    CostModelSegmented::TableScanTargets targets;

    friend std::ostream& operator<<(std::ostream& stream, const TableScanSample& sample);
  };

  void write_samples() const override;

 protected:
  void _sample_table_scan(const TableScan& table_scan) override;

 private:
  std::vector<TableScanSample> _table_scan_samples;
};

}  // namespace opossum
