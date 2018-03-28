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
  struct TableScanSample final {
    CostModelSegmented::TableScanFeatures features;
    CostModelSegmented::TableScanTargets targets;

    friend std::ostream& operator<<(std::ostream& stream, const TableScanSample& sample);
  };

  struct JoinHashSample final {
    CostModelSegmented::JoinHashFeatures features;
    CostModelSegmented::JoinHashTargets targets;

    friend std::ostream& operator<<(std::ostream& stream, const JoinHashSample& sample);
  };

  struct ProductSample final {
    CostModelSegmented::ProductFeatures features;
    CostModelSegmented::ProductTargets targets;

    friend std::ostream& operator<<(std::ostream& stream, const ProductSample& sample);
  };

  struct UnionPositionsSample final {
    CostModelSegmented::UnionPositionsFeatures features;
    CostModelSegmented::UnionPositionsTargets targets;

    friend std::ostream& operator<<(std::ostream& stream, const UnionPositionsSample& sample);
  };

  void write_samples() const override;

 protected:
  void _sample_table_scan(const TableScan& table_scan) override;
  void _sample_join_hash(const JoinHash& join_hash) override;
  void _sample_product(const Product& product) override;
  void _sample_union_positions(const UnionPositions& union_positions) override;

 private:
  std::vector<TableScanSample> _table_scan_samples;
  std::vector<JoinHashSample> _join_hash_samples;
  std::vector<ProductSample> _product_samples;
  std::vector<UnionPositionsSample> _union_positions_samples;
};

}  // namespace opossum
