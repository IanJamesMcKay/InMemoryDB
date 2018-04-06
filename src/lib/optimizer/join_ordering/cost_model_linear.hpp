#pragma once

#include <map>
#include <vector>

#include "abstract_cost_model.hpp"
#include "cost_feature.hpp"

namespace opossum {

enum class CostModelLinearSubOperator {
  JoinHash,
  JoinSortMerge,
  Product,
  UnionPosition,
  TableScanColumnValueNumeric,
  TableScanColumnColumnNumeric,
  TableScanColumnValueString,
  TableScanColumnColumnString,
  TableScanLike,
  TableScanFallback
};

/**
 * CostModel that applies
 */
class CostModelLinear : public AbstractCostModel {
 public:
  static CostModelLinear create_debug_build_model();
  static CostModelLinear create_release_build_model();

  /**
   * @return a CostModelLinear calibrated on the current build type (debug, release)
   */
  static CostModelLinear create_current_build_type_model();

  CostModelLinear(const std::map<CostModelLinearSubOperator, CostFeatureWeights>& sub_operator_models);

 protected:
   virtual Cost _cost_model_impl(const OperatorType operator_type, const AbstractCostFeatureProxy& feature_proxy) = 0;

 private:
  std::map<CostModelLinearSubOperator, CostFeatureWeights> _sub_operator_models;
};

}  // namespace opossum
