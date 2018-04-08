#pragma once

#include <map>
#include <vector>

#include "abstract_cost_model.hpp"
#include "cost_feature.hpp"

namespace opossum {

enum class CostModelLinearTableScanType {
  ColumnValueNumeric,
  ColumnColumnNumeric,
  ColumnValueString,
  ColumnColumnString,
  Like
};

struct CostModelLinearConfig final {
  std::map<CostModelLinearTableScanType, CostFeatureWeights> table_scan_models;
  std::map<OperatorType, CostFeatureWeights> other_operator_models;
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

  explicit CostModelLinear(const CostModelLinearConfig& config);

  Cost get_reference_operator_cost(const std::shared_ptr<AbstractOperator>& op) const override;

 protected:
   virtual Cost _cost_model_impl(const OperatorType operator_type, const AbstractCostFeatureProxy& feature_proxy) const override;
   Cost _predict_cost(const CostFeatureWeights& feature_weights, const AbstractCostFeatureProxy& feature_proxy) const;

 private:
  CostModelLinearConfig _config;
};

}  // namespace opossum
