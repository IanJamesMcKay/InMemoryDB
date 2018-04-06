#pragma once

#include "cost_feature.hpp"

namespace opossum {

/**
 * Interface for wrappers around Operators and LQPNodes to extract CostFeatures independently of the wrapped type
 */
class AbstractCostFeatureProxy {
 public:
  virtual ~AbstractCostFeatureProxy() = default;

  virtual float extract_feature(const CostFeature cost_feature) const = 0;
};

}  // namespace opossum
