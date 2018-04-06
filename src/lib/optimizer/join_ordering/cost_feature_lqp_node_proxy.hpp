#pragma once

#include "abstract_cost_feature_proxy.hpp"

namespace opossum {

class CostFeatureLQPNodeProxy : public AbstractCostFeatureProxy {
 public:
  CostFeatureLQPNodeProxy(const std::shared_ptr<AbstractLPQNode>& node);

  float extract_feature(const CostFeature cost_feature) const override;
};

}  // namespace opossum
