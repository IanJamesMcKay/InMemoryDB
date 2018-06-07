#pragma once

#include <memory>

#include "abstract_cost_feature_proxy.hpp"

namespace opossum {

class AbstractCardinalityEstimator;
class AbstractLQPNode;

class CostFeatureLQPNodeProxy : public AbstractCostFeatureProxy {
 public:
  CostFeatureLQPNodeProxy(const std::shared_ptr<AbstractLQPNode>& node,
                                   const std::shared_ptr<AbstractCardinalityEstimator>& cardinality_estimator);

 protected:
  CostFeatureVariant _extract_feature_impl(const CostFeature cost_feature) const override;

 private:
  std::shared_ptr<AbstractLQPNode> _node;
  std::shared_ptr<AbstractCardinalityEstimator> _cardinality_estimator;
};

}  // namespace opossum
