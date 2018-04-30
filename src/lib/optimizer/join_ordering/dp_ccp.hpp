#pragma once

#include "abstract_dp_algorithm.hpp"
#include "dp_subplan_cache_best.hpp"
#include "statistics/basic_cardinality_estimator.hpp"

namespace opossum {

class DpCcp : public AbstractDpAlgorithm {
 public:
  explicit DpCcp(const std::shared_ptr<const AbstractCostModel>& cost_model,
                 const std::shared_ptr<AbstractCardinalityEstimator>& cardinality_estimator = std::make_shared<BasicCardinalityEstimator>());

 protected:
  void _on_execute() override;
};

}  // namespace opossum
