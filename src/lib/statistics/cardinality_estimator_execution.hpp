#pragma once

#include <chrono>
#include <unordered_set>
#include <optional>

#include "optimizer/optimizer.hpp"
#include "abstract_cardinality_estimator.hpp"

namespace opossum {

class Optimizer;

/**
 * Determines the actual Cardinality by executing Predicates. Use this to, e.g., establish a "ground truth" for other
 * cardinality estimators
 */
class CardinalityEstimatorExecution : public AbstractCardinalityEstimator {
 public:
  explicit CardinalityEstimatorExecution(const std::shared_ptr<Optimizer>& optimizer = std::make_shared<Optimizer>());

  std::optional<Cardinality> estimate(const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices,
                       const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates) const override;

  std::optional<std::chrono::seconds> timeout;

 private:
  // We need a custom optimizer to produce a somewhat sane LQP, but doesn't use a CardinalityEstimatorExecution so we
  // don't end up with endless recursive calls to CardinalityEstimatorExecution
  std::shared_ptr<Optimizer> _optimizer;

  mutable std::unordered_set<size_t> _blacklist;
};

}  // namespace opossum
