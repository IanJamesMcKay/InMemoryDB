#pragma once

#include "abstract_cardinality_estimator.hpp"
#include "abstract_cardinality_estimator.hpp"

namespace opossum {

class Optimizer;

/**
 * Determines the actual Cardinality by executing Predicates. Use this to, e.g., establish a "ground truth" for other
 * cardinality estimators
 */
class CardinalityEstimatorExecution : public AbstractCardinalityEstimator {
 public:
  CardinalityEstimatorExecution();

  Cardinality estimate(const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices,
                       const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates) const override;

 private:
  // We need a custom optimizer to produce a somewhat sane LQP, but doesn't use a CardinalityEstimatorExecution so we
  // don't end up with endless recursive calls to CardinalityEstimatorExecution
  std::shared_ptr<Optimizer> _optimizer;
};

}  // namespace opossum
