#pragma once

#include <memory>
#include <vector>

namespace opossum {

class AbstractJoinPlanPredicate;
class AbstractLQPNode;

using Cardinality = float;

/**
 * Interface for an algorithm that returns a Cardinality Estimation for a given Query
 */
class AbstractCardinalityEstimator {
 public:
  virtual ~AbstractCardinalityEstimator() = default;

  virtual Cardinality estimate(const std::vector<std::shared_ptr<AbstractLQPNode>>& relations,
                               const std::vector<std::shared_ptr<AbstractJoinPlanPredicate>>& predicates) const = 0;
};

}  // namespace opossum
