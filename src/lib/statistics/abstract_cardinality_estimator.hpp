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

  /**
   * @param relations   LQPs representing the relations connected by the predicates. Using LQP nodes instead of, e.g.,
   *                    Tables, because a) the LQP can encapsulate Outer Joins not representable using normal predicates
   *                    and b) the predicates are expressions that reference Columns in LQPs, and not in raw Tables.
   *
   * @param predicates  Expressions of ExpressionType Column, Predicate and Logical.
   *
   * @return            The estimated Cardinality of the evaluating `predicates` on `relations`
   */
  virtual Cardinality estimate(const std::vector<std::shared_ptr<AbstractLQPNode>>& relations,
                               const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates) const = 0;
};

}  // namespace opossum
