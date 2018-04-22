#pragma once

#include "abstract_cardinality_estimator.hpp"

namespace opossum {

class BasicCardinalityEstimator : public AbstractCardinalityEstimator {
 public:
  Cardinality estimate(const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices,
                       const std::vector<std::shared_ptr<AbstractExpression2>>& predicates) override;
};

}  // namespace opossum