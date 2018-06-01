#pragma once

#include <memory>
#include <vector>

namespace opossum {

class AbstractQueryBlock;
class AbstractLQPNode;

using Cardinality = float;

/**
 * Interface for an algorithm that returns a Cardinality Estimation for a given Query
 */
class AbstractCardinalityEstimator {
 public:
  virtual ~AbstractCardinalityEstimator() = default;

  /**
   * Estimate the cardinality of an LQP.
   * Optionally the root of a logically equivalent QueryBlock structure can be passed in to facilitate Cache lookups.
   */
  virtual Cardinality estimate(const std::shared_ptr<AbstractLQPNode>& lqp,
                               const std::shared_ptr<AbstractQueryBlock>& query_blocks_root = {}) const = 0;
};

}  // namespace opossum
