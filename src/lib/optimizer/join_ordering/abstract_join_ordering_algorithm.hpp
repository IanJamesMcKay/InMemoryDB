#pragma once

#include <memory>

namespace opossum {

class AbstractJoinPlanNode;
class JoinGraph;

/**
 * Abstract base class for algorithms performing JoinOrdering.
 */
class AbstractJoinOrderingAlgorithm {
 public:
  virtual ~AbstractJoinOrderingAlgorithm() = default;

  virtual std::shared_ptr<const AbstractJoinPlanNode> operator()(const std::shared_ptr<const JoinGraph>& join_graph) = 0;
};

}  // namespace opossum
