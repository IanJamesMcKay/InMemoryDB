#pragma once

#include <memory>

#include "join_plan_node.hpp"

namespace opossum {

class AbstractJoinPlanNode;
class JoinGraph;

/**
 * Abstract base class for algorithms performing JoinOrdering.
 */
class AbstractJoinOrderingAlgorithm {
 public:
  virtual ~AbstractJoinOrderingAlgorithm() = default;

  virtual JoinPlanNode operator()(
      const std::shared_ptr<const JoinGraph>& join_graph) = 0;
};

}  // namespace opossum
