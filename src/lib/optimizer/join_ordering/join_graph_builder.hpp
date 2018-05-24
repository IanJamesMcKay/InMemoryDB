#pragma once

#include <map>
#include <memory>
#include <optional>
#include <set>
#include <vector>

#include "join_graph.hpp"
#include "logical_query_plan/union_node.hpp"

namespace opossum {

class AbstractLQPNode;

class JoinGraphBuilder final {
 public:
  std::shared_ptr<JoinGraph> operator()(const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices,
                                        const std::vector<std::shared_ptr<AbstractJoinPlanPredicate>>& predicates);

  static std::vector<std::shared_ptr<JoinEdge>> join_edges_from_predicates(
      const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices,
      const std::vector<std::shared_ptr<AbstractJoinPlanPredicate>>& predicates);

  static std::vector<std::shared_ptr<JoinEdge>> cross_edges_between_components(
      const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices, std::vector<std::shared_ptr<JoinEdge>> edges);
};
}  // namespace opossum
