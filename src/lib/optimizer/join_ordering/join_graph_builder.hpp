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

/**
 * Turns an LQP into a JoinGraph.
 *
 * Consider this simple LQP:
 * [0] [Projection] x1, y1
 *  \_[1] [Predicate] x2 <= y1
 *     \_[2] [Cross Join]
 *        \_[3] [MockTable] x
 *        \_[4] [MockTable] y
 *
 * The JoinGraph created from it would contain two vertices (x and y), one edge (between x and y, with the predicate
 * x2 <= y1) and one output_relation (Projection, left input side)
 */
class JoinGraphBuilder final {
 public:
  /**
   * From the subtree of root, build a JoinGraph.
   * The LQP is not modified during this process.
   *
   * Need an instance of the shared_ptr to keep the ref count > 0
   */
  std::shared_ptr<JoinGraph> operator()(const std::shared_ptr<AbstractLQPNode>& lqp);

  static std::vector<std::shared_ptr<JoinEdge>> join_edges_from_predicates(
      const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices,
      const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates);

  static std::vector<std::shared_ptr<JoinEdge>> cross_edges_between_components(
      const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices, std::vector<std::shared_ptr<JoinEdge>> edges);

  const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices() const;
  const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates() const;


 private:
  /**
   * Returns whether a node of the given type is a JoinGraph vertex in all cases. This is true for all node types that
   * aren't Predicates, Joins or Unions.
   */
  bool _lqp_node_type_is_vertex(const LQPNodeType node_type) const;

  std::vector<std::shared_ptr<AbstractLQPNode>> _vertices;
  std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> _predicates;
};
}  // namespace opossum
