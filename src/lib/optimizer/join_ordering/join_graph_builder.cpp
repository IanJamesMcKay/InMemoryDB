#include "join_graph_builder.hpp"

#include <numeric>
#include <stack>
#include <queue>

#include "join_edge.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::shared_ptr<JoinGraph> JoinGraphBuilder::operator()(const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices,
                                                        const std::vector<std::shared_ptr<AbstractJoinPlanPredicate>>& predicates) {

  auto edges = join_edges_from_predicates(vertices, predicates);
  auto cross_edges = cross_edges_between_components(vertices, edges);

  edges.insert(edges.end(), cross_edges.begin(), cross_edges.end());

  return std::make_shared<JoinGraph>(std::move(vertices), std::move(edges));
}

std::vector<std::shared_ptr<JoinEdge>> JoinGraphBuilder::join_edges_from_predicates(
    const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices,
    const std::vector<std::shared_ptr<AbstractJoinPlanPredicate>>& predicates) {
  std::unordered_map<std::shared_ptr<AbstractLQPNode>, size_t> vertex_to_index;
  std::map<JoinVertexSet, std::shared_ptr<JoinEdge>> vertices_to_edge;
  std::vector<std::shared_ptr<JoinEdge>> edges;

  for (size_t vertex_idx = 0; vertex_idx < vertices.size(); ++vertex_idx) {
    vertex_to_index[vertices[vertex_idx]] = vertex_idx;
  }

  for (const auto& predicate : predicates) {
    const auto vertex_set = predicate->get_accessed_vertex_set(vertices);
    auto iter = vertices_to_edge.find(vertex_set);
    if (iter == vertices_to_edge.end()) {
      auto edge = std::make_shared<JoinEdge>(vertex_set);
      iter = vertices_to_edge.emplace(vertex_set, edge).first;
      edges.emplace_back(edge);
    }
    iter->second->predicates.emplace_back(predicate);
  }

  return edges;
}

std::vector<std::shared_ptr<JoinEdge>> JoinGraphBuilder::cross_edges_between_components(
    const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices, std::vector<std::shared_ptr<JoinEdge>> edges) {
  /**
   * Create edges from the gathered JoinPlanPredicates. We can't directly create the JoinGraph from this since we want
   * the JoinGraph to be connected and there might be edges from CrossJoins still missing.
   * To make the JoinGraph connected, we identify all Components and connect them to a Chain.
   *
   * So this
   *   B
   *  / \    D--E   F
   * A---C
   *
   * becomes
   *   B
   *  / \
   * A---C
   * |
   * D--E
   * |
   * F
   *
   * where the edges AD and DF are being created and have no predicates. There is of course the theoretical chance that
   * different edges, say CD and EF would result in a better plan. We ignore this possibility for now.
   */

  std::unordered_set<size_t> remaining_vertex_indices;
  for (auto vertex_idx = size_t{0}; vertex_idx < vertices.size(); ++vertex_idx)
    remaining_vertex_indices.insert(vertex_idx);

  std::vector<size_t> one_vertex_per_component;

  while (!remaining_vertex_indices.empty()) {
    const auto vertex_idx = *remaining_vertex_indices.begin();

    one_vertex_per_component.emplace_back(vertex_idx);

    std::stack<size_t> bfs_stack;
    bfs_stack.push(vertex_idx);

    while (!bfs_stack.empty()) {
      const auto vertex_idx2 = bfs_stack.top();
      bfs_stack.pop();

      remaining_vertex_indices.erase(vertex_idx2);

      for (auto iter = edges.begin(); iter != edges.end();) {
        const auto& edge = *iter;
        if (!edge->vertex_set.test(vertex_idx2)) {
          ++iter;
          continue;
        }

        auto connected_vertex_idx = edge->vertex_set.find_first();
        while (connected_vertex_idx != JoinVertexSet::npos) {
          if (connected_vertex_idx != vertex_idx2) bfs_stack.push(connected_vertex_idx);
          connected_vertex_idx = edge->vertex_set.find_next(connected_vertex_idx);
        }

        iter = edges.erase(iter);
      }
    }
  }

  if (one_vertex_per_component.size() < 2) return {};

  std::vector<std::shared_ptr<JoinEdge>> inter_component_edges;
  inter_component_edges.reserve(one_vertex_per_component.size() - 1);

  for (auto component_idx = size_t{1}; component_idx < one_vertex_per_component.size(); ++component_idx) {
    JoinVertexSet vertex_set{vertices.size()};
    vertex_set.set(one_vertex_per_component[component_idx - 1]);
    vertex_set.set(one_vertex_per_component[component_idx]);

    inter_component_edges.emplace_back(std::make_shared<JoinEdge>(vertex_set));
  }

  return inter_component_edges;
}

}  // namespace opossum
