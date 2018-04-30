#include "base_join_graph.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"
#include "utils/assert.hpp"

namespace opossum {

BaseJoinGraph BaseJoinGraph::from_joined_graphs(const BaseJoinGraph& left, const BaseJoinGraph& right) {
  BaseJoinGraph joined_graph;
  joined_graph.vertices.insert(joined_graph.vertices.end(), left.vertices.begin(), left.vertices.end());
  joined_graph.vertices.insert(joined_graph.vertices.end(), right.vertices.begin(), right.vertices.end());
  joined_graph.predicates.insert(joined_graph.predicates.end(), left.predicates.begin(), left.predicates.end());
  joined_graph.predicates.insert(joined_graph.predicates.end(), right.predicates.begin(), right.predicates.end());
  return joined_graph;
}

BaseJoinGraph::BaseJoinGraph(const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices, const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates):
  vertices(vertices), predicates(predicates) {}

std::shared_ptr<AbstractLQPNode> BaseJoinGraph::find_vertex(const LQPColumnReference& column_reference) const {
  for (const auto& vertex : vertices) {
    if (vertex->find_output_column_id(column_reference)) return vertex;
  }
  Fail("Couldn't find vertex");
}

}