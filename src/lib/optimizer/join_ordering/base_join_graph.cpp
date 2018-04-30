#include "base_join_graph.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::shared_ptr<AbstractLQPNode> BaseJoinGraph::find_vertex(const LQPColumnReference& column_reference) {
  for (const auto& vertex : vertices) {
    if (vertex->find_output_column_id(column_reference)) return vertex;
  }
  Fail("Couldn't find vertex");
}

}