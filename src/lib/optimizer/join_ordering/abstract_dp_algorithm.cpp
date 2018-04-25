#include "abstract_dp_algorithm.hpp"

#include "abstract_dp_subplan_cache.hpp"
#include "build_join_plan.hpp"
#include "join_plan_vertex_node.hpp"
#include "utils/assert.hpp"

namespace opossum {

AbstractDpAlgorithm::AbstractDpAlgorithm(const std::shared_ptr<AbstractDpSubplanCache>& subplan_cache,
                                         const std::shared_ptr<const AbstractCostModel>& cost_model,
                                         const std::shared_ptr<AbstractCardinalityEstimator>& cardinality_estimator)
    : _subplan_cache(subplan_cache), _cost_model(cost_model), _cardinality_estimator(cardinality_estimator) {}

JoinPlanNode AbstractDpAlgorithm::operator()(
    const std::shared_ptr<const JoinGraph>& join_graph) {
  _subplan_cache->clear();
  _join_graph = join_graph;

  /**
   * Initialize single-vertex vertex_sets with single node JoinPlans
   */
  for (size_t vertex_idx = 0; vertex_idx < _join_graph->vertices.size(); ++vertex_idx) {
    boost::dynamic_bitset<> vertex_bit{_join_graph->vertices.size()};
    vertex_bit.set(vertex_idx);

    const auto vertex_predicates = _join_graph->find_predicates(vertex_bit);

    const auto vertex = _join_graph->vertices[vertex_idx];

    _subplan_cache->cache_plan(
        vertex_bit, build_join_plan_vertex_node(*_cost_model, vertex, vertex_predicates, *_cardinality_estimator));

//    SubJoinGraph sub_join_graph;
//    sub_join_graph.vertices = {vertex};
//    sub_join_graph.predicates = vertex_predicates;

//    _sub_join_graphs.emplace(vertex_bit, sub_join_graph);
  }

  _on_execute();

  /**
   * Build vertex set with all vertices set and return the plan for it
   */
  boost::dynamic_bitset<> all_vertices_set{_join_graph->vertices.size()};
  all_vertices_set.flip();  // Turns all bits to '1'

  const auto best_plan = _subplan_cache->get_best_plan(all_vertices_set);
  Assert(best_plan, "No plan for all vertices generated. Maybe JoinGraph isn't connected?");

  return *best_plan;
}

}  // namespace opossum
