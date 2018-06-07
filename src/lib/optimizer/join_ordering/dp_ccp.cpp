#include "dp_ccp.hpp"

#include <queue>
#include <unordered_map>

#include "build_join_plan.hpp"
#include "dp_subplan_cache_best.hpp"
#include "enumerate_ccp.hpp"
#include "join_edge.hpp"
#include "join_plan_join_node.hpp"
#include "join_plan_vertex_node.hpp"

namespace opossum {

DpCcp::DpCcp(const std::shared_ptr<const AbstractCostModel>& cost_model,
             const std::shared_ptr<AbstractCardinalityEstimator>& cardinality_estimator) :
  AbstractDpAlgorithm(std::make_shared<DpSubplanCacheBest>(), cost_model, cardinality_estimator) {}

void DpCcp::_on_execute() {
  /**
   * Build `enumerate_ccp_edges` from all vertex-to-vertex edges for EnumerateCcp,
   */
  std::vector<std::pair<size_t, size_t>> enumerate_ccp_edges;
  for (const auto& edge : _join_graph->edges) {
    if (edge->vertex_set.count() != 2) continue;

    const auto first_vertex_idx = edge->vertex_set.find_first();
    const auto second_vertex_idx = edge->vertex_set.find_next(first_vertex_idx);

    enumerate_ccp_edges.emplace_back(first_vertex_idx, second_vertex_idx);
  }

  /**
   * Actual DpCcp algorithm
   */
  const auto csg_cmp_pairs = EnumerateCcp{_join_graph->vertices.size(), enumerate_ccp_edges}();
  for (const auto& csg_cmp_pair : csg_cmp_pairs) {
    const auto predicates = _join_graph->find_predicates(csg_cmp_pair.first, csg_cmp_pair.second);

    const auto best_plan_left = _subplan_cache->get_best_plan(csg_cmp_pair.first);
    const auto best_plan_right = _subplan_cache->get_best_plan(csg_cmp_pair.second);
    DebugAssert(best_plan_left && best_plan_right, "Subplan missing. Bug in EnumerateCcp likely.");

    auto current_plan = _create_join_plan(*best_plan_left, *best_plan_right, predicates);

    _subplan_cache->cache_plan(csg_cmp_pair.first | csg_cmp_pair.second, current_plan);
  }
}

}  // namespace opossum