#include "dp_ccp.hpp"

#include <queue>
#include <unordered_map>

#include "abstract_join_plan_node.hpp"
#include "build_join_plan.hpp"
#include "dp_subplan_cache_best.hpp"
#include "enumerate_ccp.hpp"
#include "join_edge.hpp"
#include "join_plan_join_node.hpp"
#include "join_plan_vertex_node.hpp"

#define VERBOSE 0

namespace opossum {

DpCcp::DpCcp(const std::shared_ptr<const AbstractCostModel>& cost_model,
             const std::shared_ptr<const TableStatisticsCache>& statistics_cache) : AbstractDpAlgorithm(std::make_shared<DpSubplanCacheBest>(), cost_model, statistics_cache) {}

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
#if VERBOSE
    std::cout << "Considering plan for " << (csg_cmp_pair.first | csg_cmp_pair.second) << ": " << csg_cmp_pair.first
              << " + " << csg_cmp_pair.second << std::endl;
#endif

    const auto best_plan_left = _subplan_cache->get_best_plan(csg_cmp_pair.first);
    const auto best_plan_right = _subplan_cache->get_best_plan(csg_cmp_pair.second);
    const auto predicates = _join_graph->find_predicates(csg_cmp_pair.first, csg_cmp_pair.second);
    const auto current_plan = build_join_plan_join_node(*_cost_model, best_plan_left, best_plan_right, predicates, *_statistics_cache);
    const auto current_best_plan = _subplan_cache->get_best_plan(csg_cmp_pair.first | csg_cmp_pair.second);

#if VERBOSE
    std::cout << "Cost=" << current_plan->cost() << std::endl;
    current_plan->print();
    std::cout << std::endl;
#endif

    _subplan_cache->cache_plan(csg_cmp_pair.first | csg_cmp_pair.second, current_plan);
  }
}

}  // namespace opossum