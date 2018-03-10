#include "dp_ccp.hpp"

#include <queue>
#include <unordered_map>

#include "abstract_join_plan_node.hpp"
#include "dp_subplan_cache_best.hpp"
#include "enumerate_ccp.hpp"
#include "join_edge.hpp"
#include "join_plan_join_node.hpp"
#include "join_plan_vertex_node.hpp"

#define VERBOSE 0

namespace opossum {

DpCcp::DpCcp() : AbstractDpAlgorithm(std::make_shared<DpSubplanCacheBest>()) {}

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
    const auto plan_left_right = std::make_shared<JoinPlanJoinNode>(best_plan_left, best_plan_right, predicates);
    const auto plan_right_left = std::make_shared<JoinPlanJoinNode>(best_plan_right, best_plan_left, predicates);
    const auto current_plan =
        plan_left_right->plan_cost() < plan_right_left->plan_cost() ? plan_left_right : plan_right_left;
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