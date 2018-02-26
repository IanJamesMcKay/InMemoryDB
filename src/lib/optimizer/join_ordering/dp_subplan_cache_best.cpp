#include "dp_subplan_cache_best.hpp"

#include "abstract_join_plan_node.hpp"

namespace opossum {

std::shared_ptr<const AbstractJoinPlanNode> DpSubplanCacheBest::get_best_plan(
const boost::dynamic_bitset<> &vertex_set) const {
  const auto iter = _plan_by_vertex_set.find(vertex_set);
  return iter == _plan_by_vertex_set.end() ? nullptr : iter->second;
}

void DpSubplanCacheBest::cache_plan(const boost::dynamic_bitset<> &vertex_set,
                                       const std::shared_ptr<const AbstractJoinPlanNode> &plan) {
  const auto iter = _plan_by_vertex_set.find(vertex_set);

  if (iter == _plan_by_vertex_set.end() || iter->second->plan_cost()) {
    _plan_by_vertex_set.insert_or_assign(iter, vertex_set, plan);
  }
}

}  // namespace opossum
