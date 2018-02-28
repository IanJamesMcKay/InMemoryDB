#include "dp_subplan_cache_top_k.hpp"

#include "abstract_join_plan_node.hpp"

#include "utils/assert.hpp"

namespace opossum {

DpSubplanCacheTopK::DpSubplanCacheTopK(const size_t max_entry_count_per_set): _max_entry_count_per_set(max_entry_count_per_set) {

}

const DpSubplanCacheTopK::JoinPlanSet& DpSubplanCacheTopK::get_best_plans(
const boost::dynamic_bitset<> &vertex_set) const {
  return _plans_by_vertex_set[vertex_set];
}

std::shared_ptr<const AbstractJoinPlanNode> DpSubplanCacheTopK::get_best_plan(const boost::dynamic_bitset<> &vertex_set) const {
  const auto plans = get_best_plans(vertex_set);
  return plans.empty() ? nullptr : *plans.begin();
}

void DpSubplanCacheTopK::cache_plan(const boost::dynamic_bitset<>& vertex_set, const std::shared_ptr<const AbstractJoinPlanNode>& plan) {
  auto& plans = _plans_by_vertex_set[vertex_set];
  plans.insert(plan);
  if (plans.size() > _max_entry_count_per_set) {
    DebugAssert(plans.size() == _max_entry_count_per_set + 1, "Invalid number of plans in cache");
    plans.erase(std::prev(plans.end()));
  }
}

bool DpSubplanCacheTopK::JoinPlanCostCompare::operator()(const std::shared_ptr<const AbstractJoinPlanNode> &lhs, const std::shared_ptr<const AbstractJoinPlanNode>& rhs) const {
  if (lhs->plan_cost() == rhs->plan_cost()) return lhs.get() < rhs.get();

  return lhs->plan_cost() < rhs->plan_cost();
}

}  // namespace opossum
