#include "dp_subplan_cache_top_k.hpp"

#include "utils/assert.hpp"

namespace opossum {

DpSubplanCacheTopK::DpSubplanCacheTopK(const size_t max_entry_count_per_set)
    : _max_entry_count_per_set(max_entry_count_per_set) {}

const DpSubplanCacheTopK::JoinPlanSet& DpSubplanCacheTopK::get_best_plans(
    const boost::dynamic_bitset<>& vertex_set) const {
  return _plans_by_vertex_set[vertex_set];
}

std::optional<JoinPlan> DpSubplanCacheTopK::get_best_plan(
    const boost::dynamic_bitset<>& vertex_set) const {
  const auto plans = get_best_plans(vertex_set);
  if (plans.empty()) return std::nullopt;
  return *plans.begin();
}

void DpSubplanCacheTopK::clear() { _plans_by_vertex_set.clear(); }

void DpSubplanCacheTopK::cache_plan(const boost::dynamic_bitset<>& vertex_set,
                                    const JoinPlan& plan) {
  auto& plans = _plans_by_vertex_set[vertex_set];
  plans.insert(plan);
  if (plans.size() > _max_entry_count_per_set) {
    DebugAssert(plans.size() == _max_entry_count_per_set + 1, "Invalid number of plans in cache");
    plans.erase(std::prev(plans.end()));
  }
}

bool DpSubplanCacheTopK::JoinPlanCostCompare::operator()(const JoinPlan& lhs,
                                                         const JoinPlan& rhs) const {
  if (lhs.plan_cost == rhs.plan_cost) return lhs.lqp.get() < rhs.lqp.get();

  return lhs.plan_cost < rhs.plan_cost;
}

}  // namespace opossum
