#pragma once

#include <map>
#include <memory>
#include <set>

#include "abstract_dp_subplan_cache.hpp"
#include "boost/dynamic_bitset.hpp"

namespace opossum {

class AbstractJoinPlanNode;

class DpSubplanCacheBest : public AbstractDpSubplanCache {
 public:
  void clear() override;

  std::optional<JoinPlan> get_best_plan(const boost::dynamic_bitset<>& vertex_set) const override;
  void cache_plan(const boost::dynamic_bitset<>& vertex_set, const JoinPlan& plan) override;

 private:
  mutable std::map<boost::dynamic_bitset<>, JoinPlan> _plan_by_vertex_set;
};

}  // namespace opossum
