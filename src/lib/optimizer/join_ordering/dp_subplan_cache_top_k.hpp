#pragma once

#include <map>
#include <memory>
#include <set>

#include "abstract_dp_subplan_cache.hpp"
#include "boost/dynamic_bitset.hpp"

namespace opossum {

class AbstractJoinPlanNode;

class DpSubplanCacheTopK : public AbstractDpSubplanCache {
 public:
  static constexpr auto NO_ENTRY_LIMIT = std::numeric_limits<size_t>::max();

  struct JoinPlanCostCompare {
    bool operator()(const JoinPlanNode& lhs,
                    const JoinPlanNode& rhs) const;
  };

  using JoinPlanSet = std::set<JoinPlanNode, JoinPlanCostCompare>;

  explicit DpSubplanCacheTopK(const size_t max_entry_count_per_set);

  const JoinPlanSet& get_best_plans(const boost::dynamic_bitset<>& vertex_set) const;

  void clear() override;

  std::optional<JoinPlanNode> get_best_plan(const boost::dynamic_bitset<>& vertex_set) const override;
  void cache_plan(const boost::dynamic_bitset<>& vertex_set,
                  const JoinPlanNode& plan) override;

 private:
  const size_t _max_entry_count_per_set;

  mutable std::map<boost::dynamic_bitset<>, JoinPlanSet> _plans_by_vertex_set;
};

}  // namespace opossum
