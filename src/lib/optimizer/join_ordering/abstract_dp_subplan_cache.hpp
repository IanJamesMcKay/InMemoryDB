#pragma once

#include <optional>

#include "boost/dynamic_bitset.hpp"

#include "join_plan_node.hpp"

namespace opossum {

class AbstractJoinPlanNode;

class AbstractDpSubplanCache {
 public:
  virtual ~AbstractDpSubplanCache() = default;

  virtual void clear() = 0;

  virtual std::optional<JoinPlanNode> get_best_plan(
      const boost::dynamic_bitset<>& vertex_set) const = 0;
  virtual void cache_plan(const boost::dynamic_bitset<>& vertex_set,
                          const JoinPlanNode& plan) = 0;
};
}