#pragma once

#include "boost/dynamic_bitset.hpp"

namespace opossum {

class AbstractJoinPlanNode;

class AbstractDpSubplanCache {
 public:
  virtual ~AbstractDpSubplanCache() = default;

  virtual void clear() = 0;

  virtual std::shared_ptr<const AbstractJoinPlanNode> get_best_plan(const boost::dynamic_bitset<> &vertex_set) const = 0;
  virtual void cache_plan(const boost::dynamic_bitset<> &vertex_set, const std::shared_ptr<const AbstractJoinPlanNode> &plan) = 0;
};

}