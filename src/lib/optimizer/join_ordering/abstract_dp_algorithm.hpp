#pragma once

#include <bitset>
#include <map>
#include <memory>
#include <set>

#include "boost/dynamic_bitset.hpp"

#include "join_graph.hpp"

namespace opossum {

class AbstractLQPNode;
class AbstractDpSubplanCache;
class AbstractJoinPlanNode;
class JoinEdge;
class JoinGraph;

class AbstractDpAlgorithm {
 public:
  explicit AbstractDpAlgorithm(const std::shared_ptr<const JoinGraph>& join_graph, const std::shared_ptr<AbstractDpSubplanCache>& subplan_cache);
  virtual ~AbstractDpAlgorithm() = default;

  std::shared_ptr<const AbstractJoinPlanNode> operator()();

 protected:
  virtual void _on_execute() = 0;

  std::shared_ptr<const JoinGraph> _join_graph;
  std::shared_ptr<AbstractDpSubplanCache> _subplan_cache;
};

}  // namespace opossum
