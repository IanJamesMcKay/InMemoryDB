#pragma once

#include <bitset>
#include <map>
#include <memory>
#include <set>

#include "boost/dynamic_bitset.hpp"

#include "abstract_join_ordering_algorithm.hpp"
#include "join_graph.hpp"

namespace opossum {

class AbstractLQPNode;
class AbstractDpSubplanCache;
class AbstractJoinPlanNode;
class JoinEdge;
class JoinGraph;

class AbstractDpAlgorithm : public AbstractJoinOrderingAlgorithm {
 public:
  explicit AbstractDpAlgorithm(const std::shared_ptr<AbstractDpSubplanCache>& subplan_cache);

  std::shared_ptr<const AbstractJoinPlanNode> operator()(const std::shared_ptr<const JoinGraph>& join_graph) override;

 protected:
  virtual void _on_execute() = 0;

  std::shared_ptr<const JoinGraph> _join_graph;
  std::shared_ptr<AbstractDpSubplanCache> _subplan_cache;
};

}  // namespace opossum
