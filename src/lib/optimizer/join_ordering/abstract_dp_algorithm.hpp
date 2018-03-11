#pragma once

#include <bitset>
#include <map>
#include <memory>
#include <set>

#include "boost/dynamic_bitset.hpp"

#include "abstract_join_ordering_algorithm.hpp"
#include "join_graph.hpp"

namespace opossum {

class AbstractCostModel;
class AbstractLQPNode;
class AbstractDpSubplanCache;
class AbstractJoinPlanNode;
class JoinEdge;
class JoinGraph;

class AbstractDpAlgorithm : public AbstractJoinOrderingAlgorithm {
 public:
  explicit AbstractDpAlgorithm(const std::shared_ptr<AbstractDpSubplanCache>& subplan_cache, const std::shared_ptr<const AbstractCostModel>& cost_model);

  std::shared_ptr<const AbstractJoinPlanNode> operator()(const std::shared_ptr<const JoinGraph>& join_graph) override;

 protected:
  virtual void _on_execute() = 0;

  std::shared_ptr<const JoinGraph> _join_graph;
  const std::shared_ptr<AbstractDpSubplanCache> _subplan_cache;
  const std::shared_ptr<const AbstractCostModel> _cost_model;
};

}  // namespace opossum
