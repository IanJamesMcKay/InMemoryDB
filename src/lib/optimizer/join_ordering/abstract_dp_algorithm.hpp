#pragma once

#include <bitset>
#include <map>
#include <memory>
#include <set>

#include "boost/dynamic_bitset.hpp"

#include "abstract_join_ordering_algorithm.hpp"
#include "optimizer/table_statistics_cache.hpp"
#include "join_graph.hpp"
#include "join_plan_node.hpp"

namespace opossum {

class AbstractCardinalityEstimator;
class AbstractCostModel;
class AbstractLQPNode;
class AbstractDpSubplanCache;
class JoinEdge;
class JoinGraph;

class AbstractDpAlgorithm : public AbstractJoinOrderingAlgorithm {
 public:
  explicit AbstractDpAlgorithm(const std::shared_ptr<AbstractDpSubplanCache>& subplan_cache,
                               const std::shared_ptr<const AbstractCostModel>& cost_model,
                               const std::shared_ptr<AbstractCardinalityEstimator>& cardinality_estimator);

  JoinPlanNode operator()(const std::shared_ptr<const JoinGraph>& join_graph) override;

 protected:
  virtual void _on_execute() = 0;

  std::shared_ptr<const JoinGraph> _join_graph;
  const std::shared_ptr<AbstractDpSubplanCache> _subplan_cache;
  const std::shared_ptr<const AbstractCostModel> _cost_model;
  const std::shared_ptr<AbstractCardinalityEstimator> _cardinality_estimator;
};

}  // namespace opossum
