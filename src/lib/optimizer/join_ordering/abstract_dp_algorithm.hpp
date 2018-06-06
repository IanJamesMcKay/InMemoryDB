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

class AbstractDpAlgorithm : public AbstractJoinOrderingAlgorithm {
 public:
  explicit AbstractDpAlgorithm(const std::shared_ptr<AbstractDpSubplanCache>& subplan_cache,
                               const std::shared_ptr<AbstractCostModel>& cost_model,
                               const std::shared_ptr<AbstractCardinalityEstimator>& cardinality_estimator);

  std::shared_ptr<PlanBlock> operator()(const std::shared_ptr<PredicateJoinBlock>& input_block) final;

 protected:
  virtual void _on_execute() = 0;

  std::shared_ptr<PredicateJoinBlock> _input_block;

  const std::shared_ptr<AbstractDpSubplanCache> _subplan_cache;
  const std::shared_ptr<AbstractCostModel> _cost_model;
  const std::shared_ptr<AbstractCardinalityEstimator> _cardinality_estimator;
};

}  // namespace opossum
