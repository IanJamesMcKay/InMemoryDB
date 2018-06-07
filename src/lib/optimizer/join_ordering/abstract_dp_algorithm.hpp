#pragma once

#include <bitset>
#include <map>
#include <memory>
#include <set>

#include "boost/dynamic_bitset.hpp"

#include "abstract_join_ordering_algorithm.hpp"
#include "optimizer/table_statistics_cache.hpp"
#include "join_graph.hpp"
#include "join_plan.hpp"

namespace opossum {

class AbstractCardinalityEstimator;
class AbstractQueryBlock;
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

  JoinPlan _create_join_plan_leaf(const std::shared_ptr<AbstractQueryBlock> &vertex_block,
                                  const std::vector<std::shared_ptr<AbstractJoinPlanPredicate>> &predicates) const;
  JoinPlan _create_join_plan(const JoinPlan& left_input, const JoinPlan& right_input, const std::vector<std::shared_ptr<AbstractJoinPlanPredicate>>& predicates) const;
  void _add_join_plan_predicate(JoinPlan& join_plan, const std::shared_ptr<AbstractJoinPlanPredicate>& predicate) const;

  std::shared_ptr<PredicateJoinBlock> _input_block;
  std::shared_ptr<JoinGraph> _join_graph;

  const std::shared_ptr<AbstractDpSubplanCache> _subplan_cache;
  const std::shared_ptr<AbstractCostModel> _cost_model;
  const std::shared_ptr<AbstractCardinalityEstimator> _cardinality_estimator;
};

}  // namespace opossum
