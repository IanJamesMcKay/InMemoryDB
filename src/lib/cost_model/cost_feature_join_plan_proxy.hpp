#pragma once

#include <memory>

#include "statistics/abstract_cardinality_estimator.hpp"
#include "optimizer/join_ordering/join_plan_predicate.hpp"
#include "abstract_cost_feature_proxy.hpp"

namespace opossum {

class AbstractCardinalityEstimator;
class AbstractLQPNode;
class BaseJoinGraph;
enum class OperatorType;

class CostFeatureJoinPlanProxy : public AbstractCostFeatureProxy {
 public:
  explicit CostFeatureJoinPlanProxy(const OperatorType operator_type,
                                    const std::shared_ptr<AbstractCardinalityEstimator>& cardinality_estimator,
                                    const std::shared_ptr<AbstractJoinPlanPredicate>& predicate,
                                    const std::shared_ptr<BaseJoinGraph>& left_input_graph,
                                    const std::shared_ptr<BaseJoinGraph>& right_input_graph);

 protected:
  CostFeatureVariant _extract_feature_impl(const CostFeature cost_feature) const override;

 private:
  const OperatorType _operator_type;
  const std::shared_ptr<AbstractCardinalityEstimator> _cardinality_estimator;
  const std::shared_ptr<AbstractJoinPlanPredicate> _predicate;
  const std::shared_ptr<BaseJoinGraph> _left_input_graph;
  const std::shared_ptr<BaseJoinGraph> _right_input_graph;
};

}  // namespace opossum
