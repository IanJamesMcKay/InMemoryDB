#pragma once

#include <memory>

#include "statistics/abstract_cardinality_estimator.hpp"
#include "optimizer/join_ordering/join_plan_predicate.hpp"
#include "abstract_cost_feature_proxy.hpp"

namespace opossum {

class JoinPlanAtomicPredicate;
class AbstractCardinalityEstimator;
class AbstractLQPNode;
class BaseJoinGraph;
enum class OperatorType;

struct GenericInputCostFeatures final {
  static GenericInputCostFeatures from_join_graph(const BaseJoinGraph &input_join_graph, const std::shared_ptr<AbstractCardinalityEstimator> &cardinality_estimator);

  GenericInputCostFeatures() = default;
  GenericInputCostFeatures(const float row_count, const bool is_references);

  float row_count{0};
  bool is_references{false};  // or data
};

struct GenericPredicateCostFeatures final {
  static GenericPredicateCostFeatures from_join_plan_predicate(const std::shared_ptr<const JoinPlanAtomicPredicate> &predicate,
                                                               const BaseJoinGraph &input_join_graph,
                                                               const std::shared_ptr<AbstractCardinalityEstimator> &cardinality_estimator);

  GenericPredicateCostFeatures() = default;
  GenericPredicateCostFeatures(const DataType left_data_type,
                               const DataType right_data_type,
                               const bool right_is_column,
                               const PredicateCondition predicate_condition);

  DataType left_data_type{DataType::Null};
  DataType right_data_type{DataType::Null};
  bool right_is_column{false};
  PredicateCondition predicate_condition{PredicateCondition::Equals};
};

class CostFeatureGenericProxy : public AbstractCostFeatureProxy {
 public:
  static CostFeatureGenericProxy from_join_plan_predicate(const std::shared_ptr<const JoinPlanAtomicPredicate> &predicate,
                                                          const BaseJoinGraph &input_join_graph,
                                                          const std::shared_ptr<AbstractCardinalityEstimator> &cardinality_estimator);

  static CostFeatureGenericProxy from_join_plan_predicate(const std::shared_ptr<const JoinPlanAtomicPredicate> &predicate,
                                                          const BaseJoinGraph &left_input_join_graph,
                                                          const BaseJoinGraph &right_input_join_graph,
                                                          const std::shared_ptr<AbstractCardinalityEstimator> &cardinality_estimator);

  static CostFeatureGenericProxy from_cross_join(const BaseJoinGraph &left_input_join_graph,
                                                 const BaseJoinGraph &right_input_join_graph,
                                                 const std::shared_ptr<AbstractCardinalityEstimator> &cardinality_estimator);

  CostFeatureGenericProxy(
    const OperatorType operator_type,
    const GenericInputCostFeatures& left_input_features,
    const std::optional<GenericInputCostFeatures>& right_input_features,
    const std::optional<GenericPredicateCostFeatures>& predicate_features,
    const float output_row_count);

 protected:
  CostFeatureVariant _extract_feature_impl(const CostFeature cost_feature) const override;
  
 private:
  const OperatorType _operator_type;
  const GenericInputCostFeatures _left_input_features;
  const std::optional<GenericInputCostFeatures> _right_input_features;
  const std::optional<GenericPredicateCostFeatures> _predicate_features;
  const float _output_row_count;
};

}  // namespace opossum
