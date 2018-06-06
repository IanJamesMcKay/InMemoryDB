#pragma once

#include <unordered_map>
#include <set>

#include "abstract_cardinality_estimator.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"

namespace opossum {

class AbstractColumnStatistics;

class CardinalityEstimatorColumnStatistics : public AbstractCardinalityEstimator {
 public:
  std::optional<Cardinality> estimate(const std::vector<std::shared_ptr<AbstractLQPNode>>& relations,
                       const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates) const override;

  void set_penalty(const float penalty);

 private:
  struct EstimationState final {
    std::unordered_map<LQPColumnReference, std::shared_ptr<const AbstractColumnStatistics>> column_statistics;
    std::unordered_map<LQPColumnReference, std::shared_ptr<AbstractLQPNode>> vertices;
    std::set<std::shared_ptr<AbstractLQPNode>> vertices_not_joined;
    Cardinality current_cardinality{0};
  };

  static void _init_estimation_state(const AbstractJoinPlanPredicate& predicate, const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices, EstimationState& estimation_state);
  static void _add_column_reference(const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices, const LQPColumnReference& column_reference, EstimationState& estimation_state);
  static void _apply_predicate(const AbstractJoinPlanPredicate& predicate, EstimationState& estimation_state);
  static void _ensure_vertex_is_joined(EstimationState& estimation_state, const LQPColumnReference& column_reference);

  float _penalty{1.0f};
};

}  // namespace opossum