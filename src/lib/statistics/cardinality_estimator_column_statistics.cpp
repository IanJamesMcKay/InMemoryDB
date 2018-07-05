#include "cardinality_estimator_column_statistics.hpp"

#include <algorithm>
#include <unordered_set>
#include <optimizer/join_ordering/join_plan_predicate.hpp>

#include "optimizer/join_ordering/join_plan_predicate.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/logical_expression.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "utils/assert.hpp"
#include "statistics/column_statistics.hpp"
#include "statistics/table_statistics.hpp"

namespace opossum {

std::optional<Cardinality> CardinalityEstimatorColumnStatistics::estimate(const std::vector<std::shared_ptr<AbstractLQPNode>>& relations,
                                                                          const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates) const {
  Assert(!relations.empty(), "Can't perform estimation on empty set of statistics");

  /**
   * Initialise EstimationState
   */
  EstimationState estimation_state;

  for (const auto& predicate : predicates) {
    _init_estimation_state(*predicate, relations, estimation_state);
  }

  estimation_state.vertices_not_joined.insert(relations.begin() + 1, relations.end());
  estimation_state.current_cardinality = relations.front()->get_statistics()->row_count();

  while (!estimation_state.vertices_not_joined.empty()) {
    
  }

  return estimation_state.current_cardinality * _penalty;
}

void CardinalityEstimatorColumnStatistics::set_penalty(const float penalty) {
  _penalty = penalty;
}

void CardinalityEstimatorColumnStatistics::_init_estimation_state(const AbstractJoinPlanPredicate& predicate, const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices, EstimationState& estimation_state) {
  if (predicate.type() == JoinPlanPredicateType::Atomic)  {
    const auto& atomic_predicate = static_cast<const JoinPlanAtomicPredicate&>(predicate);

    _add_column_reference(vertices, atomic_predicate.left_operand, estimation_state);

    if (is_lqp_column_reference(atomic_predicate.right_operand)) {
      const auto right_column_reference = boost::get<LQPColumnReference>(atomic_predicate.right_operand);
      _add_column_reference(vertices, boost::get<LQPColumnReference>(atomic_predicate.right_operand), estimation_state);
    }
  } else {
    const auto& logical_predicate = static_cast<const JoinPlanLogicalPredicate&>(predicate);
    _init_estimation_state(*logical_predicate.left_operand, vertices, estimation_state);
    _init_estimation_state(*logical_predicate.right_operand, vertices, estimation_state);
  }
}

void CardinalityEstimatorColumnStatistics::_add_column_reference(const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices, const LQPColumnReference& column_reference, EstimationState& estimation_state) {
  for (const auto& vertex : vertices) {
    const auto column_id = vertex->find_output_column_id(column_reference);
    if (column_id) {
      estimation_state.column_statistics.emplace(column_reference,
                                                 vertex->get_statistics()->column_statistics()[*column_id]);
      estimation_state.vertices.emplace(column_reference, vertex);
      return;
    }
  }
  Fail("No vertex outputs this column");
}

void CardinalityEstimatorColumnStatistics::_apply_predicate(const AbstractJoinPlanPredicate& predicate,
                                                 EstimationState& estimation_state) {
  if (predicate.type() == JoinPlanPredicateType::Atomic)  {
    const auto& atomic_predicate = static_cast<const JoinPlanAtomicPredicate&>(predicate);

    _ensure_vertex_is_joined(estimation_state, atomic_predicate.left_operand);

    if (is_lqp_column_reference(atomic_predicate.right_operand)) {
      _ensure_vertex_is_joined(estimation_state, boost::get<LQPColumnReference>(atomic_predicate.right_operand));
    }

    auto& left_column_statistics = estimation_state.column_statistics.find(atomic_predicate.left_operand)->second;

    if (is_lqp_column_reference(atomic_predicate.right_operand)) {
      const auto right_column_reference = boost::get<LQPColumnReference>(atomic_predicate.right_operand);
      auto& right_column_statistics = estimation_state.column_statistics.find(right_column_reference)->second;

      const auto estimate = left_column_statistics->estimate_predicate_with_column(atomic_predicate.predicate_condition, *right_column_statistics);

      left_column_statistics = estimate.left_column_statistics;
      right_column_statistics = estimate.right_column_statistics;
      estimation_state.current_cardinality *= estimate.selectivity;
    } else {
      const auto right_value = boost::get<AllTypeVariant >(atomic_predicate.right_operand);

      const auto estimate = left_column_statistics->estimate_predicate_with_value(atomic_predicate.predicate_condition, right_value);

      left_column_statistics = estimate.column_statistics;
      estimation_state.current_cardinality *= estimate.selectivity;
    }

    // Cap distinct_counts to cardinality because there can't be more distinct values than there are rows
    for (auto& pair : estimation_state.column_statistics) {
      const auto& column = pair.first;
      if (estimation_state.vertices_not_joined.find(estimation_state.vertices.find(column)->second) != estimation_state.vertices_not_joined.end()) continue;

      auto& column_statistics = pair.second;
      if (column_statistics->distinct_count() < estimation_state.current_cardinality) continue;

      if (!column_statistics.unique()) column_statistics = column_statistics->clone();

      std::const_pointer_cast<AbstractColumnStatistics>(column_statistics)->set_distinct_count(estimation_state.current_cardinality);
    }
  } else {
    const auto& logical_predicate = static_cast<const JoinPlanLogicalPredicate&>(predicate);

    switch (logical_predicate.logical_operator) {
      case JoinPlanPredicateLogicalOperator::And : {
        _apply_predicate(*logical_predicate.left_operand, estimation_state);
        _apply_predicate(*logical_predicate.right_operand, estimation_state);
      }
        break;

      case JoinPlanPredicateLogicalOperator::Or: {
        auto estimation_state_left_input = estimation_state;
        auto estimation_state_right_input = estimation_state;

        _apply_predicate(*logical_predicate.left_operand, estimation_state_left_input);
        _apply_predicate(*logical_predicate.right_operand, estimation_state_right_input);

        // Merge left and right s
        estimation_state.current_cardinality =
        estimation_state_left_input.current_cardinality + estimation_state_right_input.current_cardinality * TableStatistics::DEFAULT_DISJUNCTION_SELECTIVITY;
        estimation_state.vertices_not_joined.clear();
        estimation_state.column_statistics = estimation_state_left_input.column_statistics; // The LQP::derive_statistics does this too, so let's not diverge from that for now

        std::set_intersection(estimation_state_left_input.vertices_not_joined.begin(),
                              estimation_state_left_input.vertices_not_joined.end(),
                              estimation_state_right_input.vertices_not_joined.begin(),
                              estimation_state_right_input.vertices_not_joined.end(),
                              std::inserter(estimation_state.vertices_not_joined, estimation_state.vertices_not_joined.begin()));
      } break;
    }
  }
}

void CardinalityEstimatorColumnStatistics::_ensure_vertex_is_joined(EstimationState& estimation_state, const LQPColumnReference& column_reference) {
  const auto vertex = estimation_state.vertices.find(column_reference)->second;

  const auto vertex_iter = estimation_state.vertices_not_joined.find(vertex);
  if (vertex_iter != estimation_state.vertices_not_joined.end()) {
    estimation_state.vertices_not_joined.erase(vertex_iter);
    estimation_state.current_cardinality *= vertex->get_statistics()->row_count();
  }
}

}  // namespace opossum
