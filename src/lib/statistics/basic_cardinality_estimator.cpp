#include "basic_cardinality_estimator.hpp"

#include <unordered_set>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "expression/expression_utils.hpp"
#include "utils/assert.hpp"
#include "statistics/table_statistics.hpp"

namespace {

using namespace opossum; // NOLINT

struct VertexPredicateMatrix final {
  VertexPredicateMatrix(const size_t vertex_count, const size_t predicate_count) {
    data.resize(predicate_count);
    for (auto& column : data) {
      column.resize(vertex_count, false);
    }
  }

  /**
   * Predicates * Vertices matrix indicating whether a Predicate accesses a Vertex
   */
  std::vector<std::vector<bool>> data;

};

VertexPredicateMatrix build_vertex_predicate_matrix(const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices,
                                                    const std::vector<std::shared_ptr<AbstractExpression2>>& predicates) {
  VertexPredicateMatrix matrix{vertices.size(), predicates.size()};

  std::unordered_map<std::shared_ptr<AbstractLQPNode>, size_t> vertex_to_idx;
  for (auto vertex_idx = size_t{0}; vertex_idx < vertices.size(); ++vertex_idx) {
    vertex_to_idx.emplace(vertices[vertex_idx], vertex_idx);
  }

  for (auto predicate_idx = size_t{0}; predicate_idx < predicates.size(); ++predicate_idx) {
    auto predicate = predicates[predicate_idx];
    visit_expression(predicate, [&](const auto& expression){
      if (expression.type != ExpressionType::Column) return true;

      const auto lqp_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
      Assert(lqp_column_expression, "Unexpected ColumnExpression type");

      const auto vertex_idx_iter = vertex_to_idx.find(lqp_column_expression->column_reference.original_node());
      Assert(vertex_idx_iter != vertex_to_idx.end(), "Illegal Expression used, must refer to vertices only");

      matrix.data[predicate_idx][vertex_idx_iter->second] = true;
    });
  }

  return matrix;
}

std::shared_ptr<AbstractLQPNode> apply_predicate(const AbstractExpression2& predicate,
                                const std::shared_ptr<AbstractLQPNode>& current_node) {
  if (predicate.type == ExpressionType2::Logical) {

  } else if (predicate.type == ExpressionType2::Predicate) {

  } else {
    Fail("Unexpected predicate");
  }
}

}  // namespace

namespace opossum {

Cardinality BasicCardinalityEstimator::estimate(const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices,
                                                const std::vector<std::shared_ptr<AbstractExpression2>>& predicates) {
  Assert(!vertices.empty(), "Can't perform estimation on empty set of statistics");

  auto remaining_predicates = predicates;
  auto usable_vertices = std::unordered_set<std::shared_ptr<AbstractLQPNode>>{}; //

  auto included_vertices = std::vector<bool>{vertices.size(), false};
  auto current_estimate = *vertices.front()->get_statistics();
  auto current_node = *vertices.front()->get_statistics();

  const auto vertex_predicate_matrix = build_vertex_predicate_matrix(vertices, predicates);

  /**
   * We could do a Cross Product of all Vertices here and then just apply all Predicates. But doing a Cross would
   * probably make the temporary Cardinality explode beyond the reasonable. So we're bringing in Vertex by Vertex while
   * reducing the temporary Cardinality after each Vertex by applying all Predicates that can be executed on the
   * assembled Vertices.
   */

  for (auto predicate_idx = size_t{0}; predicate_idx < predicates.size(); ++predicate_idx) {
    for (auto vertex_idx = size_t{0}; vertex_idx < vertices.size(); ++vertex_idx) {
      if (vertex_predicate_matrix.data[predicate_idx][vertex_idx] && !included_vertices[vertex_idx]) {
        current_estimate = current_estimate.estimate_cross_join(*vertices[vertex_idx]->get_statistics());
        included_vertices[vertex_idx] = true;
      }
    }

    const auto& predicate = predicates[predicate_idx];

    current_estimate = apply_predicate(*predicate, current_estimate);
  }

  return current_estimate;
}

}  // namespace opossum
