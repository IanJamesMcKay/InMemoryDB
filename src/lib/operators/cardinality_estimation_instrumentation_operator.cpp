#include "cardinality_estimation_instrumentation_operator.hpp"

#include "optimizer/join_ordering/join_graph_builder.hpp"
#include "statistics/cardinality_estimation_cache.hpp"

namespace opossum {

CardinalityEstimationInstrumentationOperator::CardinalityEstimationInstrumentationOperator(const std::shared_ptr<const AbstractOperator>& left,
                                                                                           const std::shared_ptr<CardinalityEstimationCache>& cardinality_estimation_cache):
  AbstractReadOnlyOperator(OperatorType::CardinalityEstimationInstrumentation, left), _cardinality_estimation_cache(cardinality_estimation_cache) {}

const std::string CardinalityEstimationInstrumentationOperator::name() const {
  return "CardinalityEstimationInstrumentationNode";
}

std::shared_ptr<const Table> CardinalityEstimationInstrumentationOperator::_on_execute() {
  if (!input_left()->lqp_node()) return input_table_left();

  const auto lqp = input_left()->lqp_node();

  auto join_graph_builder = JoinGraphBuilder{};
  join_graph_builder.traverse(lqp);

  _cardinality_estimation_cache->put({join_graph_builder.vertices(), join_graph_builder.predicates()}, input_table_left()->row_count());

  return input_table_left();
}

}  // namespace opossum