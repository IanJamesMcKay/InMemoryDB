#include "cardinality_caching_callback.hpp"

#include "optimizer/join_ordering/join_graph_builder.hpp"
#include "statistics/cardinality_estimation_cache.hpp"
#include "abstract_operator.hpp"
#include "storage/table.hpp"

namespace opossum {

CardinalityCachingCallback::CardinalityCachingCallback(const std::shared_ptr<CardinalityEstimationCache>& cardinality_estimation_cache):
  _cardinality_estimation_cache(cardinality_estimation_cache) {}

void CardinalityCachingCallback::operator()(const std::shared_ptr<AbstractOperator>& op) {
  if (!op->lqp_node()) return;
  if (!op->get_output()) return;

  const auto lqp = op->lqp_node();

  if (lqp->type() != LQPNodeType::Predicate && lqp->type() != LQPNodeType::Join && lqp->type() != LQPNodeType::StoredTable) return;

  auto join_graph_builder = JoinGraphBuilder{};
  join_graph_builder.traverse(lqp);

  _cardinality_estimation_cache->put({join_graph_builder.vertices(), join_graph_builder.predicates()}, op->get_output()->row_count());
}

}  // namespace opossum