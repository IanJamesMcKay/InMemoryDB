#include "cardinality_estimation_cache.hpp"

#include <iostream>

#include "optimizer/join_ordering/join_plan_predicate.hpp"

namespace opossum {

std::optional<Cardinality> CardinalityEstimationCache::get(const BaseJoinGraph& join_graph) {
  auto normalized_join_graph = _normalize(join_graph);
  
  const auto cache_iter = _cache.find(normalized_join_graph);
  if (cache_iter != _cache.end()) {
    std::cout << "CardinalityEstimationCache [HIT ]: " << normalized_join_graph.description() << ": " << cache_iter->second << std::endl;
    ++_hit_count;
    return cache_iter->second;
  } else {
    std::cout << "CardinalityEstimationCache [MISS]: " << normalized_join_graph.description() << std::endl;
    ++_miss_count;
    return std::nullopt;
  }
}

void CardinalityEstimationCache::put(const BaseJoinGraph& join_graph, const Cardinality cardinality) {
  auto normalized_join_graph = _normalize(join_graph);
  
  if (_cache.count(normalized_join_graph) == 0) {
    std::cout << "CardinalityEstimationCache [PUT ]: " << normalized_join_graph.description() << ": " << cardinality << std::endl;
  }
  _cache[normalized_join_graph] = cardinality;
}

size_t CardinalityEstimationCache::cache_hit_count() const {
  return _hit_count;
}

size_t CardinalityEstimationCache::cache_miss_count() const {
  return _miss_count;
}

size_t CardinalityEstimationCache::size() const {
  return _cache.size();
}

BaseJoinGraph CardinalityEstimationCache::_normalize(const BaseJoinGraph& join_graph) {
  auto normalized_join_graph = join_graph;

  for (auto& predicate : normalized_join_graph.predicates) {
    if (predicate->type() == JoinPlanPredicateType::Atomic) {
      const auto& atomic_predicate = std::static_pointer_cast<JoinPlanAtomicPredicate>(predicate);
      if (is_lqp_column_reference(atomic_predicate->right_operand)) {
        const auto right_operand_column_reference = boost::get<LQPColumnReference>(atomic_predicate->right_operand);

        if (std::hash<LQPColumnReference>{}(right_operand_column_reference) < std::hash<LQPColumnReference>{}(atomic_predicate->left_operand)) {
          flip_predicate_condition(atomic_predicate->)

          predicate = std::make_shared<JoinPlanAtomicPredicate>()
        }
      }
    } else {

    }

  }

  return join_graph;
}

}  // namespace opossum