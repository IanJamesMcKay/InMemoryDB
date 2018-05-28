#include "cardinality_estimator_cached.hpp"

#include <iostream>

#include "cardinality_estimation_cache.hpp"

namespace opossum {

CardinalityEstimatorCached::CardinalityEstimatorCached(const std::shared_ptr<CardinalityEstimationCache>& cache,
                           const CardinalityEstimationCacheMode cache_mode,
                           const std::shared_ptr<AbstractCardinalityEstimator>& fallback_estimator):
  _cache(cache), _cache_mode(cache_mode), _fallback_estimator(fallback_estimator)
{}

std::optional<Cardinality> CardinalityEstimatorCached::estimate(const std::vector<std::shared_ptr<AbstractLQPNode>>& relations,
                     const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates) const {
  const auto cached_cardinality = _cache->get({relations, predicates});

  if (cached_cardinality) return *cached_cardinality;

  auto fallback_cardinality = std::optional<Cardinality>{};

  if (_fallback_estimator) {
    fallback_cardinality = _fallback_estimator->estimate(relations, predicates);
  } else {
    BaseJoinGraph join_graph{relations, predicates};
    std::cout << "Cardinality for " << join_graph.description() << " not in cache - and no fallback estimator specified" << std::endl;
  }

  if (fallback_cardinality && _cache_mode == CardinalityEstimationCacheMode::ReadAndUpdate) {
    _cache->put({relations, predicates}, fallback_cardinality.value());
  }

  return fallback_cardinality;
}

}  // namespace opossum