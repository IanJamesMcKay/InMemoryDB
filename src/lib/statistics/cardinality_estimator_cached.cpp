#include "cardinality_estimator_cached.hpp"

#include "cardinality_estimation_cache.hpp"

namespace opossum {

CardinalityEstimatorCached::CardinalityEstimatorCached(const std::shared_ptr<CardinalityEstimationCache>& cache,
                           const CardinalityEstimationCacheMode cache_mode,
                           const std::shared_ptr<AbstractCardinalityEstimator>& fallback_estimator):
  _cache(cache), _cache_mode(cache_mode), _fallback_estimator(fallback_estimator)
{}

Cardinality CardinalityEstimatorCached::estimate(const std::vector<std::shared_ptr<AbstractLQPNode>>& relations,
                     const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates) const {
  const auto cached_cardinality = _cache->get({relations, predicates});

  if (cached_cardinality) return *cached_cardinality;

  const auto fallback_cardinality = _fallback_estimator->estimate(relations, predicates);

  if (_cache_mode == CardinalityEstimationCacheMode::ReadAndUpdate) {
    _cache->put({relations, predicates}, fallback_cardinality);
  }

  return fallback_cardinality;
}

}  // namespace opossum