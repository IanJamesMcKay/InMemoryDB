#include "cardinality_estimation_cache.hpp"

namespace opossum {

std::optional<Cardinality> CardinalityEstimationCache::get(const BaseJoinGraph& join_graph) {
  const auto cache_iter = _cache.find(join_graph);
  if (cache_iter != _cache.end()) {
    ++_hit_count;
    return cache_iter->second;
  } else {
    ++_miss_count;
    return std::nullopt;
  }
}

void CardinalityEstimationCache::put(const BaseJoinGraph& join_graph, const Cardinality cardinality) {
  _cache[join_graph] = cardinality;
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

}  // namespace opossum