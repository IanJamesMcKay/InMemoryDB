#include "cardinality_estimation_cache.hpp"

namespace opossum {

std::optional<Cardinality> CardinalityEstimationCache::get(const BaseJoinGraph& join_graph) const {
  const auto cache_iter = _cache.find(join_graph);
  return cache_iter != _cache.end() ? cache_iter->second : std::optional<Cardinality>{};
}

void CardinalityEstimationCache::put(const BaseJoinGraph& join_graph, const Cardinality& cardinality) {
  _cache[join_graph] = cardinality;
}

size_t CardinalityEstimationCache::size() const {
  return _cache.size();
}

}  // namespace opossum