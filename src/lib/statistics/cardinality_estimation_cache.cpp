#include "cardinality_estimation_cache.hpp"

#include <iostream>

namespace opossum {

std::optional<Cardinality> CardinalityEstimationCache::get(const BaseJoinGraph& join_graph) {
  const auto cache_iter = _cache.find(join_graph);
  if (cache_iter != _cache.end()) {
    std::cout << "CardinalityEstimationCache [HIT ]: " << join_graph.description() << ": " << cache_iter->second << std::endl;
    ++_hit_count;
    return cache_iter->second;
  } else {
    std::cout << "CardinalityEstimationCache [MISS]: " << join_graph.description() << std::endl;
    ++_miss_count;
    return std::nullopt;
  }
}

void CardinalityEstimationCache::put(const BaseJoinGraph& join_graph, const Cardinality cardinality) {
  if (_cache.count(join_graph) == 0) {
    std::cout << "CardinalityEstimationCache [PUT ]: " << join_graph.description() << ": " << cardinality << std::endl;
  }
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