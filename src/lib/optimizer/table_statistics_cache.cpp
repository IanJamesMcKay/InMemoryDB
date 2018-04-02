#include "table_statistics_cache.hpp"

#include "table_statistics.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

std::shared_ptr<TableStatistics> TableStatisticsCache::get(const std::shared_ptr<AbstractLQPNode>& lqp) const {
  const auto cache_iter = _cache.find(lqp);
  if (cache_iter != _cache.end()) {
    ++_hit_count;
    return cache_iter->second;
  }
  ++_miss_count;
  return lqp->get_statistics();
}

void TableStatisticsCache::put(const std::shared_ptr<AbstractLQPNode>& lqp, const std::shared_ptr<TableStatistics>& statistics) {
  _cache.emplace(lqp, statistics);
}

size_t TableStatisticsCache::hit_count() const {
  return _hit_count;
}

size_t TableStatisticsCache::miss_count() const {
  return _miss_count;
}

size_t TableStatisticsCache::LQPHash::operator()(const std::shared_ptr<AbstractLQPNode>& lqp) const {
  return lqp->hash();
}

size_t TableStatisticsCache::LQPEqual::operator()(const std::shared_ptr<AbstractLQPNode>& lhs, const std::shared_ptr<AbstractLQPNode>& rhs) const {
  return !lhs->find_first_subplan_mismatch(rhs);
}

}  // namespace opossum
