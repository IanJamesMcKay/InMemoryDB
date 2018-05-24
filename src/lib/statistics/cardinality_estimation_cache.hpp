#pragma once

#include <optional>
#include <unordered_map>

#include "abstract_cardinality_estimator.hpp"
#include "optimizer/join_ordering/base_join_graph.hpp"

namespace opossum {

class CardinalityEstimationCache final {
 public:
  std::optional<Cardinality> get(const BaseJoinGraph& join_graph) ;
  void put(const BaseJoinGraph& join_graph, const Cardinality cardinality);

  size_t cache_hit_count() const;
  size_t cache_miss_count() const;

  size_t size() const;

  size_t distinct_request_count() const;
  size_t distinct_hit_count() const;
  size_t distinct_miss_count() const;
  void reset_distinct_hit_miss_counts();

  void clear();

  void set_log(const std::shared_ptr<std::ostream>& log);

  static BaseJoinGraph _normalize(const BaseJoinGraph& join_graph);
  static std::shared_ptr<AbstractJoinPlanPredicate> _normalize(const std::shared_ptr<AbstractJoinPlanPredicate>& predicate);

 private:
  struct Entry {
    std::optional<Cardinality> cardinality;
    size_t request_count{0};
  };

  std::unordered_map<BaseJoinGraph, Entry> _cache;

  std::shared_ptr<std::ostream> _log;
  size_t _hit_count{0};
  size_t _miss_count{0};
};

}  // namespace opossum
