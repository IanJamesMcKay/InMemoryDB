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

  void clear();

  static BaseJoinGraph _normalize(const BaseJoinGraph& join_graph);
  static std::shared_ptr<const AbstractJoinPlanPredicate> _normalize(const std::shared_ptr<const AbstractJoinPlanPredicate>& predicate);

 private:
  std::unordered_map<BaseJoinGraph, Cardinality> _cache;
  size_t _hit_count{0};
  size_t _miss_count{0};
};

}  // namespace opossum
