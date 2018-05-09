#pragma once

#include <optional>
#include <unordered_map>

#include "abstract_cardinality_estimator.hpp"
#include "optimizer/join_ordering/base_join_graph.hpp"

namespace opossum {

class CardinalityEstimationCache final {
 public:
  std::optional<Cardinality> get(const BaseJoinGraph& join_graph) const;
  void put(const BaseJoinGraph& join_graph, const Cardinality& cardinality);

  size_t size() const;

 private:
  std::unordered_map<BaseJoinGraph, Cardinality> _cache;
};

}  // namespace opossum
