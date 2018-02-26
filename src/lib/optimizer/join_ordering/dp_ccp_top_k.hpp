#pragma once

#include "abstract_dp_algorithm.hpp"
#include "dp_subplan_cache_top_k.hpp"

namespace opossum {

class DpCcpTopK : public AbstractDpAlgorithm {
 public:
  explicit DpCcpTopK(const std::shared_ptr<const JoinGraph>& join_graph, const size_t max_entry_count_per_set);

  std::shared_ptr<DpSubplanCacheTopK> subplan_cache();
  std::shared_ptr<const DpSubplanCacheTopK> subplan_cache() const;

 protected:
  void _on_execute() override;
};

}  // namespace opossum
