#pragma once

#include <vector>
#include <string>
#include <optional>

#include "abstract_join_ordering_workload.hpp"

namespace opossum {

class TpchJoinOrderingWorkload : public AbstractJoinOrderingWorkload {
 public:
  TpchJoinOrderingWorkload(float scale_factor, const std::optional<std::vector<QueryID>>& query_ids);

  void setup() override;
  size_t query_count() const override;
  std::string get_query(const size_t query_idx) const override;
  std::string get_query_name(const size_t query_idx) const override;

 private:
  float _scale_factor;
  std::vector<QueryID> _query_ids;
};

}  // namespace opossum