#pragma once

#include <vector>
#include <string>
#include <optional>

#include "abstract_join_ordering_workload.hpp"

namespace opossum {


class JobWorkload : public AbstractJoinOrderingWorkload {
 public:
  explicit JobWorkload(const std::optional<std::vector<std::string>>& query_names);

  void setup() override;
  size_t query_count() const override;
  std::string get_query(const size_t query_idx) const override;
  std::string get_query_name(const size_t query_idx) const override;

 private:
  std::vector<std::string> _query_names;
};

}  // namespace opossum
