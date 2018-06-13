#pragma once

#include <memory>

#include "numa_job_config.hpp"
#include "numa_job_query.hpp"

namespace opossum {

class NumaJob final {
 public:
  explicit NumaJob(const std::shared_ptr<NumaJobConfig>& config);

  void run();

  std::shared_ptr<NumaJobConfig> config;

  std::vector<NumaJobQuery> queries;
};

}  // namespace opossum
