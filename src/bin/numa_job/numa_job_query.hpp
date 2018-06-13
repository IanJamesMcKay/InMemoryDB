#pragma once

#include <chrono>

#include "numa_job_config.hpp"

namespace opossum {

struct NumaJobQuerySample {
  std::string name;
  std::chrono::microseconds execution_duration{0};
  // std::shared_ptr<NumaJobPlan> best_plan;
};

std::ostream &operator<<(std::ostream &stream, const NumaJobQuerySample &sample);

class NumaJobQuery final {
 public:
  NumaJobQuery(const std::shared_ptr<NumaJobConfig>& config, const std::string& name, const std::string& sql);

  void run();

  std::shared_ptr<NumaJobConfig> config;
  std::string sql;

  std::chrono::steady_clock::time_point execution_begin;

  NumaJobQuerySample sample;
};

}  // namespace opossum
