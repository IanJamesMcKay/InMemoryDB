#pragma once

#include "joe_config.hpp"
#include "joe_plan.hpp"

namespace opossum {

class JoeQuery;

struct JoeQueryIterationSample {
  std::chrono::microseconds execution_duration;
  std::chrono::microseconds planning_duration;
  size_t ce_cache_hit_count{0};
  size_t ce_cache_miss_count{0};
  size_t ce_cache_size{0};
  size_t ce_cache_distinct_hit_count{0};
  size_t ce_cache_distinct_miss_count{0};
};

std::ostream &operator<<(std::ostream &stream, const JoeQueryIterationSample &sample);

class JoeQueryIteration final {
 public:
  JoeQueryIteration(JoeQuery& query, const size_t idx);

  void run();

  JoeQuery& query;

  JoeQueryIterationSample sample;

  size_t idx;
  std::string name;
  std::vector<JoePlan> plans;
  std::optional<std::chrono::seconds> current_plan_timeout;
  std::chrono::microseconds best_plan_execution_duration{0};
  size_t best_plan_idx{0};
};

}  // namespace opossum
