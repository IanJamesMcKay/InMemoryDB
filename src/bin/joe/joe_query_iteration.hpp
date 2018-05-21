#pragma once

#include "joe_config.hpp"
#include "joe_plan.hpp"

namespace opossum {

class JoeQuery;

struct JoeQueryIterationSample {
  std::shared_ptr<JoePlan> rank_zero_plan;
  std::shared_ptr<JoePlan> best_plan;
  std::chrono::microseconds planning_duration{0};
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
  std::vector<std::shared_ptr<JoePlan>> plans;
  size_t plans_execution_count{0};
  std::optional<std::chrono::seconds> current_plan_timeout;
};

}  // namespace opossum
