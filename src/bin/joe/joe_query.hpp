#pragma once

#include "joe_config.hpp"
#include "joe_query_iteration.hpp"

namespace opossum {

struct JoeQuerySample {
  std::string name;
  std::shared_ptr<JoePlan> best_plan;
};

std::ostream &operator<<(std::ostream &stream, const JoeQuerySample &sample);

class JoeQuery final {
 public:
  JoeQuery(const std::shared_ptr<JoeConfig>& config, const std::string& name, const std::string& sql);

  void run();

  std::shared_ptr<JoeConfig> config;
  std::string sql;

  std::chrono::steady_clock::time_point execution_begin;
  bool save_plan_results{false};
  size_t plans_execution_count{0};
  std::unordered_set<std::shared_ptr<AbstractLQPNode>, LQPHash, LQPEqual> executed_plans;

  std::vector<JoeQueryIteration> query_iterations;

  JoeQuerySample sample;
};

}  // namespace opossum
