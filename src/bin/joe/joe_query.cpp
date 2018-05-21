#include "joe_query.hpp"

#include <chrono>

#include "out.hpp"
#include "write_csv.hpp"

namespace opossum {

std::ostream &operator<<(std::ostream &stream, const JoeQuerySample &sample) {
  stream << sample.name << ",";

  if (sample.best_plan) {
    stream << sample.best_plan->sample.execution_duration.count();
  } else {
    stream << 0;
  }

  return stream;
}

JoeQuery::JoeQuery(const std::shared_ptr<JoeConfig>& config, const std::string& name, const std::string& sql):
  config(config), sql(sql) {
  sample.name = name;
}

void JoeQuery::run() {
  out() << "-- Evaluating Query: " << sample.name << std::endl;

  // Enable CardinalityEstimationCache logging
  if (config->cardinality_estimation_cache_log) {
    config->cardinality_estimation_cache->set_log(std::make_shared<std::ofstream>(config->evaluation_prefix + "CardinalityEstimationCache-" + sample.name + ".log"));
  }

  execution_begin = std::chrono::steady_clock::now();
  save_plan_results = config->save_results;

  // Init Query Iterations
  for (size_t query_iteration_idx = size_t{0}; query_iteration_idx < config->iterations_per_query; ++query_iteration_idx) {
    query_iterations.emplace_back(*this, query_iteration_idx);
  }

  // Run Query Iterations
  for (auto& query_iteration : query_iterations) {
    query_iteration.run();

    if (!sample.best_plan || sample.best_plan->sample.execution_duration > query_iteration.sample.best_plan->sample.execution_duration) {
      sample.best_plan = query_iteration.sample.best_plan;
    }

    if (config->save_query_iterations_results) {
      write_csv(query_iterations,
                "BestPlanExecutionDuration,PlanningDuration,CECacheHitCount,CECacheMissCount,CECacheSize,CECacheDistinctHitCount,CECacheDistinctMissCount,BestPlanHash",
                config->evaluation_prefix + sample.name + ".Iterations.csv");
    }
  }
}

}  // namespace opossum
