#include "joe_query.hpp"

#include <chrono>

#include "out.hpp"
#include "write_csv.hpp"

namespace opossum {

std::ostream &operator<<(std::ostream &stream, const JoeQuerySample &sample) {
  stream << sample.name << "," << sample.best_plan_execution_duration.count();
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
    query_iterations.emplace_back(*this);
  }

  // Run Query Iterations
  for (auto query_iteration : query_iterations) {
    query_iteration.run();
    std::chrono::microseconds execution_duration;
    std::chrono::microseconds planning_duration;
    size_t ce_cache_hit_count{0};
    size_t ce_cache_miss_count{0};
    size_t ce_cache_size{0};
    size_t ce_cache_distinct_hit_count{0};
    size_t ce_cache_distinct_miss_count{0};
    write_csv(query_iterations,
              "ExecutionDuration,",
              config->evaluation_prefix + "Queries.csv");
  }
}

}  // namespace opossum
