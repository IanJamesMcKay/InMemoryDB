#include "numa_job.hpp"

#include "write_csv.hpp"

namespace opossum {

NumaJob::NumaJob(const std::shared_ptr<NumaJobConfig>& config): config(config) {}


void NumaJob::run() {
  // Init queries
  for (auto query_idx = size_t{0}; query_idx < config->workload->query_count(); ++query_idx) {
    queries.emplace_back(config, config->workload->get_query_name(query_idx), config->workload->get_query(query_idx));
  }

  for (auto &query : queries) {
    query.run();
    // if (config->isolate_queries) config->cardinality_estimation_cache->clear();
  }

  write_csv(queries, "Name,ExecutionDurationPerIteration", config->evaluation_prefix + "Queries.csv");
}

}  // namespace opossum