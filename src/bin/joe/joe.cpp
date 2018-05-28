#include "joe.hpp"

#include "write_csv.hpp"

namespace opossum {

Joe::Joe(const std::shared_ptr<JoeConfig>& config): config(config) {}


void Joe::run() {
  // Init queries
  for (auto query_idx = size_t{0}; query_idx < config->workload->query_count(); ++query_idx) {
    queries.emplace_back(config, config->workload->get_query_name(query_idx), config->workload->get_query(query_idx));
  }

  for (auto &query : queries) {
    query.run();

    if (config->cardinality_estimation_cache_access == CardinalityEstimationCacheAccess::ReadAndWrite) {
      out() << "-- Updating persistent cardinality estimation cache '" << config->cardinality_estimation_cache_path << "'" << std::endl;
      config->cardinality_estimation_cache->update(config->cardinality_estimation_cache_path);
      out() << "-- Done!" << std::endl;
    }

    if (config->isolate_queries) config->cardinality_estimation_cache->clear();

    write_csv(queries, "Name,BestPlanExecutionDuration", config->evaluation_prefix + "Queries.csv");

  }
}

}  // namespace opossum