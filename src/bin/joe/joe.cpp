#include "joe.hpp"

#include <algorithm>
#include <random>
#include <sstream>

#include "write_csv.hpp"

namespace opossum {

Joe::Joe(const std::shared_ptr<JoeConfig>& config, const size_t workload_iteration_idx): config(config), workload_iteration_idx(workload_iteration_idx) {}

void Joe::run() {
  // Init queries
  for (auto query_idx = size_t{0}; query_idx < config->workload->query_count(); ++query_idx) {
    queries.emplace_back(std::make_shared<JoeQuery>(*this,
                                                    config->workload->get_query_name(query_idx),
                                                    config->workload->get_query(query_idx)));
  }

  if (config->permute_workload) {
    out() << "-- Shuffling queries in workload" << std::endl;
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(queries.begin(), queries.end(), g);
    for (const auto& query : queries) {
      out() << "--- " << query->sample.name << std::endl;
    }
  }

  for (auto &query : queries) {
    query->run();

    if (config->cardinality_estimation_cache_access == CardinalityEstimationCacheAccess::ReadAndWrite) {
      out() << "-- Updating persistent cardinality estimation cache '" << config->cardinality_estimation_cache_path << "'" << std::endl;
      config->cardinality_estimation_cache->update(config->cardinality_estimation_cache_path);
      out() << "-- Done!" << std::endl;
    }

    if (config->isolate_queries) config->cardinality_estimation_cache->clear();

    write_csv(queries, "Name,BestPlanExecutionDuration", prefix() + "Queries.csv");

  }
}

std::string Joe::prefix() const {
  std::stringstream stream;
  stream << config->evaluation_prefix;
  stream << "WIter." << workload_iteration_idx;
  stream << ".";
  return stream.str();
}

}  // namespace opossum