#pragma once

#include <string>
#include <optional>

#include <cxxopts.hpp>

#include "cost_model/cost_model_naive.hpp"
#include "cost_model/cost_model_linear.hpp"
#include "tpch_join_ordering_workload.hpp"
#include "job_join_ordering_workload.hpp"

namespace opossum {

enum class CardinalityEstimationMode { Statistics, CacheOnly, Execution };
enum class CardinalityEstimationCacheAccess { None, ReadOnly, ReadAndWrite };

struct JoeConfig final {
  /**
   * CLI options
   */
  std::string cost_model_str = "linear";
  std::string workload_str = "tpch";
  std::string cardinality_estimation_str = "cached";
  std::string cardinality_estimation_cache_access_str = "none";
  std::string cardinality_estimation_cache_path = "joe/cardinality_estimation_cache.production.json";
  std::string imdb_dir = "";
  std::string job_dir = "";
  float scale_factor = 0.1f;
  float cardinality_estimator_statistics_penalty = 1.0f;
  bool visualize = false;
  std::optional<long> plan_timeout_seconds = std::optional<long>{0};
  std::optional<long> query_timeout_seconds = std::optional<long>{0};
  std::optional<long> dynamic_plan_timeout = std::optional<long>{0};
  bool dynamic_plan_timeout_enabled = true;
  std::optional<size_t> max_plan_execution_count = std::optional<size_t>{0};
  std::optional<size_t> max_plan_generation_count = std::optional<size_t>{0};
  bool save_results = true;
  std::optional<long> plan_order_shuffling = std::optional<long>{0};
  std::optional<std::vector<std::string>> query_name_strs;
  size_t iterations_per_query{1};
  size_t iterations_per_workload{1};
  bool permute_workload{false};
  bool isolate_queries{true};
  bool save_plan_results{true};
  bool save_query_iterations_results{true};
  CardinalityEstimationMode cardinality_estimation_mode{CardinalityEstimationMode::Statistics};
  std::optional<long> cardinality_estimator_execution_timeout{0};
  bool cardinality_estimation_cache_log{true};
  bool cardinality_estimation_cache_dump{true};
  bool unique_plans{false};
  bool force_plan_zero{false};
  bool join_graph_log{true};
  std::string evaluation_name;
  std::optional<std::string> cost_sample_dir{""};

  /**
   * Objects intitialised from CLI options
   */
  std::shared_ptr<AbstractCostModel> cost_model;
  std::shared_ptr<AbstractJoinOrderingWorkload> workload;
  std::shared_ptr<CardinalityEstimationCache> cardinality_estimation_cache;
  std::shared_ptr<AbstractCardinalityEstimator> fallback_cardinality_estimator;
  std::shared_ptr<AbstractCardinalityEstimator> main_cardinality_estimator;
  CardinalityEstimationCacheAccess cardinality_estimation_cache_access{CardinalityEstimationCacheAccess::None};

  /**
   * Misc
   */
  std::string evaluation_dir;
  std::string evaluation_prefix;
  std::string tmp_dot_file_path;

  /**
   * tmps
   */
  std::string max_plan_generation_count_str;
  std::string max_plan_execution_count_str;

  void add_options(cxxopts::Options& cli_options_description);
  void parse(const cxxopts::ParseResult& parse_result);
  void setup();
};

}  // namespace opossum
