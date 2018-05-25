#include "joe_config.hpp"

#include <cxxopts.hpp>
#include "out.hpp"
#include "utils/assert.hpp"

#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>

using boost::lexical_cast;
using boost::uuids::uuid;
using boost::uuids::random_generator;

namespace opossum {

void JoeConfig::add_options(cxxopts::Options& cli_options_description) {
  // clang-format off
  cli_options_description.add_options()
  ("help", "print this help message")
  ("evaluation", "Specify a name for the evaluation. Leave empty and one will be generated based on the current time and date", cxxopts::value<std::string>(evaluation_name)->default_value(""))
  ("verbose", "Print log messages", cxxopts::value<bool>(verbose)->default_value("true"))
  ("scale", "Database scale factor (1.0 ~ 1GB). TPCH only", cxxopts::value<float>(scale_factor)->default_value("0.001"))
  ("cost-model", "CostModel to use (all, naive, linear)", cxxopts::value<std::string>(cost_model_str)->default_value(cost_model_str))  // NOLINT
  ("timeout-plan", "Timeout per plan, in seconds. Default: 120", cxxopts::value<long>(*plan_timeout_seconds)->default_value("120"))  // NOLINT
  ("dynamic-timeout-plan", "If active, lower timeout to current fastest plan.", cxxopts::value<bool>(dynamic_plan_timeout_enabled)->default_value("true"))  // NOLINT
  ("timeout-query", "Timeout per plan, in seconds. Default: 1800", cxxopts::value<long>(*query_timeout_seconds)->default_value("1800"))  // NOLINT
  ("max-plan-execution-count", "Maximum number of plans per query to execute. Default: 100", cxxopts::value<size_t>(*max_plan_execution_count)->default_value("100"))  // NOLINT
  ("max-plan-generation-count", "Maximum number of plans to generate. Default: 100", cxxopts::value<size_t>(*max_plan_generation_count)->default_value("100"))  // NOLINT
  ("visualize", "Visualize every query plan", cxxopts::value<bool>(visualize)->default_value("false"))  // NOLINT
  ("workload", "Workload to run (tpch, job). Default: tpch", cxxopts::value(workload_str)->default_value(workload_str))  // NOLINT
  ("imdb-dir", "Location of the JOB data", cxxopts::value(imdb_dir)->default_value(imdb_dir))  // NOLINT
  ("job-dir", "Location of the JOB queries", cxxopts::value(job_dir)->default_value(job_dir))  // NOLINT
  ("save-results", "Save head of result tables.", cxxopts::value(save_results)->default_value("true"))  // NOLINT
  ("shuffle-idx", "Shuffle plan order from this index on, 0 to disable", cxxopts::value(*plan_order_shuffling)->default_value("0"))  // NOLINT
  ("iterations-per-query", "Number of times to execute/optimize each query", cxxopts::value(iterations_per_query)->default_value("1"))  // NOLINT
  ("isolate-queries", "Reset all cached data for each query", cxxopts::value(isolate_queries)->default_value("true"))  // NOLINT  // NOLINT
  ("save-plan-results", "Save measurements per plan", cxxopts::value(save_plan_results)->default_value("true"))  // NOLINT
  ("save-query-iterations-results", "Save measurements per query iterations", cxxopts::value(save_query_iterations_results)->default_value("true"))  // NOLINT
  ("cardinality-estimation", "Mode for cardinality estimation. Values: cached, executed", cxxopts::value(cardinality_estimation_str)->default_value("cached"))  // NOLINT
  ("cardinality-estimator-execution-timeout", "If the CardinalityEstimatorExecution is used, this specifies its timeout. 0 to disable", cxxopts::value(*cardinality_estimator_execution_timeout)->default_value("120"))
  ("cardinality-estimation-cache-log", "Create logfiles for accesses to the CardinalityEstimationCache", cxxopts::value(cardinality_estimation_cache_log)->default_value("true"))  // NOLINT
  ("cardinality-estimation-cache-dump", "Store the state of the cardinality estimation Cache", cxxopts::value(cardinality_estimation_cache_dump)->default_value("true"))  // NOLINT
  ("cardinality-estimation-execution-cache-store", "Storing of info in the CardinalityEstimationCache in execution mode", cxxopts::value(cardinality_estimation_execution_cache_store_mode_str)->default_value(cardinality_estimation_execution_cache_store_mode_str))  // NOLINT
  ("join-graph-log", "For each query, create a logfile with the join graph", cxxopts::value(join_graph_log)->default_value("true"))  // NOLINT
  ("unique-plans", "For each query, execute only plans that were not executed before", cxxopts::value(unique_plans)->default_value("false"))  // NOLINT
  ("force-plan-zero", "Independently of shuffling, always executed the plan the optimizer labeled as best", cxxopts::value(force_plan_zero)->default_value("false"))  // NOLINT
  ("queries", "Specify queries to run, default is all of the workload that are supported", cxxopts::value<std::vector<std::string>>()); // NOLINT
  ;
  // clang-format on
}

void JoeConfig::parse(const cxxopts::ParseResult& cli_parse_result) {
  // Process "evaluation_name" parameter
  if (!evaluation_name.empty()) {
    out() << "-- Using specified evaluation name '" << evaluation_name << "'" << std::endl;
  } else {
    auto t = std::time(nullptr);
    auto tm = *std::localtime(&t);
    std::stringstream stream;
    stream << std::put_time(&tm, "%Y-%m-%d-%H:%M:%S");
    evaluation_name = stream.str();
    out() << "-- Using generated evaluation name '" << evaluation_name << "'" << std::endl;
  }

  // Process "queries" parameter
  if (cli_parse_result.count("queries")) {
    query_name_strs = cli_parse_result["queries"].as<std::vector<std::string>>();
  }
  if (query_name_strs) {
    out() << "-- Benchmarking queries ";
    for (const auto& query_name : *query_name_strs) {
      out() << query_name << " ";
    }
    out() << std::endl;
  } else {
    out() << "-- Benchmarking all supported queries of workload" << std::endl;
  }

  // Process "visualize"
  out() << "-- Visualizing plans " << (visualize ? "enabled" : "disabled") << std::endl;

  // Process "join_graph_log"
  out() << "-- Logging JoinGraphs " << (join_graph_log ? "enabled" : "disabled") << std::endl;

  // Process "timeout-plan/query" parameters
  if (*plan_timeout_seconds <= 0) {
    plan_timeout_seconds.reset();
    out() << "-- No plan timeout" << std::endl;
  } else {
    out() << "-- Plans will timeout after " << *plan_timeout_seconds << " seconds" << std::endl;
  }
  if (*query_timeout_seconds <= 0) {
    query_timeout_seconds.reset();
    out() << "-- No query timeout" << std::endl;
  } else {
    out() << "-- Queries will timeout after " << *query_timeout_seconds << " seconds" << std::endl;
  }
  if (dynamic_plan_timeout_enabled) {
    out() << "-- Plan timeout is dynamic" << std::endl;
  } else {
    out() << "-- Dynamic plan timeout is disabled" << std::endl;
  }

  // Process "max-plan-count" parameter
  if (*max_plan_execution_count <= 0) {
    max_plan_execution_count.reset();
    out() << "-- Executing all plans of a query" << std::endl;
  } else {
    out() << "-- Executing a maximum of " << *max_plan_execution_count << " plans per query" << std::endl;
  }

  if (*max_plan_generation_count <= 0) {
    max_plan_generation_count.reset();
    out() << "-- Generating all plans" << std::endl;
  } else {
    if (max_plan_execution_count) {
      max_plan_generation_count = std::max(*max_plan_generation_count, *max_plan_generation_count);
    }
    out() << "-- Generating at max " << *max_plan_generation_count << " plans" << std::endl;
  }

  // Process "cost-model" parameter
  if (cost_model_str == "naive") {
    out() << "-- Using CostModelNaive" << std::endl;
    cost_model = std::make_shared<CostModelNaive>();
  } else if (cost_model_str == "linear") {
    out() << "-- Using CostModelLinear" << std::endl;
    cost_model = std::make_shared<CostModelLinear>();
  } else {
    Fail("No valid cost model specified");
  }

  // Process "save_results" parameter
  if (save_results) {
    out() << "-- Saving query results" << std::endl;
  } else {
    out() << "-- Not saving query results" << std::endl;
  }

  // Process "shuffle-idx" parameter
  if (*plan_order_shuffling <= 0) {
    plan_order_shuffling.reset();
    out() << "-- Plan order shuffling disabled" << std::endl;
  } else {
    out() << "-- Plan order shuffling from index  " << (*plan_order_shuffling) << " on" << std::endl;
  }

  // Process "iterations-per-query" parameter
  out() << "-- Running " << iterations_per_query << " iteration(s) per query" << std::endl;

  // Process "isolate-queries" parameter
  if (isolate_queries) {
    out() << "-- Isolating query evaluations from each other" << std::endl;
  } else {
    out() << "-- Not isolating query evaluations from each other" << std::endl;
  }

  // Process "save_plan_results" parameter
  if (save_plan_results) {
    out() << "-- Saving measurements per Plan" << std::endl;
  } else {
    out() << "-- Not saving measurements per Plan" << std::endl;
  }

  // Process "save_query_iterations_results" parameter
  if (save_query_iterations_results) {
    out() << "-- Saving measurements per Query Iteration" << std::endl;
  } else {
    out() << "-- Not saving measurements per Query Iteration" << std::endl;
  }

  // Process "cardinality_estimation_str" parameter
  if (cardinality_estimation_str == "statistics") {
    cardinality_estimation_mode = CardinalityEstimationMode::ColumnStatistics;
    out() << "-- Using CardinalityEstimationMode::ColumnStatistics" << std::endl;
  } else if (cardinality_estimation_str == "executed") {
    cardinality_estimation_mode = CardinalityEstimationMode::Executed;
    out() << "-- Using CardinalityEstimationMode::Executed" << std::endl;

    if (*cardinality_estimator_execution_timeout > 0) {
      out() << "-- CardinaltiyEstimatorExecution timeout: " << (*cardinality_estimator_execution_timeout) << "s" << std::endl;
    } else {
      cardinality_estimator_execution_timeout.reset();
      out() << "-- No CardinaltiyEstimatorExecution timeout" << std::endl;
    }
  } else {
    Fail("Unsupported CardinalityEstimationMode");
  }

  // Process "cardinality_estimation_execution_cache_store_mode_str" parameter
  if (cardinality_estimation_execution_cache_store_mode_str == "none") {
    out() << "-- Not using persistent cardinality estimation cache" << std::endl;
    cardinality_estimation_execution_cache_store_mode = CardinalityEstimationCacheStoreMode::None;
  } else if (cardinality_estimation_execution_cache_store_mode_str == "ro") {
    out() << "-- Reading from persistent cardinality estimation cache" << std::endl;
    cardinality_estimation_execution_cache_store_mode = CardinalityEstimationCacheStoreMode::ReadOnly;
  } else if (cardinality_estimation_execution_cache_store_mode_str == "rw") {
    Assert(cardinality_estimation_mode == CardinalityEstimationMode::Executed, "Writing to persistent CardinalityEstimationCache only enabled in Executed mode, for safety");
    Assert(!isolate_queries, "Populating the persistent cache and isolating queries doesn't work in combination");
    out() << "-- ReadAndWrite access to persistent cardinality estimation cache" << std::endl;
    cardinality_estimation_execution_cache_store_mode = CardinalityEstimationCacheStoreMode::ReadAndWrite;
  } else {
    Fail("Invalid cardinality_estimation_execution_cache_store_mode_str");
  }

  // Process "cardinality_estimation_cache_log" parameter
  if (cardinality_estimation_cache_log) {
    out() << "-- CardinalityEstimationCache logging enabled" << std::endl;
  } else {
    out() << "-- CardinalityEstimationCache logging disabled" << std::endl;
  }

  // Process "cardinality_estimation_cache_dump" parameter
  if (cardinality_estimation_cache_dump) {
    out() << "-- CardinalityEstimationCache dumping enabled" << std::endl;
  } else {
    out() << "-- CardinalityEstimationCache dumping disabled" << std::endl;
  }

  // Process "unique_plans" parameter
  if (unique_plans) {
    out() << "-- Executing only unique plans is enabled" << std::endl;
  } else {
    out() << "-- Executing only unique plans is disabled" << std::endl;
  }

  // Process "force_plan_zero" parameter
  if (force_plan_zero) {
    out() << "-- Always executing plan at rank #0" <<std::endl;
  }

  // Process "workload" parameter
  if (workload_str == "tpch") {
    out() << "-- Using TPCH workload" << std::endl;
    std::optional<std::vector<QueryID>> query_ids;
    if (query_name_strs) {
      query_ids.emplace();
      for (const auto& query_name : *query_name_strs) {
        query_ids->emplace_back(std::stoi(query_name) - 1);  // Offset because TPC-H query 1 has index 0
      }
    }
    workload = std::make_shared<TpchJoinOrderingWorkload>(scale_factor, query_ids);
  } else if (workload_str == "job") {
    out() << "-- Using Join Order Benchmark workload" << std::endl;
    out() << "-- Loading IMDB from '" << imdb_dir << "'" << std::endl;
    out() << "-- Loading JOB from '" << job_dir << "'" << std::endl;
    workload = std::make_shared<JobWorkload>(query_name_strs, imdb_dir, job_dir);
  } else {
    Fail("Unknown workload");
  }

}

void JoeConfig::setup() {
  /**
  * Create evaluation dir
  */
  evaluation_dir = "joe/" + evaluation_name;
  out() << "-- Writing results to '" << evaluation_dir << "'" << std::endl;
  tmp_dot_file_path = evaluation_dir + "/viz/" + boost::lexical_cast<std::string>(boost::uuids::random_generator{}()) + ".dot";
  std::experimental::filesystem::create_directories(evaluation_dir);
  std::experimental::filesystem::create_directory(evaluation_dir + "/viz");
  evaluation_prefix = evaluation_dir + "/" + cost_model->name() + "-" + std::string(IS_DEBUG ? "d" : "r") + "-";
  cardinality_estimation_execution_cache_path = "joe/cardinality_estimation_execution_cache.json";

  /**
   * Load workload
   */
  out() << "-- Setting up workload" << std::endl;
  workload->setup();
  out() << std::endl;

  /**
   * Setup CardinalityEstimator
   */
  if (cardinality_estimation_execution_cache_store_mode != CardinalityEstimationCacheStoreMode::None &&
      std::experimental::filesystem::exists(cardinality_estimation_execution_cache_path)) {
    out() << "-- Loading CardinalityEstimationCache from file..." << std::endl;
    cardinality_estimation_cache = CardinalityEstimationCache::load(cardinality_estimation_execution_cache_path);
  } else {
    out() << "-- Using a fresh CardinalityEstimationCache" << std::endl;
    cardinality_estimation_cache = std::make_shared<CardinalityEstimationCache>();
  }


  if (cardinality_estimation_mode == CardinalityEstimationMode::ColumnStatistics) {
    fallback_cardinality_estimator = std::make_shared<CardinalityEstimatorColumnStatistics>();
    main_cardinality_estimator = std::make_shared<CardinalityEstimatorCached>(cardinality_estimation_cache,
                                                                              CardinalityEstimationCacheMode::ReadOnly, fallback_cardinality_estimator);
  } else {
    const auto cardinaltiy_estimator_execution = std::make_shared<CardinalityEstimatorExecution>();
    if (cardinality_estimator_execution_timeout) {
      cardinaltiy_estimator_execution->timeout = std::chrono::seconds{*cardinality_estimator_execution_timeout};
    }
    fallback_cardinality_estimator = cardinaltiy_estimator_execution;
    main_cardinality_estimator = std::make_shared<CardinalityEstimatorCached>(cardinality_estimation_cache,
                                                                              CardinalityEstimationCacheMode::ReadAndUpdate, fallback_cardinality_estimator);
  }
}

}  // namespace opossum