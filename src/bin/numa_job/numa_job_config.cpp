#include "numa_job_config.hpp"

#include <iomanip>

#include <cxxopts.hpp>
#include <experimental/filesystem>

#include "scheduler/topology.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "utils/assert.hpp"

#include "out.hpp"

namespace opossum {

void NumaJobConfig::add_options(cxxopts::Options& cli_options_description) {
  // clang-format off
  cli_options_description.add_options()
  ("h,help", "print this help message")
  ("e,evaluation-name", "Specify a name for the evaluation. Leave empty and one will be generated based on the current time and date", cxxopts::value<std::string>(evaluation_name)->default_value(""))
  ("v,verbose", "Print log messages", cxxopts::value<bool>(verbose)->default_value("true"))
  ("s,scale", "Database scale factor (1.0 ~ 1GB). TPCH only", cxxopts::value<float>(scale_factor)->default_value("0.001"))
  ("w,workload", "Workload to run (tpch, job). Default: tpch", cxxopts::value(workload_str)->default_value(workload_str))  // NOLINT
  ("save-results", "Save head of result tables.", cxxopts::value(save_results)->default_value("true"))  // NOLINT
  ("iterations-per-query", "Number of times to execute/optimize each query", cxxopts::value(iterations_per_query)->default_value("1"))  // NOLINT
  ("isolate-queries", "Reset all cached data for each query", cxxopts::value(isolate_queries)->default_value("true"))  // NOLINT
  ("queries", "Specify queries to run, default is all of the workload that are supported", cxxopts::value<std::vector<std::string>>()) // NOLINT
  ("use-scheduler", "Use the NodeQueueScheduler", cxxopts::value(use_scheduler)->default_value("true")) // NOLINT
  ("numa-cores", "Specify number of cores used by the scheduler", cxxopts::value(numa_cores)->default_value("0")); // NOLINT
  ;
  // clang-format on
}

void NumaJobConfig::parse(const cxxopts::ParseResult& cli_parse_result) {
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

  // Process "save_results" parameter
  if (save_results) {
    out() << "-- Saving query results" << std::endl;
  } else {
    out() << "-- Not saving query results" << std::endl;
  }

  // Process "iterations-per-query" parameter
  out() << "-- Running " << iterations_per_query << " iteration(s) per query" << std::endl;

  // Process "isolate-queries" parameter
  if (isolate_queries) {
    out() << "-- Isolating query evaluations from each other" << std::endl;
  } else {
    out() << "-- Not isolating query evaluations from each other" << std::endl;
  }

  // Process "use-scheduler" and "numa-cores" parameters
  if (use_scheduler) {
    out() << "-- Using Hyrise's NodeQueueScheduler with a NUMA topology using " << numa_cores << " cores" << std::endl;
  } else {
    out() << "-- Scheduler NOT active" << std::endl;
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
    workload = std::make_shared<JobWorkload>(query_name_strs);
  } else {
    Fail("Unknown workload");
  }
}

void NumaJobConfig::setup() {
  /**
  * Create evaluation dir
  */
  evaluation_dir = "numa_job/" + evaluation_name;
  out() << "-- Writing results to '" << evaluation_dir << "'" << std::endl;
  std::experimental::filesystem::create_directories(evaluation_dir);
  evaluation_prefix = evaluation_dir + "/" + "TODO" + "-" + std::string(IS_DEBUG ? "d" : "r") + "-";

  /**
   * Init Scheduler
   */
  if (use_scheduler) {
    auto topology = Topology::create_numa_topology(numa_cores);
    CurrentScheduler::set(std::make_shared<NodeQueueScheduler>(topology));
    CurrentScheduler::get()->topology()->print();
  }

  /**
   * Load workload
   */
  out() << "-- Setting up workload" << std::endl;
  workload->setup();
  out() << std::endl;
}

}  // namespace opossum