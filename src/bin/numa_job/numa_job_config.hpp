#pragma once

#include <string>
#include <optional>

#include <cxxopts.hpp>

#include "tpch_join_ordering_workload.hpp"
#include "job_join_ordering_workload.hpp"

namespace opossum {

struct NumaJobConfig final {
  /**
   * CLI options
   */
  std::string workload_str = "tpch";
  float scale_factor = 0.1f;
  bool save_results = true;
  std::optional<std::vector<std::string>> query_name_strs;
  size_t iterations_per_query{1};
  bool isolate_queries{true};
  std::string evaluation_name;

  /**
   * Objects intitialised from CLI options
   */
  std::shared_ptr<AbstractJoinOrderingWorkload> workload;

  /**
   * Misc
   */
  std::string evaluation_dir;
  std::string evaluation_prefix;

  void add_options(cxxopts::Options& cli_options_description);
  void parse(const cxxopts::ParseResult& parse_result);
  void setup();
};

}  // namespace opossum
