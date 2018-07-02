#include <array>
#include <experimental/filesystem>
#include <chrono>
#include <thread>
#include <fstream>
#include <iostream>
#include <iostream>
#include <iomanip>
#include <ctime>

#include <cxxopts.hpp>
#include <statistics/generate_table_statistics.hpp>
#include <experimental/filesystem>
#include <numa.h>
#include <random>

#include "constant_mappings.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "operators/cardinality_caching_callback.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/product.hpp"
#include "concurrency/transaction_context.hpp"
#include "concurrency/transaction_manager.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/utils/flatten_pqp.hpp"
#include "operators/export_binary.hpp"
#include "operators/import_binary.hpp"
#include "operators/limit.hpp"
#include "optimizer/join_ordering/abstract_join_plan_node.hpp"
#include "cost_model/cost.hpp"
#include "cost_model/cost_model_naive.hpp"
#include "cost_model/cost_model_linear.hpp"
#include "import_export/csv_parser.hpp"
#include "optimizer/join_ordering/dp_ccp.hpp"
#include "optimizer/join_ordering/dp_ccp_top_k.hpp"
#include "optimizer/strategy/join_ordering_rule.hpp"
#include "statistics/table_statistics.hpp"
#include "optimizer/table_statistics_cache.hpp"
#include "planviz/lqp_visualizer.hpp"
#include "planviz/sql_query_plan_visualizer.hpp"
#include "scheduler/current_scheduler.hpp"
#include "statistics/cardinality_estimator_execution.hpp"
#include "statistics/cardinality_estimator_column_statistics.hpp"
#include "statistics/cardinality_estimation_cache.hpp"
#include "statistics/cardinality_estimator_cached.hpp"
#include "statistics/statistics_import_export.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "sql/sql.hpp"
#include "storage/storage_manager.hpp"
#include "tpch/tpch_db_generator.hpp"
#include "tpch/tpch_queries.hpp"
#include "utils/timer.hpp"
#include "utils/table_generator2.hpp"
#include "utils/format_duration.hpp"
#include "utils/execute_with_timeout.hpp"
#include "optimizer/table_statistics_cache.hpp"

#include <boost/lexical_cast.hpp>
using boost::lexical_cast;

#include <boost/uuid/uuid.hpp>
using boost::uuids::uuid;

#include <boost/uuid/uuid_generators.hpp>
using boost::uuids::random_generator;

#include <boost/uuid/uuid_io.hpp>
#include <cost_model/cost_feature_lqp_node_proxy.hpp>
#include <cost_model/cost_feature_operator_proxy.hpp>

#include "joe/job_join_ordering_workload.hpp"
#include "joe/tpch_join_ordering_workload.hpp"
#include "joe/joe_config.hpp"
#include "joe/joe.hpp"

using namespace opossum;

int main(int argc, char ** argv) {
  // Pin to the current node
  {
    const auto cpu = sched_getcpu();
    Assert(cpu >= 0, "Couldn't determine CPU");

    const auto node = numa_node_of_cpu(cpu);
    Assert(node >= 0, "Couldn't determine Node");

    std::cout << "Binding process to CPU " << cpu << " on Node " << node << std::endl;

    auto * node_mask = numa_allocate_nodemask();
    numa_bitmask_setbit(node_mask, node);

    numa_bind(node_mask);

    numa_free_nodemask(node_mask);
  }

  cxxopts::Options cli_options_description{"Joe, Hyrise's Join Ordering Evaluator", ""};

  const auto config = std::make_shared<JoeConfig>();
  config->add_options(cli_options_description);

  cli_options_description.parse_positional("queries");
  const auto cli_parse_result = cli_options_description.parse(argc, argv);

  // Display usage and quit
  if (cli_parse_result.count("help")) {
    std::cout << cli_options_description.help({}) << std::endl;
    return 0;
  }

  config->parse(cli_parse_result);
  config->setup();

  for (auto workload_iteration_idx = size_t{0};
       workload_iteration_idx < config->iterations_per_workload;
       ++workload_iteration_idx) {

    if (config->iterations_per_workload != 1) {
      out() << "-- Workload Iteration " << workload_iteration_idx << std::endl;
    }

    Joe joe{config, workload_iteration_idx};
    joe.run();

    //
    config->cardinality_estimation_cache->clear();
  }
}
