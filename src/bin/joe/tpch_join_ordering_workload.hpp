#pragma once
#include <array>
#include <chrono>
#include <thread>
#include <fstream>
#include <iostream>

#include <cxxopts.hpp>
#include <statistics/generate_table_statistics.hpp>
#include <experimental/filesystem>
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
#include "statistics/cardinality_estimator_statistics.hpp"
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

#include "abstract_join_ordering_workload.hpp"
#include "out.hpp"

namespace opossum {

class TpchJoinOrderingWorkload : public AbstractJoinOrderingWorkload {
 public:
  TpchJoinOrderingWorkload(float scale_factor, const std::optional<std::vector<QueryID>>& query_ids);

  void setup() override;
  size_t query_count() const override;
  std::string get_query(const size_t query_idx) const override;
  std::string get_query_name(const size_t query_idx) const override;

 private:
  float _scale_factor;
  std::vector<QueryID> _query_ids;
};

}  // namespace opossum