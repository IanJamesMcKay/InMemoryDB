#include <iostream>
#include <chrono>

#include "types.hpp"

#include "storage/storage_manager.hpp"
#include "sql/sql_pipeline.hpp"
#include "scheduler/current_scheduler.hpp"
#include "optimizer/join_ordering/join_graph_builder.hpp"
#include "optimizer/join_ordering/dp_ccp.hpp"
#include "optimizer/join_ordering/dp_ccp_top_k.hpp"
#include "optimizer/strategy/join_ordering_rule.hpp"
#include "optimizer/optimizer.hpp"
#include "utils/load_table.hpp"
#include "tpch/tpch_queries.hpp"
#include "tpch/tpch_db_generator.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "planviz/abstract_visualizer.hpp"
#include "planviz/join_graph_visualizer.hpp"
#include "planviz/join_plan_visualizer.hpp"
#include "planviz/lqp_visualizer.hpp"
#include "planviz/sql_query_plan_visualizer.hpp"
#include "planviz/sql_pipeline_report.hpp"

using namespace opossum;  // NOLINT

int main() {
  opossum::TpchDbGenerator(0.001f, Chunk::MAX_SIZE).generate_and_store();

  SQLPipeline sql_pipeline{tpch_queries[6], UseMvcc::No};

  const auto unoptimized_lqps = sql_pipeline.get_unoptimized_logical_plans();

  const auto join_graph = JoinGraphBuilder{}(unoptimized_lqps.at(0));

  DpCcpTopK dp_ccp_top_k{join_graph, 10};
  dp_ccp_top_k();

  boost::dynamic_bitset<> vertex_set(join_graph->vertices.size());
  vertex_set.flip();

  const auto join_plans = dp_ccp_top_k.subplan_cache()->get_best_plans(vertex_set);

  for (const auto& join_plan : join_plans) {
    std::cout << "Executing plan; Cost: " << join_plan->plan_cost() << std::endl;

    auto lqp = join_plan->to_lqp();

    auto pqp = LQPTranslator{}.translate_node(lqp);

    SQLQueryPlan query_plan;
    query_plan.add_tree_by_root(pqp);

    const auto begin = std::chrono::high_resolution_clock::now();
    CurrentScheduler::schedule_and_wait_for_tasks(query_plan.create_tasks());
    const auto end = std::chrono::high_resolution_clock::now();
    const auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();

    std::cout << "Cost: " << join_plan->plan_cost() << "; Time: " << microseconds << "; Ratio: " << (microseconds / join_plan->plan_cost()) << std::endl;
    std::cout << std::endl;


  }


//  for (size_t supported_tpch_query_idx{0}; supported_tpch_query_idx < NUM_SUPPORTED_TPCH_QUERIES; ++supported_tpch_query_idx) {
//    const auto tpch_query_idx = tpch_supported_queries[supported_tpch_query_idx];
//    const auto sql = tpch_queries[tpch_query_idx];
//
//    GraphvizConfig graphviz_config;
//    graphviz_config.format = "svg";
//
//    VizGraphInfo graph_info;
//    graph_info.bg_color = "black";
//
//    SQLPipeline pipeline{sql, false};
//    create_sql_pipeline_report(pipeline, "TPCH-" + std::to_string(tpch_query_idx + 1), graphviz_config, graph_info);
//  }

  return 0;
}
