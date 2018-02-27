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
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "planviz/abstract_visualizer.hpp"
#include "planviz/join_graph_visualizer.hpp"
#include "planviz/join_plan_visualizer.hpp"
#include "planviz/lqp_visualizer.hpp"
#include "planviz/sql_query_plan_visualizer.hpp"
#include "planviz/sql_pipeline_report.hpp"

using namespace opossum;  // NOLINT

int main() {
  opossum::TpchDbGenerator(0.001f, Chunk::MAX_SIZE).generate_and_store();


  const char* const query =
    R"(SELECT
          supp_nation,
          cust_nation,
          l_year,
          SUM(volume) as revenue
      FROM
          (SELECT
              n1.n_name as supp_nation,
              n2.n_name as cust_nation,
              l_shipdate as l_year,
              l_extendedprice * (1.0 - l_discount) as volume
          FROM
              supplier,
              lineitem,
              orders,
              customer,
              nation n1,
              nation n2
          WHERE
              s_suppkey = l_suppkey AND
              o_orderkey = l_orderkey AND
              c_custkey = o_custkey AND
              s_nationkey = n1.n_nationkey AND
              c_nationkey = n2.n_nationkey AND
              ((n1.n_name = 'IRAN' AND n2.n_name = 'IRAQ') OR
               (n1.n_name = 'IRAQ' AND n2.n_name = 'IRAN')) AND
              l_shipdate BETWEEN '1995-01-01' AND '1996-12-31'
          ) as shipping
      GROUP BY
          supp_nation, cust_nation, l_year
      ORDER BY
          supp_nation, cust_nation, l_year;)";
  SQLPipeline sql_pipeline{query, UseMvcc::No};

  const auto unoptimized_lqps = sql_pipeline.get_unoptimized_logical_plans();
  const auto unoptimized_lqp = LogicalPlanRootNode::make(unoptimized_lqps.at(0));

  const auto join_graph = JoinGraphBuilder{}(unoptimized_lqp);

  DpCcpTopK dp_ccp_top_k{join_graph, 10};
  dp_ccp_top_k();

  boost::dynamic_bitset<> vertex_set(join_graph->vertices.size());
  vertex_set.flip();

  const auto join_plans = dp_ccp_top_k.subplan_cache()->get_best_plans(vertex_set);

  auto min_time = std::numeric_limits<long>::max();

  for (const auto& join_plan : join_plans) {
    std::cout << "Executing plan; Cost: " << join_plan->plan_cost() << std::endl;

    auto lqp = join_plan->to_lqp();
    for (const auto& parent_relation : join_graph->parent_relations) {
      parent_relation.parent->set_child(parent_relation.child_side, lqp);
    }

    auto pqp = LQPTranslator{}.translate_node(unoptimized_lqp->left_child());



    SQLQueryPlan query_plan;
    query_plan.add_tree_by_root(pqp);

    const auto begin = std::chrono::high_resolution_clock::now();
    CurrentScheduler::schedule_and_wait_for_tasks(query_plan.create_tasks());
    const auto end = std::chrono::high_resolution_clock::now();
    const auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();

    if (microseconds < min_time) {
      lqp->print();
      min_time = microseconds;
    }

    std::cout << "Cost: " << join_plan->plan_cost() << "; Time: " << microseconds << "; Ratio: " << (microseconds / join_plan->plan_cost()) << std::endl;

    Print::print(pqp->get_output());

    std::cout << std::endl;
    std::cout << std::endl;
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
