#include <chrono>
#include <iostream>

#include "types.hpp"

#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "optimizer/join_ordering/cost_model_segmented_sampler.hpp"
#include "optimizer/join_ordering/cost_model_segmented.hpp"
#include "optimizer/join_ordering/dp_ccp.hpp"
#include "optimizer/join_ordering/dp_ccp_top_k.hpp"
#include "optimizer/join_ordering/join_graph_builder.hpp"
#include "optimizer/optimizer.hpp"
#include "optimizer/strategy/join_ordering_rule.hpp"
#include "planviz/abstract_visualizer.hpp"
#include "planviz/join_graph_visualizer.hpp"
#include "planviz/join_plan_visualizer.hpp"
#include "planviz/lqp_visualizer.hpp"
#include "planviz/sql_pipeline_report.hpp"
#include "planviz/sql_query_plan_visualizer.hpp"
#include "scheduler/current_scheduler.hpp"
#include "sql/sql.hpp"
#include "sql/sql_pipeline.hpp"
#include "storage/storage_manager.hpp"
#include "tpch/tpch_db_generator.hpp"
#include "tpch/tpch_queries.hpp"
#include "utils/load_table.hpp"

#include <boost/numeric/ublas/matrix_proxy.hpp>
#include "boost/numeric/ublas/matrix.hpp"

using namespace opossum;  // NOLINT

using namespace boost::numeric;

int main() {

  std::vector<std::string> queries = {
    R"(SELECT * FROM "part" WHERE p_size > 50;)",
    R"(SELECT * FROM "part" WHERE p_size > 0 AND p_size < 30;)",
    R"(SELECT * FROM "part" WHERE p_size > 10 AND p_size < 40;)",
    R"(SELECT * FROM "part" WHERE p_size > 30 AND p_size < 35;)",
    R"(SELECT * FROM customer WHERE c_nationkey = 0;)",
    R"(SELECT * FROM customer WHERE c_acctbal > 0.0 AND c_acctbal > 1000.0;)",
    R"(SELECT * FROM customer WHERE c_acctbal > 1000.0 AND c_acctbal < 4000.0;)",
    R"(SELECT * FROM customer WHERE c_acctbal < 5000.0 AND c_acctbal > 300.0;)",
    R"(SELECT * FROM lineitem WHERE l_quantity = 10;)",
    R"(SELECT * FROM lineitem WHERE l_quantity > 10 AND l_quantity < 40;)",
    R"(SELECT * FROM lineitem WHERE l_quantity > 5 AND l_quantity < 20;)",
    R"(SELECT * FROM lineitem WHERE l_quantity > 1 AND l_quantity < 15;)",
    R"(SELECT * FROM lineitem WHERE l_shipdate > '1995-01-01' AND l_shipdate < '1996-12-31';)",
    R"(SELECT * FROM lineitem WHERE l_shipdate > '1996-01-01' AND l_shipdate < '1996-12-31';)",
    R"(SELECT * FROM lineitem WHERE l_shipdate > '1996-05-01' AND l_shipdate < '1996-07-31';)",
    R"(SELECT * FROM lineitem WHERE l_shipdate = '1996-12-31';)",
    R"(SELECT * FROM supplier AS s1, supplier AS s2 WHERE s1.s_nationkey = s2.s_nationkey AND s1.s_acctbal > s2.s_acctbal;)",
    R"(SELECT * FROM "part" AS a, "part" AS b WHERE a.p_type = b.p_type AND a.p_retailsize > b.p_retailsize AND a.p_size > b.p_size;)"
  };

  std::vector<float> scale_factors = {0.005f, 0.01f, 0.05f, 0.1f, 0.2f};

  CostModelSegmentedSampler cost_model_sampler;

  for (const auto scale_factor : scale_factors) {
    std::cout << "- Scale Factor: " << scale_factor << std::endl;
    opossum::TpchDbGenerator(scale_factor).generate_and_store();
    for (const auto &query : queries) {
      std::cout << "-- Executing: " << query << std::flush;
      auto sql_pipeline = SQL{query}.disable_mvcc().pipeline();

      sql_pipeline.get_result_table();
      std::cout << " - " << sql_pipeline.get_result_table()->row_count() << " rows" << std::endl;

      cost_model_sampler.sample(sql_pipeline.get_query_plans().at(0)->tree_roots().at(0));
      cost_model_sampler.write_samples();
    }
    StorageManager::reset();
  }

  //  opossum::TpchDbGenerator(0.05f).generate_and_store();
//
//  CostModelSegmentedSampler cost_model_sampler;
//
//  for (const auto tpch_idx : {0, 2, 4, 5, 6, 8, 9}) {
//    std::cout << "Executing TPCH " << (tpch_idx + 1) << std::endl;
//    auto sql_pipeline = SQL{tpch_queries[tpch_idx]}.disable_mvcc().pipeline();
//
//    const auto unoptimized_lqps = sql_pipeline.get_unoptimized_logical_plans();
//    const auto unoptimized_lqp = LogicalPlanRootNode::make(unoptimized_lqps.at(0));
//
//    const auto join_graph = JoinGraphBuilder{}(unoptimized_lqp);
//
//    const auto cost_model = std::make_shared<CostModelSegmented>();
//
//    DpCcpTopK dp_ccp_top_k{20, cost_model};
//    dp_ccp_top_k(join_graph);
//
//    boost::dynamic_bitset<> vertex_set(join_graph->vertices.size());
//    vertex_set.flip();
//
//    const auto join_plans = dp_ccp_top_k.subplan_cache()->get_best_plans(vertex_set);
//
//    auto min_time = std::numeric_limits<long>::max();
//
//    std::cout << join_plans.size() << " plans generated" << std::endl;
//
//    size_t plan_idx = 0;
//    for (const auto &join_plan : join_plans) {
//      auto lqp = join_plan->to_lqp();
//      for (const auto &output_relation : join_graph->output_relations) {
//        output_relation.output->set_input(output_relation.input_side, lqp);
//      }
//
//      auto pqp = LQPTranslator{}.translate_node(unoptimized_lqp->left_input());
//
//      SQLQueryPlan query_plan;
//      query_plan.add_tree_by_root(pqp);
//
//      const auto begin = std::chrono::high_resolution_clock::now();
//      CurrentScheduler::schedule_and_wait_for_tasks(query_plan.create_tasks());
//
//      cost_model_sampler.sample(query_plan.tree_roots().at(0));
//
//      const auto end = std::chrono::high_resolution_clock::now();
//      const auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
//
//      std::cout << (++plan_idx) << " - Cost: " << join_plan->plan_cost() << "; Time: " << microseconds << "; Ratio: "
//                << (microseconds / join_plan->plan_cost()) << std::endl;
//
//      if (microseconds < min_time) {
//        std::cout << "  --> Best plan so far!" << std::endl;
//
//        min_time = microseconds;
//
//        GraphvizConfig graphviz_config;
//        graphviz_config.format = "svg";
//
//        VizGraphInfo graph_info;
//        graph_info.bg_color = "black";
//
//        auto prefix_pqp = std::string("viz/pqp_") + std::to_string(microseconds) + "ms";
//        auto prefix_join_plan = std::string("viz/join_plan") + std::to_string(microseconds) + "ms";
//        auto prefix_lqp = std::string("viz/lqp") + std::to_string(microseconds) + "ms";
//        SQLQueryPlanVisualizer{graphviz_config, graph_info, {}, {}}.visualize(query_plan, "tmp.dot",
//                                                                              prefix_pqp + ".svg");
//        JoinPlanVisualizer{graphviz_config, graph_info, {}, {}}.visualize(join_plan, "tmp.dot",
//                                                                          prefix_join_plan + ".svg");
//
//        LQPVisualizer lqp_visualizer{graphviz_config, graph_info, {}, {}};
//        lqp_visualizer.set_cost_model(cost_model);
//        lqp_visualizer.visualize({unoptimized_lqp->left_input()}, "tmp.dot", prefix_lqp + ".svg");
//      }
//
//      cost_model_sampler.write_samples();
//    }
//  }

  return 0;
}
