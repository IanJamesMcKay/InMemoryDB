#include <chrono>
#include <iostream>

#include "types.hpp"

#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/lqp_translator.hpp"
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
  opossum::TpchDbGenerator(0.001f, 10'000).generate_and_store();

  SQLPipeline sql_pipeline{tpch_queries[4], UseMvcc::No};

  const auto unoptimized_lqps = sql_pipeline.get_unoptimized_logical_plans();
  const auto unoptimized_lqp = LogicalPlanRootNode::make(unoptimized_lqps.at(0));

  const auto join_graph = JoinGraphBuilder{}(unoptimized_lqp);

  DpCcpTopK dp_ccp_top_k{DpSubplanCacheTopK::NO_ENTRY_LIMIT, std::make_shared<CostModelSegmented>()};
  dp_ccp_top_k(join_graph);

  boost::dynamic_bitset<> vertex_set(join_graph->vertices.size());
  vertex_set.flip();

  const auto join_plans = dp_ccp_top_k.subplan_cache()->get_best_plans(vertex_set);

  auto min_time = std::numeric_limits<long>::max();

  std::cout << join_plans.size() << " plans generated" << std::endl;

  size_t plan_idx = 0;
  for (const auto& join_plan : join_plans) {
//    std::cout << "Executing plan; Cost: " << join_plan->plan_cost() << std::endl;

    auto lqp = join_plan->to_lqp();
    for (const auto& output_relation : join_graph->output_relations) {
      output_relation.output->set_input(output_relation.input_side, lqp);
    }

    auto pqp = LQPTranslator{}.translate_node(unoptimized_lqp->left_input());

    SQLQueryPlan query_plan;
    query_plan.add_tree_by_root(pqp);

    const auto begin = std::chrono::high_resolution_clock::now();
    CurrentScheduler::schedule_and_wait_for_tasks(query_plan.create_tasks());
    const auto end = std::chrono::high_resolution_clock::now();
    const auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();

    if (microseconds < min_time) {
      pqp->print();
      min_time = microseconds;

      GraphvizConfig graphviz_config;
      graphviz_config.format = "svg";

      VizGraphInfo graph_info;
      graph_info.bg_color = "black";

      auto prefix_pqp = std::string("pqp_") + std::to_string(microseconds) + "ms";
      auto prefix_join_plan = std::string("join_plan") + std::to_string(microseconds) + "ms";
      SQLQueryPlanVisualizer{graphviz_config, graph_info, {}, {}}.visualize(query_plan, "tmp.dot", prefix_pqp + ".svg");
      JoinPlanVisualizer{graphviz_config, graph_info, {}, {}}.visualize(join_plan, "tmp.dot", prefix_join_plan + ".svg");
    }

    std::cout << (++plan_idx) << " - Cost: " << join_plan->plan_cost() << "; Time: " << microseconds << "; Ratio: " << (microseconds / join_plan->plan_cost()) << std::endl;

  }
  return 0;
}
