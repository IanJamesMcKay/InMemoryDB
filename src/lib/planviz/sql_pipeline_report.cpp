#include "sql_pipeline_report.hpp"

#include "join_graph_visualizer.hpp"
#include "join_plan_visualizer.hpp"
#include "lqp_visualizer.hpp"
#include "optimizer/join_ordering/dp_ccp.hpp"
#include "optimizer/join_ordering/join_graph_builder.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql_query_plan_visualizer.hpp"

namespace opossum {

void create_sql_pipeline_report(SQLPipeline& sql_pipeline, const std::string& name,
                                const GraphvizConfig& graphviz_config, const VizGraphInfo& graph_info,
                                const VizVertexInfo& vertex_info, const VizEdgeInfo& edge_info) {
  const auto extension = "." + graphviz_config.format;

  const auto unoptimized_lqps = sql_pipeline.get_unoptimized_logical_plans();
  LQPVisualizer{graphviz_config, graph_info, vertex_info, edge_info}.visualize(unoptimized_lqps, name + ".raw_lqp.dot",
                                                                               name + ".raw_lqp" + extension);

  const auto optimized_lqps = sql_pipeline.get_optimized_logical_plans();
  LQPVisualizer{graphviz_config, graph_info, vertex_info, edge_info}.visualize(optimized_lqps, name + ".opt_lqp.dot",
                                                                               name + ".opt_lqp" + extension);

  const auto result_table = sql_pipeline.get_result_table();

  const auto sql_query_plans = sql_pipeline.get_query_plans();
  for (size_t sql_query_plan_idx{0}; sql_query_plan_idx < sql_query_plans.size(); ++sql_query_plan_idx) {
    const auto prefix = name + ".sql_query_plan_" + std::to_string(sql_query_plan_idx);
    SQLQueryPlanVisualizer{graphviz_config, graph_info, vertex_info, edge_info}.visualize(
        *sql_query_plans[sql_query_plan_idx], prefix + ".dot", prefix + extension);
  }

  std::cout << "Compilation time: " << sql_pipeline.compile_time_microseconds().count() << "µs" << std::endl;
  std::cout << "Execution time: " << sql_pipeline.execution_time_microseconds().count() << "µs" << std::endl;
  //std::cout << "Optimization time: " << sql_pipeline.optimization_time_microseconds().count() << "µs" << std::endl;
  std::cout << std::endl;

  /**
   * Hacky Join graphs and plans
   */
  const auto unoptimized_lqps2 = sql_pipeline.get_unoptimized_logical_plans();

  for (size_t lqp_idx{0}; lqp_idx < unoptimized_lqps2.size(); ++lqp_idx) {
    const auto& unoptimized_lqp = unoptimized_lqps2[lqp_idx];

    std::cout << "Report Raw LQP" << std::endl;
    unoptimized_lqp->print();
    const auto join_graph = JoinGraphBuilder{}(unoptimized_lqp);
    std::cout << "Report Join Graph" << std::endl;
    join_graph->print();
    const auto prefix_join_graph = name + ".join_graph_" + std::to_string(lqp_idx);
    JoinGraphVisualizer{graphviz_config, graph_info, vertex_info, edge_info}.visualize(
        join_graph, prefix_join_graph + ".dot", prefix_join_graph + extension);

    const auto join_plan = DpCcp{}(join_graph);
    std::cout << "Report Join Plan" << std::endl;
    join_plan->print();
    const auto prefix_join_plan = name + ".join_plan_" + std::to_string(lqp_idx);
    JoinPlanVisualizer{graphviz_config, graph_info, vertex_info, edge_info}.visualize(
        join_plan, prefix_join_plan + ".dot", prefix_join_plan + extension);
  }
}

}  // namespace opossum
