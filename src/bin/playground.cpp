#include <iostream>

#include "types.hpp"

#include "table_generator.hpp"
#include "operators/print.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "tpch/tpch_db_generator.hpp"
#include "planviz/lqp_visualizer.hpp"
#include "storage/storage_manager.hpp"

using namespace opossum;  // NOLINT

void visualize(const std::string& name, const std::string& sql) {
  auto statement = SQLPipelineBuilder{sql}.disable_mvcc().create_pipeline_statement();

  GraphvizConfig graphviz_config;
  graphviz_config.format = "svg";

  VizGraphInfo graph_info;
  graph_info.bg_color = "white";

  VizVertexInfo vertex_info;
  vertex_info.color = "black";
  vertex_info.font_color = "black";

  VizEdgeInfo edge_info;
  edge_info.color = "black";
  edge_info.font_color = "black";

  LQPVisualizer visualizer{graphviz_config, graph_info, vertex_info, edge_info};

  visualizer.visualize_cardinality_estimates = false;

  visualizer.visualize({statement.get_optimized_logical_plan()}, "tmp.dot", name + ".svg");
}

int main() {
  TpchDbGenerator{0.001}.generate_and_store();

  TableGenerator table_generator;
  table_generator.num_columns = 5;
  table_generator.num_rows = 10;

  StorageManager::get().add_table("t1", table_generator.generate_table(10));
  StorageManager::get().add_table("t2", table_generator.generate_table(10));

  visualize("01-literal", "SELECT 42");
  visualize("02-get_table", "SELECT a FROM t1");
  visualize("03-calc_col", "SELECT a * b FROM t1");
  visualize("03-case", "SELECT CASE WHEN a > 5 THEN '>5' ELSE '<=5' END FROM t1");
  visualize("04-select_by_col", "SELECT * FROM t1 WHERE a > 5");
  visualize("05-select_by_expr", "SELECT * FROM t1 WHERE a * b > 5 AND a * b < 10");
  visualize("06-subselect", "SELECT (SELECT a FROM t1 LIMIT 1), (SELECT b FROM t2 LIMIT 1)");
  visualize("07-correlated-subselect", "SELECT (SELECT a FROM t1 WHERE t1.a = t2.a LIMIT 1), (SELECT b FROM t1 WHERE t1.a > t2.a LIMIT 1) FROM t2");
  visualize("08-aggregate", "SELECT SUM(a+e) + SUM(b) FROM t1 GROUP BY d HAVING SUM(c) > 5");
  visualize("09-order-by", "SELECT * FROM t1 ORDER BY a + b");
  visualize("10-join", "SELECT * FROM t1 JOIN t2 ON t1.a = t2.b");
  visualize("11-join-complex", "SELECT * FROM t1 JOIN t2 ON t1.a + t1.b = t2.b * 3 AND t1.a = 2");

  return 0;
}
