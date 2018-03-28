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
  // Column-Value Scans
  R"(SELECT * FROM "part" WHERE p_size > 50;)",
  R"(SELECT * FROM "part" WHERE p_size > 0 AND p_size < 30;)",
  R"(SELECT * FROM "part" WHERE p_size > 10 AND p_size < 40;)",
  R"(SELECT * FROM "part" WHERE p_size > 30 AND p_size < 35;)",
  R"(SELECT * FROM "part" WHERE p_size > 1 OR p_size < 30;)",
  R"(SELECT * FROM "part" WHERE p_size > 10 OR p_size < 40;)",
  R"(SELECT * FROM "part" WHERE p_size > 30 OR p_size < 35;)",
  R"(SELECT * FROM customer WHERE c_nationkey = 0;)",
  R"(SELECT * FROM customer WHERE c_acctbal > 0.0 AND c_acctbal > 1000.0;)",
  R"(SELECT * FROM customer WHERE c_acctbal > 1000.0 AND c_acctbal < 4000.0;)",
  R"(SELECT * FROM customer WHERE c_acctbal < 5000.0 AND c_acctbal > 300.0;)",
  R"(SELECT * FROM customer WHERE c_acctbal > 0.0 OR c_acctbal > 1000.0;)",
  R"(SELECT * FROM customer WHERE c_acctbal > 1000.0 OR c_acctbal < 4000.0;)",
  R"(SELECT * FROM customer WHERE c_acctbal < 5000.0 OR c_acctbal > 300.0;)",
  R"(SELECT * FROM lineitem WHERE l_quantity = 10;)",
  R"(SELECT * FROM lineitem WHERE l_quantity > 10 AND l_quantity < 40;)",
  R"(SELECT * FROM lineitem WHERE l_quantity > 5 AND l_quantity < 20;)",
  R"(SELECT * FROM lineitem WHERE l_quantity > 1 AND l_quantity < 15;)",
  R"(SELECT * FROM lineitem WHERE l_quantity > 10 OR l_quantity < 40;)",
  R"(SELECT * FROM lineitem WHERE l_quantity > 5 OR l_quantity < 20;)",
  R"(SELECT * FROM lineitem WHERE l_quantity > 1 OR l_quantity < 15;)",
  R"(SELECT * FROM lineitem WHERE l_shipdate > '1995-01-01' AND l_shipdate < '1996-12-31';)",
  R"(SELECT * FROM lineitem WHERE l_shipdate > '1996-01-01' AND l_shipdate < '1996-12-31';)",
  R"(SELECT * FROM lineitem WHERE l_shipdate > '1996-05-01' AND l_shipdate < '1996-07-31';)",
  R"(SELECT * FROM lineitem WHERE l_shipdate > '1995-01-01' OR l_shipdate < '1996-12-31';)",
  R"(SELECT * FROM lineitem WHERE l_shipdate > '1996-01-01' OR l_shipdate < '1996-12-31';)",
  R"(SELECT * FROM lineitem WHERE l_shipdate > '1996-05-01' OR l_shipdate < '1996-07-31';)",
  R"(SELECT * FROM lineitem WHERE l_shipdate = '1996-12-31';)",

  // Joins and Column-Column Scans
  R"(SELECT * FROM supplier AS s1, supplier AS s2 WHERE s1.s_nationkey = s2.s_nationkey AND s1.s_acctbal > s2.s_acctbal;)",
  R"(SELECT * FROM "part" AS a, "part" AS b WHERE a.p_type = b.p_type AND a.p_retailsize > b.p_retailsize AND a.p_size > b.p_size;)",

  // TPCH Queries
  tpch_queries[0],
  tpch_queries[2],
  tpch_queries[4],
  tpch_queries[5],
  tpch_queries[6],
  tpch_queries[8],
  tpch_queries[9]
  };

  std::vector<float> scale_factors = {0.001f, 0.005f, 0.01f, 0.05f, 0.08f};

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

  return 0;
}
