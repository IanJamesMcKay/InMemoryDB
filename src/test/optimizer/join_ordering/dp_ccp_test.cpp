#include "gtest/gtest.h"

#include <string>

#include "sql/sql_pipeline.hpp"
#include "optimizer/join_ordering/abstract_join_plan_node.hpp"
#include "optimizer/join_ordering/join_edge.hpp"
#include "optimizer/join_ordering/join_graph_builder.hpp"
#include "optimizer/join_ordering/dp_ccp.hpp"
#include "storage/storage_manager.hpp"
#include "optimizer/strategy/join_ordering_rule.hpp"
#include "utils/load_table.hpp"
#include "testing_assert.hpp"

namespace {

struct SqlAndPredicates {
  std::string sql;
  std::vector<std::string> predicates;
};

}  // namespace

namespace opossum {

// Fixture for tests that check whether the expected predicates are generated from an SQL query.
class DpCcpSqlToPredicatesTest : public ::testing::TestWithParam<SqlAndPredicates> {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("t_a", load_table("src/test/tables/sqlite/int_int_int_100.tbl"));
    StorageManager::get().add_table("t_b", load_table("src/test/tables/sqlite/int_int_int_100.tbl"));
    StorageManager::get().add_table("t_c", load_table("src/test/tables/sqlite/int_int_int_100.tbl"));
  }
  void TearDown() override {
    StorageManager::reset();
  }

  bool _join_graph_has_predicates(const JoinGraph& join_graph, const std::vector<std::string>& predicates) const {
    auto predicate_count_in_join_graph = size_t{0};
    for (const auto& edge : join_graph.edges) {
      predicate_count_in_join_graph += edge->predicates.size();
    }

    if (predicates.size() != predicate_count_in_join_graph) return false;

    return std::all_of(predicates.begin(), predicates.end(), [&](const auto& predicate) {
      auto found_predicate = std::any_of(join_graph.edges.begin(), join_graph.edges.end(), [&](const auto& edge) {
        return std::any_of(edge->predicates.begin(), edge->predicates.end(), [&](const auto& predicate2) {
          std::stringstream stream;
          predicate2->print(stream);
          std::cout << stream.str() << std::endl;  // uncomment for debugging
          return stream.str() == predicate;
        });
      });

      if (!found_predicate) {
        std::cout << "Couldn't find predicate '" << predicate << "'" << std::endl;
      }

      return found_predicate;
    });
  }
};

TEST_P(DpCcpSqlToPredicatesTest, PreservesPredicates) {
  const auto sql_and_predicates = GetParam();

  SQLPipeline sql_pipeline{sql_and_predicates.sql};

  ASSERT_EQ(sql_pipeline.get_unoptimized_logical_plans().size(), 1u);
  const auto unoptimized_lqp = sql_pipeline.get_unoptimized_logical_plans().at(0);
  const auto unoptimized_join_graph = JoinGraphBuilder{}(unoptimized_lqp);

  EXPECT_TRUE(_join_graph_has_predicates(*unoptimized_join_graph, sql_and_predicates.predicates));

  const auto join_plan = DpCcp{}(unoptimized_join_graph);

  const auto optimized_lqp = join_plan->to_lqp();
  const auto optimized_join_graph = JoinGraphBuilder{}(unoptimized_lqp);
  EXPECT_TRUE(_join_graph_has_predicates(*optimized_join_graph, sql_and_predicates.predicates));
}

// clang-format off
INSTANTIATE_TEST_CASE_P(DpCcpSqlToPredicatesTestInstances,
                        DpCcpSqlToPredicatesTest,
                        ::testing::Values(

SqlAndPredicates{"SELECT * FROM t_a WHERE int_a > 5 AND int_b < 3",
                 {"t_a.int_a > 5", "t_a.int_b < 3"}},

SqlAndPredicates{"SELECT * FROM t_a, t_b WHERE t_a.int_a > 5 AND (t_a.int_a > t_b.int_b OR t_a.int_a < t_b.int_b)",
                 {"t_a.int_a > 5", "t_a.int_a > t_b.int_b OR t_a.int_a < t_b.int_b"}},

SqlAndPredicates{"SELECT * FROM t_a JOIN t_b ON t_a.int_a = t_b.int_a WHERE (t_a.int_a < 5 OR t_a.int_b < 5 OR t_b.int_a < 5) AND t_a.int_c = 4",
                 {"t_a.int_a = t_b.int_a", "(t_a.int_a < 5 OR t_a.int_b < 5) OR t_b.int_a < 5", "t_a.int_c = 4"}}

),);

// clang-format on

// Test that the same table is generated from an SQL query, with and without DpCcp
class DpCcpSqlTest: public ::testing::TestWithParam<std::string> {
 public:
  void SetUp() override {
    StorageManager::get().add_table("t_a", load_table("src/test/tables/sqlite/int_int_int_100.tbl"));
    StorageManager::get().add_table("t_b", load_table("src/test/tables/sqlite/int_int_int_100.tbl"));
    StorageManager::get().add_table("t_c", load_table("src/test/tables/sqlite/int_int_int_100.tbl"));
  }
  void TearDown() override {
    StorageManager::reset();
  }
};


TEST_P(DpCcpSqlTest, SameResult) {
  const auto sql = GetParam();

  auto optimizer = std::make_shared<Optimizer>(1);
  RuleBatch rule_batch(RuleBatchExecutionPolicy::Once);
  rule_batch.add_rule(std::make_shared<JoinOrderingRule>(std::make_shared<DpCcp>()));
  optimizer->add_rule_batch(rule_batch);

  SQLPipelineStatement optimized_sql_statement{sql, optimizer};
  SQLPipelineStatement unoptimized_sql_statement{sql, Optimizer::get_dummy_optimizer()};

  EXPECT_TABLE_EQ_UNORDERED(optimized_sql_statement.get_result_table(), unoptimized_sql_statement.get_result_table());
}

// clang-format off
INSTANTIATE_TEST_CASE_P(DpCcpSqlTestInstances,
                        DpCcpSqlTest,
                        ::testing::Values(
  R"(SELECT * FROM t_a, t_b, t_c WHERE t_a.id = t_b.id;)"
                        ), );
// clang-format on

}  // namespace opossum
