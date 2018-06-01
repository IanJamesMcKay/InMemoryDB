#include "gtest/gtest.h"

#include "cost_model/cost_model_naive.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "optimizer/join_ordering/build_join_plan.hpp"
#include "optimizer/join_ordering/dp_ccp.hpp"
#include "optimizer/join_ordering/join_edge.hpp"
#include "optimizer/join_ordering/join_graph.hpp"
#include "optimizer/join_ordering/join_plan_join_node.hpp"
#include "optimizer/join_ordering/join_plan_vertex_node.hpp"
#include "storage/storage_manager.hpp"
#include "statistics/cardinality_estimator_statistics.hpp"
#include "statistics/column_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/load_table.hpp"
#include "testing_assert.hpp"

namespace opossum {

class DpCcpTest : public ::testing::Test {
 public:
  void SetUp() override {
    cardinality_estimator = std::make_shared<CardinalityEstimatorColumnStatistics>();
    cost_model = std::make_shared<CostModelNaive>();

    const auto column_statistics_a = std::make_shared<ColumnStatistics<int32_t>>(0.0f, 10.0f, 1, 10);
    const auto column_statistics_d = std::make_shared<ColumnStatistics<int32_t>>(0.0f, 3.0f, 5, 7);
    const auto column_statistics_f = std::make_shared<ColumnStatistics<int32_t>>(0.0f, 5.0f, 6, 9);

    const auto table_statistics_x = std::make_shared<TableStatistics>(TableType::Data, 20, std::vector<std::shared_ptr<const AbstractColumnStatistics>>{column_statistics_a});
    const auto table_statistics_y = std::make_shared<TableStatistics>(TableType::Data, 3, std::vector<std::shared_ptr<const AbstractColumnStatistics>>{column_statistics_d});
    const auto table_statistics_z = std::make_shared<TableStatistics>(TableType::Data, 5, std::vector<std::shared_ptr<const AbstractColumnStatistics>>{column_statistics_f});

    vertex_r = std::make_shared<MockNode>(table_statistics_x);
    vertex_s = std::make_shared<MockNode>(table_statistics_y);
    vertex_t = std::make_shared<MockNode>(table_statistics_z);

    a = vertex_r->output_column_references().at(0);
    d = vertex_s->output_column_references().at(0);
    f = vertex_t->output_column_references().at(0);

    a_lt_4 = std::make_shared<JoinPlanAtomicPredicate>(vertex_r->output_column_references().at(0), PredicateCondition::LessThan, 4);
    a_gt_5 = std::make_shared<JoinPlanAtomicPredicate>(vertex_r->output_column_references().at(0), PredicateCondition::GreaterThan, 5);
    a_gt_8 = std::make_shared<JoinPlanAtomicPredicate>(vertex_r->output_column_references().at(0), PredicateCondition::GreaterThan, 8);
    a_lt_8 = std::make_shared<JoinPlanAtomicPredicate>(vertex_r->output_column_references().at(0), PredicateCondition::LessThan, 8);
    a_eq_d = std::make_shared<JoinPlanAtomicPredicate>(vertex_r->output_column_references().at(0), PredicateCondition::Equals, vertex_s->output_column_references().at(0));
    d_eq_f = std::make_shared<JoinPlanAtomicPredicate>(vertex_s->output_column_references().at(0), PredicateCondition::Equals, vertex_t->output_column_references().at(0));
  }

  std::shared_ptr<AbstractLQPNode> vertex_r, vertex_s, vertex_t;
  std::shared_ptr<AbstractJoinPlanPredicate> a_lt_4, a_gt_5, a_gt_8, a_lt_8, a_eq_d, d_eq_f;
  std::shared_ptr<CardinalityEstimatorColumnStatistics> cardinality_estimator;
  std::shared_ptr<CostModelNaive> cost_model;
  LQPColumnReference a, d, f;
};

TEST_F(DpCcpTest, Basic) {
  /**
   * Test two vertices and a single join predicate
   */

  auto join_edge = std::make_shared<JoinEdge>(boost::dynamic_bitset<>{2, 0b11}, std::vector<std::shared_ptr<AbstractJoinPlanPredicate>>({a_eq_d}));
  auto join_graph = std::make_shared<JoinGraph>(
    std::vector<std::shared_ptr<AbstractLQPNode>>({vertex_r, vertex_s}),
    std::vector<LQPOutputRelation>(),
    std::vector<std::shared_ptr<JoinEdge>>({join_edge})
  );
  DpCcp dp_ccp{cost_model, cardinality_estimator};

  const auto actual_lqp = dp_ccp(join_graph).lqp;

  // clang-format off
  const auto expected_lqp =
  JoinNode::make(JoinMode::Inner, LQPColumnReferencePair(a, d), PredicateCondition::Equals,
    vertex_r,
    vertex_s
  );
  // clang-format on

  EXPECT_LQP_EQ(expected_lqp, actual_lqp);
}

}  // namespace opossum