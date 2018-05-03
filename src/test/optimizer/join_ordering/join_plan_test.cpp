#include "gtest/gtest.h"

#include "cost_model/cost_model_naive.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "optimizer/join_ordering/build_join_plan.hpp"
#include "optimizer/join_ordering/join_plan_join_node.hpp"
#include "optimizer/join_ordering/join_plan_vertex_node.hpp"
#include "storage/storage_manager.hpp"
#include "statistics/basic_cardinality_estimator.hpp"
#include "statistics/column_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/load_table.hpp"
#include "testing_assert.hpp"

namespace opossum {

class JoinPlanTest : public ::testing::Test {
 public:
  void SetUp() override {
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
  std::shared_ptr<const AbstractJoinPlanPredicate> a_lt_4, a_gt_5, a_gt_8, a_lt_8, a_eq_d, d_eq_f;
  BasicCardinalityEstimator cardinality_estimator;
  CostModelNaive cost_model;
  LQPColumnReference a, d, f;
};

TEST_F(JoinPlanTest, VertexNode) {
  const auto expected_vertices = std::vector<std::shared_ptr<AbstractLQPNode>>({vertex_r});

  /** Single vertex - no predicates */
  const auto vertex_node_0 = build_join_plan_vertex_node(cost_model, vertex_r, {}, cardinality_estimator);
  EXPECT_EQ(vertex_node_0.plan_cost, 0);
  EXPECT_EQ(vertex_node_0.join_graph.vertices, expected_vertices);
  EXPECT_TRUE(vertex_node_0.join_graph.predicates.empty());
  EXPECT_LQP_EQ(vertex_node_0.lqp, vertex_r);

  /** Single vertex - one predicate */
  const auto vertex_node_1 = build_join_plan_vertex_node(cost_model, vertex_r, {a_lt_4}, cardinality_estimator);
  EXPECT_EQ(vertex_node_1.plan_cost, 20);
  EXPECT_EQ(vertex_node_1.join_graph.vertices, expected_vertices);
  EXPECT_EQ(vertex_node_1.join_graph.predicates.size(), 1u);
  EXPECT_EQ(vertex_node_1.join_graph.predicates.at(0), a_lt_4);

  // clang-format off
  auto expected_lqp_1 =
  PredicateNode::make(vertex_r->output_column_references().at(0), PredicateCondition::LessThan, 4,
    vertex_r
  );
  // clang-format on

  EXPECT_LQP_EQ(expected_lqp_1, vertex_node_1.lqp);


  /** Single vertex - two predicates */
  const auto vertex_node_2 = build_join_plan_vertex_node(cost_model, vertex_r, {a_lt_4, a_gt_8}, cardinality_estimator);
  EXPECT_EQ(vertex_node_2.plan_cost, 26);
  EXPECT_EQ(vertex_node_2.join_graph.vertices, expected_vertices);
  EXPECT_EQ(vertex_node_2.join_graph.predicates.size(), 2u);
  EXPECT_EQ(vertex_node_2.join_graph.predicates.at(0), a_lt_4);
  EXPECT_EQ(vertex_node_2.join_graph.predicates.at(1), a_gt_8);

  // clang-format off
  auto expected_lqp_2 =
  PredicateNode::make(vertex_r->output_column_references().at(0), PredicateCondition::GreaterThan, 8,
    PredicateNode::make(vertex_r->output_column_references().at(0), PredicateCondition::LessThan, 4,
      vertex_r
    ));
  // clang-format on

  EXPECT_LQP_EQ(expected_lqp_2, vertex_node_2.lqp);

  /** Single vertex - one "complex" predicate */
  const auto a_lt_4_or_gt_8 = std::make_shared<JoinPlanLogicalPredicate>(a_lt_4, JoinPlanPredicateLogicalOperator::Or, a_gt_8);
  const auto vertex_node_3 = build_join_plan_vertex_node(cost_model, vertex_r, {a_lt_4_or_gt_8}, cardinality_estimator);
  EXPECT_EQ(vertex_node_3.plan_cost, 40); // OR is considered free right now.
  EXPECT_EQ(vertex_node_3.join_graph.vertices, expected_vertices);
  EXPECT_EQ(vertex_node_3.join_graph.predicates.size(), 1u);
  EXPECT_EQ(vertex_node_3.join_graph.predicates.at(0), a_lt_4_or_gt_8);

  // clang-format off
  auto expected_lqp_3 =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(vertex_r->output_column_references().at(0), PredicateCondition::LessThan, 4, vertex_r),
    PredicateNode::make(vertex_r->output_column_references().at(0), PredicateCondition::GreaterThan, 8, vertex_r)
  );
  // clang-format on

  EXPECT_LQP_EQ(expected_lqp_3, vertex_node_3.lqp);
}

TEST_F(JoinPlanTest, JoinNode) {
  const auto vertex_node_0 = build_join_plan_vertex_node(cost_model, vertex_r, {a_lt_4}, cardinality_estimator);
  const auto vertex_node_1 = build_join_plan_vertex_node(cost_model, vertex_s, {}, cardinality_estimator);
  const auto vertex_node_2 = build_join_plan_vertex_node(cost_model, vertex_t, {}, cardinality_estimator);

  /**
   * Two vertices, one local predicate, cross join
   */
  const auto join_node_0 = build_join_plan_join_node(cost_model, vertex_node_0, vertex_node_1, {}, cardinality_estimator);

  EXPECT_FLOAT_EQ(join_node_0.plan_cost, 38);
  EXPECT_EQ(join_node_0.join_graph.vertices.size(), 2u);
  EXPECT_EQ(join_node_0.join_graph.vertices.at(0), vertex_r);
  EXPECT_EQ(join_node_0.join_graph.vertices.at(1), vertex_s);
  EXPECT_EQ(join_node_0.join_graph.predicates.size(), 1u);
  EXPECT_EQ(join_node_0.join_graph.predicates.at(0), a_lt_4);

  // clang-format off
  auto expected_lqp_0 =
  JoinNode::make(JoinMode::Cross,
   PredicateNode::make(vertex_r->output_column_references().at(0), PredicateCondition::LessThan, 4, vertex_r),
   vertex_s
  );
  // clang-format on

  EXPECT_LQP_EQ(expected_lqp_0, join_node_0.lqp);


  /**
   * Three vertices, one local predicate, two join predicates, one secondary predicate
   */
  const auto join_node_1 = build_join_plan_join_node(cost_model, vertex_node_0, vertex_node_1, {a_eq_d}, cardinality_estimator);
  const auto join_node_2 = build_join_plan_join_node(cost_model, join_node_1, vertex_node_2, {d_eq_f, a_lt_8}, cardinality_estimator);

  EXPECT_FLOAT_EQ(join_node_1.plan_cost, 29);
  EXPECT_FLOAT_EQ(join_node_2.plan_cost, 142);

  // clang-format off
  auto expected_lqp_1 =
  JoinNode::make(JoinMode::Inner, LQPColumnReferencePair(a, d), PredicateCondition::Equals,
     PredicateNode::make(a, PredicateCondition::LessThan, 4, vertex_r),
     vertex_s
  );
  // clang-format on

  EXPECT_LQP_EQ(expected_lqp_1, join_node_1.lqp);

}

}  // namespace opossum