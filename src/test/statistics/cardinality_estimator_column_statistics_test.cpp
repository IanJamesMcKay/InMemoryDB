#include "gtest/gtest.h"

#include "logical_query_plan/mock_node.hpp"
#include "optimizer/join_ordering/join_plan_predicate.hpp"
#include "statistics/cardinality_estimator_statistics.hpp"
#include "statistics/column_statistics.hpp"
#include "statistics/table_statistics.hpp"

namespace opossum {

class BasicCardinalityEstimatorTest : public ::testing::Test {
 public:
  void SetUp() override {
    const auto column_statistics_x1 = std::make_shared<ColumnStatistics<int32_t>>(0.0f, 10.0f, 1, 10);
    const auto column_statistics_x2 = std::make_shared<ColumnStatistics<int32_t>>(0.0f, 7.0f, 6, 15);
    const auto column_statistics_y1 = std::make_shared<ColumnStatistics<int32_t>>(0.0f, 3.0f, 5, 7);
    const auto column_statistics_z1 = std::make_shared<ColumnStatistics<int32_t>>(0.0f, 5.0f, 6, 9);

    const auto table_statistics_x = std::make_shared<TableStatistics>(TableType::Data, 20, std::vector<std::shared_ptr<const AbstractColumnStatistics>>{column_statistics_x1, column_statistics_x2});
    const auto table_statistics_y = std::make_shared<TableStatistics>(TableType::Data, 3, std::vector<std::shared_ptr<const AbstractColumnStatistics>>{column_statistics_y1});
    const auto table_statistics_z = std::make_shared<TableStatistics>(TableType::Data, 5, std::vector<std::shared_ptr<const AbstractColumnStatistics>>{column_statistics_z1});

    vertex_x = std::make_shared<MockNode>(table_statistics_x);
    vertex_y = std::make_shared<MockNode>(table_statistics_y);
    vertex_z = std::make_shared<MockNode>(table_statistics_z);

    const auto x1 = vertex_x->output_column_references().at(0);
    const auto x2 = vertex_x->output_column_references().at(1);
    const auto y1 = vertex_y->output_column_references().at(0);
    const auto z1 = vertex_z->output_column_references().at(0);

    x1_lt_4 = std::make_shared<JoinPlanAtomicPredicate>(x1, PredicateCondition::LessThan, 4);
    x1_gt_5 = std::make_shared<JoinPlanAtomicPredicate>(x1, PredicateCondition::GreaterThan, 5);
    x2_ge_10 = std::make_shared<JoinPlanAtomicPredicate>(x2, PredicateCondition::GreaterThanEquals, 10);
    x2_ge_14 = std::make_shared<JoinPlanAtomicPredicate>(x2, PredicateCondition::GreaterThanEquals, 14);
    x1_gt_8 = std::make_shared<JoinPlanAtomicPredicate>(x1, PredicateCondition::GreaterThan, 8);
    x1_lt_8 = std::make_shared<JoinPlanAtomicPredicate>(x1, PredicateCondition::LessThan, 8);
    x1_eq_y1 = std::make_shared<JoinPlanAtomicPredicate>(x1, PredicateCondition::Equals, y1);
    x1_ne_y1 = std::make_shared<JoinPlanAtomicPredicate>(x1, PredicateCondition::NotEquals, y1);
    y1_eq_z1 = std::make_shared<JoinPlanAtomicPredicate>(y1, PredicateCondition::Equals, z1);
    y1_gt_5 = std::make_shared<JoinPlanAtomicPredicate>(y1, PredicateCondition::GreaterThan, 5);
  }

  std::shared_ptr<AbstractLQPNode> vertex_x, vertex_y, vertex_z;
  std::shared_ptr<AbstractJoinPlanPredicate> x1_lt_4, x1_gt_5, x2_ge_10, x2_ge_14, x1_gt_8, x1_lt_8, x1_eq_y1, x1_ne_y1, y1_eq_z1, y1_gt_5;
  CardinalityEstimatorColumnStatistics cardinality_estimator;

};

TEST_F(BasicCardinalityEstimatorTest, AtomicVertexPredicates) {
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_x}, {}), 20.0f);
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_x}, {x1_gt_5}), 10.0f);
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_x}, {x1_gt_8}), 4.0f);
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_x}, {x1_gt_8, x2_ge_10}), 2.4f);
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_x}, {x2_ge_10}), 12.0f);
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_x}, {x1_gt_5, x1_gt_8}), 4.0f);
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_y}, {y1_gt_5}), 2.0f);

  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_x, vertex_y}, {}), 60.0f);
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_x, vertex_y}, {x1_gt_5}), 30.0f);
}

TEST_F(BasicCardinalityEstimatorTest, AtomicJoinPredicates) {
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_x, vertex_y}, {x1_eq_y1}), 6.0f);
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_x, vertex_y}, {y1_gt_5, x1_eq_y1}), 4.0f);
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_x, vertex_y, vertex_z}, {x1_eq_y1}), 30.0f);
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_x, vertex_y, vertex_z}, {x1_eq_y1, y1_eq_z1}), 4.0f);
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_x, vertex_y}, {x2_ge_10}), 36.f);
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_x, vertex_y}, {x2_ge_10, x1_eq_y1}), 3.6f);
}

TEST_F(BasicCardinalityEstimatorTest, AtomicJoinPredicatesWithDistinctCapping) {
  // Test that `vertex_x JOIN vertex_y ON x1 = y1 AND x2 >= 14` receives a selectivity penalty for x2 >= 14 selecting
  // less tuples than the distinct count of x1.
  // Currently the CardinalityEstimatorColumnStatistics will just cap x1's distinct_count at 4, which is the cardinality after
  // applying (x2 >= 14).
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_x, vertex_y}, {x2_ge_14, x1_eq_y1}), 1.2f);
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_y, vertex_x}, {x1_eq_y1, x2_ge_14}), 1.2f);
}

TEST_F(BasicCardinalityEstimatorTest, LogicalPredicates) {
  const auto x1_gt_5_and_lt_8 = std::make_shared<JoinPlanLogicalPredicate>(x1_gt_5, JoinPlanPredicateLogicalOperator::And, x1_lt_8);
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_x}, {x1_gt_5_and_lt_8}), 4);

  const auto x1_lt_4_or_gt_8 = std::make_shared<JoinPlanLogicalPredicate>(x1_lt_4, JoinPlanPredicateLogicalOperator::Or, x1_gt_8);
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_x}, {x1_lt_4_or_gt_8}), 6.8f);

  // (x1 < 4 AND x2 >= 10)
  const auto x1_lt_4_and_x2_ge_10 = std::make_shared<JoinPlanLogicalPredicate>(x1_lt_4, JoinPlanPredicateLogicalOperator::And, x2_ge_10);
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_x}, {x1_lt_4_and_x2_ge_10}), 3.6);

  // (x1 < 4 AND x2 >= 10) OR x > 8
  const auto or_x1_gt_8 = std::make_shared<JoinPlanLogicalPredicate>(x1_lt_4_and_x2_ge_10, JoinPlanPredicateLogicalOperator::Or, x1_gt_8);
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_x}, {or_x1_gt_8}), 4.4f);

  // ((x1 < 4 AND x2 >= 10) OR x > 8) AND x2 >= 14
  const auto and_x2_ge_14 = std::make_shared<JoinPlanLogicalPredicate>(or_x1_gt_8, JoinPlanPredicateLogicalOperator::And, x2_ge_14);
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_x}, {and_x2_ge_14}), 4.4f / 3.0f);
}

}  // namespace opossum