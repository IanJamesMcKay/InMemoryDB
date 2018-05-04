#include "gtest/gtest.h"

#include "logical_query_plan/mock_node.hpp"
#include "optimizer/join_ordering/join_plan_predicate.hpp"
#include "statistics/basic_cardinality_estimator.hpp"
#include "statistics/column_statistics.hpp"
#include "statistics/table_statistics.hpp"

namespace opossum {

class BasicCardinalityEstimatorTest : public ::testing::Test {
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

};

TEST_F(BasicCardinalityEstimatorTest, AtomicPredicates) {
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_r}, {}), 20.0f);
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_r}, {a_gt_5}), 10.0f);
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_r}, {a_gt_8}), 4.0f);
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_r}, {a_gt_5, a_gt_8}), 4.0f);

  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_r, vertex_s}, {}), 60.0f);
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_r, vertex_s}, {a_gt_5}), 30.0f);

  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_r, vertex_s}, {a_eq_d}), 6.0f);
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_r, vertex_s, vertex_t}, {a_eq_d}), 30.0f);
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_r, vertex_s, vertex_t}, {a_eq_d, d_eq_f}), 4.0f);
}

TEST_F(BasicCardinalityEstimatorTest, LogicalPredicates) {
  const auto a_gt_5_and_lt_8 = std::make_shared<JoinPlanLogicalPredicate>(a_gt_5, JoinPlanPredicateLogicalOperator::And, a_lt_8);
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_r}, {a_gt_5_and_lt_8}), 4);

  const auto a_lt_4_or_gt_8 = std::make_shared<JoinPlanLogicalPredicate>(a_lt_4, JoinPlanPredicateLogicalOperator::Or, a_gt_8);
  EXPECT_FLOAT_EQ(cardinality_estimator.estimate({vertex_r}, {a_lt_4_or_gt_8}), 6.8);
}

}  // namespace opossum