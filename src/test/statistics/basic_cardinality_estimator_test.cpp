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
    auto column_statistics_a = std::make_shared<ColumnStatistics<int32_t>>(0.0f, 10.0f, 1, 10);

    auto table_statistics_x = std::make_shared<TableStatistics>(TableType::Data, 20, std::vector<std::shared_ptr<const AbstractColumnStatistics>>{column_statistics_a});

    vertex_r = std::make_shared<MockNode>(table_statistics_x);

    a_gt_5 = std::make_shared<JoinPlanAtomicPredicate>(vertex_r->output_column_references().at(0), PredicateCondition::GreaterThan, 5);
    a_gt_8 = std::make_shared<JoinPlanAtomicPredicate>(vertex_r->output_column_references().at(0), PredicateCondition::GreaterThan, 8);
  }

  std::shared_ptr<AbstractLQPNode> vertex_r;
  std::shared_ptr<const AbstractJoinPlanPredicate> a_gt_5, a_gt_8;
};

TEST_F(BasicCardinalityEstimatorTest, Basics) {
  BasicCardinalityEstimator estimator;

  EXPECT_EQ(estimator.estimate({vertex_r}, {}), 20);
  EXPECT_EQ(estimator.estimate({vertex_r}, {a_gt_5}), 10);
  EXPECT_EQ(estimator.estimate({vertex_r}, {a_gt_8}), 4);
  EXPECT_EQ(estimator.estimate({vertex_r}, {a_gt_5, a_gt_8}), 4);
}

}  // namespace opossum