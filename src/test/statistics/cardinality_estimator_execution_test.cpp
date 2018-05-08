#include "gtest/gtest.h"

#include <string>

#include "optimizer/join_ordering/join_plan_predicate.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "statistics/cardinality_estimator_execution.hpp"
#include "utils/load_table.hpp"
#include "storage/storage_manager.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

class CardinalityEstimatorExecutionTest : public ::testing::Test {
 public:
  void SetUp() override {
    StorageManager::get().add_table("lineitem", load_table("src/test/tables/tpch/sf-0.001/lineitem.tbl"));
    lineitem_node = StoredTableNode::make("lineitem");

    l_orderkey = lineitem_node->get_column("l_orderkey"s);
    l_partkey = lineitem_node->get_column("l_partkey"s);

    cardinality_estimator = std::make_shared<CardinalityEstimatorExecution>();

    l_orderkey_gt_100 = std::make_shared<JoinPlanAtomicPredicate>(l_orderkey, PredicateCondition::GreaterThan, 100);
    l_partkey_lt_50 = std::make_shared<JoinPlanAtomicPredicate>(l_partkey, PredicateCondition::LessThan, 50);
  }

  void TearDown() override {
    StorageManager::reset();
  }

  std::shared_ptr<AbstractLQPNode> lineitem_node;
  std::shared_ptr<CardinalityEstimatorExecution> cardinality_estimator;
  LQPColumnReference l_orderkey, l_partkey;

  std::shared_ptr<AbstractJoinPlanPredicate> l_orderkey_gt_100, l_partkey_lt_50;
};

TEST_F(CardinalityEstimatorExecutionTest, Basics) {
  EXPECT_EQ(cardinality_estimator->estimate({lineitem_node}, {}), 6005);
  EXPECT_EQ(cardinality_estimator->estimate({lineitem_node}, {l_orderkey_gt_100}), 5895);
}

TEST_F(CardinalityEstimatorExecutionTest, DoesntAffectLQP) {
  // Test that running the CardinalityEstimatorExecution on vertices doesn't destroy the LQP(s) they are in and also
  // that the execution leaves no traces (tangling pointers) in the existing LQP(s)

  const auto existing_predicate_node_a = PredicateNode::make(l_orderkey, PredicateCondition::LessThan, 200, lineitem_node);
  const auto existing_predicate_node_b = PredicateNode::make(l_partkey, PredicateCondition::Equals, 50, existing_predicate_node_a);

  cardinality_estimator->estimate({lineitem_node}, {l_orderkey_gt_100, l_partkey_lt_50});
  EXPECT_EQ(existing_predicate_node_b->left_input(), existing_predicate_node_a);
  EXPECT_EQ(existing_predicate_node_a->left_input(), lineitem_node);
  EXPECT_EQ(lineitem_node->outputs().at(0), existing_predicate_node_a);
}

}  // namespace opossum
