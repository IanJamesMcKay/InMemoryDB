#include "gtest/gtest.h"

#include "storage/storage_manager.hpp"
#include "optimizer/join_ordering/join_plan_predicate.hpp"
#include "statistics/cardinality_estimation_cache.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "utils/load_table.hpp"

using namespace std::string_literals;

namespace opossum {

class CardinalityEstimationCacheTest : public ::testing::Test {
 public:
  void SetUp() override {
    StorageManager::get().add_table("int_float", load_table("src/test/tables/int_float.tbl"));
    StorageManager::get().add_table("int_float2", load_table("src/test/tables/int_float2.tbl"));

    int_float = StoredTableNode::make("int_float");
    int_float->set_alias("some_alias_for_int_float");
    int_float2 = StoredTableNode::make("int_float2");

    int_float_a = int_float->get_column("a"s);
    int_float_b = int_float->get_column("b"s);
    int_float2_a = int_float2->get_column("a"s);
    int_float2_b = int_float2->get_column("b"s);

    int_float_a_eq_int_float_b = std::make_shared<const JoinPlanAtomicPredicate>(
      int_float_a, PredicateCondition::Equals, int_float_b
    );
    int_float_a_gt_int_float2_b = std::make_shared<const JoinPlanAtomicPredicate>(
      int_float_a, PredicateCondition::GreaterThan, int_float2_b
    );
    int_float_a_eq_five = std::make_shared<const JoinPlanAtomicPredicate>(
      int_float_a, PredicateCondition::Equals, 5
    );
    int_float_b_eq_hello = std::make_shared<const JoinPlanAtomicPredicate>(
      int_float_b, PredicateCondition::Equals, "Hello"
    );

    p1_and_p2_or_p3 = std::make_shared<const JoinPlanLogicalPredicate>(
      int_float_a_eq_int_float_b, JoinPlanPredicateLogicalOperator::And,
      std::make_shared<const JoinPlanLogicalPredicate>(
        int_float_a_eq_five, JoinPlanPredicateLogicalOperator::Or, int_float_a_gt_int_float2_b
      )
    );

    cache = std::make_shared<CardinalityEstimationCache>();
  }

  void TearDown() override {
    StorageManager::reset();
  }

  std::shared_ptr<StoredTableNode> int_float, int_float2;
  LQPColumnReference int_float_a, int_float_b, int_float2_a, int_float2_b;
  std::shared_ptr<const AbstractJoinPlanPredicate> int_float_a_eq_int_float_b;
  std::shared_ptr<const AbstractJoinPlanPredicate> int_float_a_gt_int_float2_b;
  std::shared_ptr<const AbstractJoinPlanPredicate> int_float_a_eq_five;
  std::shared_ptr<const AbstractJoinPlanPredicate> int_float_b_eq_hello;
  std::shared_ptr<const AbstractJoinPlanPredicate> p1_and_p2_or_p3;
  std::shared_ptr<CardinalityEstimationCache> cache;
};

TEST_F(CardinalityEstimationCacheTest, Json) {
  cache->put(BaseJoinGraph{{int_float}, {int_float_a_eq_int_float_b}}, 13);
  cache->put(BaseJoinGraph{{int_float2, int_float}, {int_float_a_eq_int_float_b, int_float_a_gt_int_float2_b}}, 12);
  cache->put(BaseJoinGraph{{int_float2, int_float}, {int_float_a_eq_five, int_float_a_eq_int_float_b, int_float_a_gt_int_float2_b}}, 11);
  cache->put(BaseJoinGraph{{int_float2, int_float}, {p1_and_p2_or_p3}}, 19);
  cache->put(BaseJoinGraph{{int_float2, int_float}, {p1_and_p2_or_p3, int_float_b_eq_hello}}, 15);
  cache->set_timeout(BaseJoinGraph{{int_float2, int_float}, {p1_and_p2_or_p3, int_float_a_eq_int_float_b}}, std::chrono::seconds{25});

  const auto json = cache->to_json();
  const auto cache_b = CardinalityEstimationCache::from_json(json);

  /** */


  EXPECT_EQ(cache_b->get(BaseJoinGraph{{int_float}, {int_float_a_eq_int_float_b}}), 13);
  EXPECT_EQ(cache_b->get(BaseJoinGraph{{int_float, int_float2}, {int_float_a_gt_int_float2_b, int_float_a_eq_int_float_b}}), 12);
  EXPECT_EQ(cache_b->get(BaseJoinGraph{{int_float2, int_float}, {int_float_a_eq_int_float_b, int_float_a_eq_five, int_float_a_gt_int_float2_b}}), 11);
  EXPECT_EQ(cache_b->get(BaseJoinGraph{{int_float2, int_float}, {p1_and_p2_or_p3}}), 19);
  EXPECT_EQ(cache_b->get(BaseJoinGraph{{int_float2, int_float}, {p1_and_p2_or_p3, int_float_b_eq_hello}}), 15);
  EXPECT_EQ(cache_b->get_timeout(BaseJoinGraph{{int_float2, int_float}, {p1_and_p2_or_p3, int_float_b_eq_hello}}), std::nullopt);
  EXPECT_EQ(cache_b->get(BaseJoinGraph{{int_float2, int_float}, {p1_and_p2_or_p3, int_float_a_eq_int_float_b}}), std::nullopt);
  EXPECT_EQ(cache_b->get_timeout(BaseJoinGraph{{int_float2, int_float}, {p1_and_p2_or_p3, int_float_a_eq_int_float_b}}), std::chrono::seconds{25});
}


}  // namespace opossum
