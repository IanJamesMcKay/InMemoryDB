#include "gtest/gtest.h"

#include <memory>
#include <string>

#include "logical_query_plan/mock_node.hpp"
#include "optimizer/join_ordering/join_plan_predicate.hpp"
#include "statistics/cardinality_estimator_cached.hpp"
#include "statistics/cardinality_estimation_cache.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

class CardinalityEstimatorDummy : public AbstractCardinalityEstimator {
 public:
  Cardinality estimate(const std::vector<std::shared_ptr<AbstractLQPNode>>& relations,
                       const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates) const {
    return 42;
  }
};

class CardinalityEstimatorCachedTest : public ::testing::Test {
 public:
  void SetUp() override {
    vertex_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a0"}, {DataType::Int, "a1"}});
    vertex_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "b0"}});

    a0 = vertex_a->get_column("a0"s);
    a1 = vertex_a->get_column("a1"s);
    b0 = vertex_b->get_column("b0"s);

    a0_eq_b0 = std::make_shared<JoinPlanAtomicPredicate>(a0, PredicateCondition::Equals, b0);
    b0_eq_a0 = std::make_shared<JoinPlanAtomicPredicate>(b0, PredicateCondition::Equals, a0);
    a0_gt_b0 = std::make_shared<JoinPlanAtomicPredicate>(a0, PredicateCondition::GreaterThan, b0);
    b0_lt_a0 = std::make_shared<JoinPlanAtomicPredicate>(b0, PredicateCondition::LessThan, a0);

    cardinality_estimation_cache = std::make_shared<CardinalityEstimationCache>();
    const auto fallback_cardinality_estimator = std::make_shared<CardinalityEstimatorDummy>();
    cardinality_estimator = std::make_shared<CardinalityEstimatorCached>(cardinality_estimation_cache,
    CardinalityEstimationCacheMode::ReadOnly, fallback_cardinality_estimator);
  }

  std::shared_ptr<MockNode> vertex_a, vertex_b;
  LQPColumnReference a0, a1, b0;
  std::shared_ptr<AbstractJoinPlanPredicate> b0_eq_a0, a0_eq_b0, b0_lt_a0, a0_gt_b0;

  std::shared_ptr<CardinalityEstimationCache> cardinality_estimation_cache;
  std::shared_ptr<CardinalityEstimatorCached> cardinality_estimator;
};

TEST_F(CardinalityEstimatorCachedTest, Cache) {
  /**
   * put()
   */
  cardinality_estimation_cache->put({{vertex_a}, {}}, 5);
  EXPECT_EQ(cardinality_estimation_cache->size(), 1u);

  cardinality_estimation_cache->put({{vertex_b}, {}}, 33);
  EXPECT_EQ(cardinality_estimation_cache->size(), 2u);

  cardinality_estimation_cache->put({{vertex_a, vertex_b}, {}}, 165);
  EXPECT_EQ(cardinality_estimation_cache->size(), 3u);

  cardinality_estimation_cache->put({{vertex_a, vertex_b}, {a0_eq_b0}}, 22);
  EXPECT_EQ(cardinality_estimation_cache->size(), 4u);

  cardinality_estimation_cache->put({{vertex_a, vertex_b}, {a0_gt_b0}}, 100);
  EXPECT_EQ(cardinality_estimation_cache->size(), 5u);

  /**
   * get() - including shuffled argument order
   */
  EXPECT_EQ(cardinality_estimation_cache->get({{vertex_a, vertex_b}, {}}), 165u);
  EXPECT_EQ(cardinality_estimation_cache->get({{vertex_b, vertex_a}, {}}), 165u);
  EXPECT_EQ(cardinality_estimation_cache->get({{vertex_a}, {}}), 5u);
  EXPECT_EQ(cardinality_estimation_cache->get({{vertex_b}, {}}), 33u);
  EXPECT_EQ(cardinality_estimation_cache->get({{vertex_a, vertex_b}, {a0_gt_b0}}), 100u);
  EXPECT_EQ(cardinality_estimation_cache->get({{vertex_b, vertex_a}, {a0_gt_b0}}), 100u);
  EXPECT_EQ(cardinality_estimation_cache->get({{vertex_a, vertex_b}, {a0_eq_b0}}), 22u);
  EXPECT_EQ(cardinality_estimation_cache->get({{vertex_b, vertex_a}, {a0_eq_b0}}), 22u);

  EXPECT_EQ(cardinality_estimation_cache->get({{},{}}), std::nullopt);
  EXPECT_EQ(cardinality_estimation_cache->get({{vertex_a},{a0_gt_b0}}), std::nullopt);
  EXPECT_EQ(cardinality_estimation_cache->get({{vertex_a},{a0_gt_b0}}), std::nullopt);

  /**
   * get() - with shuffled predicates (e.g. a > 5 --> 5 < a)
   */
  EXPECT_EQ(cardinality_estimation_cache->get({{vertex_a, vertex_b}, {b0_lt_a0}}), 100u);
  EXPECT_EQ(cardinality_estimation_cache->get({{vertex_a, vertex_b}, {b0_eq_a0}}), 22u);
}

TEST_F(CardinalityEstimatorCachedTest, EmptyCache) {
  EXPECT_EQ(cardinality_estimator->estimate({}, {}).value(), 42);
  EXPECT_EQ(cardinality_estimator->estimate({vertex_a}, {}).value(), 42);
  EXPECT_EQ(cardinality_estimator->estimate({vertex_a, vertex_b}, {a0_eq_b0}).value(), 42);

  cardinality_estimation_cache->put({{vertex_a}, {}}, 5);
  EXPECT_EQ(cardinality_estimator->estimate({vertex_a}, {}).value(), 5);
  EXPECT_EQ(cardinality_estimator->estimate({vertex_a, vertex_b}, {a0_eq_b0}).value(), 42);
}

}  // namespace opossum
