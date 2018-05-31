#include "gtest/gtest.h"

#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "query_blocks/abstract_query_block.hpp"
#include "query_blocks/predicate_join_block.hpp"
#include "query_blocks/outer_join_block.hpp"
#include "query_blocks/stored_table_block.hpp"
#include "query_blocks/plan_block.hpp"
#include "query_blocks/query_blocks_from_lqp.hpp"
#include "optimizer/join_ordering/join_plan_predicate.hpp"
#include "utils/load_table.hpp"
#include "storage/storage_manager.hpp"

using namespace std::string_literals;

namespace opossum {

class QueryBlocksFromLQPTest : public ::testing::Test {
 public:
  void SetUp() override {
    StorageManager::get().add_table("int_float", load_table("src/test/tables/int_float.tbl"));
    StorageManager::get().add_table("int_float2", load_table("src/test/tables/int_float2.tbl"));

    int_float = StoredTableNode::make("int_float");
    int_float2 = StoredTableNode::make("int_float2");

    int_float_a = int_float->get_column("a"s);
    int_float_b = int_float->get_column("b"s);
    int_float2_a = int_float2->get_column("a"s);
    int_float2_b = int_float2->get_column("b"s);
  }

  void TearDown() override {
    StorageManager::reset();
  }

  bool has_stored_table_sub_block(const std::shared_ptr<AbstractQueryBlock>& query_block,
                     const std::shared_ptr<StoredTableNode>& node) const {
    for (const auto& sub_block : query_block->sub_blocks) {
      if (sub_block->type != QueryBlockType::StoredTable) continue;
      const auto stored_table_block = std::static_pointer_cast<StoredTableBlock>(sub_block);
      if (stored_table_block->node == node) return true;
    }
    return false;
  }

  std::string to_string(const std::shared_ptr<AbstractJoinPlanPredicate>& predicate) {
    std::stringstream stream;
    predicate->print(stream);
    return stream.str();
  }

  std::shared_ptr<StoredTableNode> int_float, int_float2;
  LQPColumnReference int_float_a, int_float_b, int_float2_a, int_float2_b;
};

TEST_F(QueryBlocksFromLQPTest, InnerJoinSimple) {
  // clang-format off
  const auto lqp =
  JoinNode::make(JoinMode::Inner, LQPColumnReferencePair{int_float_a, int_float2_a}, PredicateCondition::Equals,  // NOLINT
    int_float,
    int_float2);
  // clang-format on

  const auto query_block = query_blocks_from_lqp(lqp);

  const auto predicates_block = std::dynamic_pointer_cast<PredicateJoinBlock>(query_block);
  ASSERT_TRUE(predicates_block);
  ASSERT_EQ(predicates_block->predicates.size(), 1u);
  const auto predicate_a = std::dynamic_pointer_cast<JoinPlanAtomicPredicate>(predicates_block->predicates.at(0));
  ASSERT_TRUE(predicate_a);
  EXPECT_EQ(predicate_a->predicate_condition, PredicateCondition::Equals);
  EXPECT_EQ(predicate_a->left_operand, int_float_a);
  EXPECT_EQ(predicate_a->right_operand, AllParameterVariant(int_float2_a));

  // Test StoredTableNodes - NOTE that the order is dependant on the hash function and we therefore can't test it
  EXPECT_EQ(predicates_block->sub_blocks.size(), 2u);
  EXPECT_TRUE(has_stored_table_sub_block(predicates_block, int_float));
  EXPECT_TRUE(has_stored_table_sub_block(predicates_block, int_float2));
}

TEST_F(QueryBlocksFromLQPTest, ComplexJoinsAndPredicates) {
  //[0] [Projection] z1, y1
  // \_[1] [Predicate] y2 BETWEEN 1 AND 42
  //    \_[2] [Predicate] x2 <= z1
  //       \_[3] [Cross Join]
  //          \_[4] [Predicate] sum_a <= y1
  //          |  \_[5] [Predicate] y1 > 32
  //          |     \_[6] [Inner Join] x2 = y2
  //          |        \_[7] [Predicate] sum_a = 5
  //          |        |  \_[8] [Aggregate] SUM(x1) AS "sum_a" GROUP BY [x2]
  //          |        |     \_[9] [MockTable]
  //          |        \_[10] [Predicate] y2 < 200
  //          |           \_[11] [UnionNode] Mode: UnionPositions
  //          |              \_[12] [Predicate] y2 = 7
  //          |              |  \_[13] [Predicate] y1 = 6
  //          |              |     \_[14] [MockTable]
  //          |              \_[15] [UnionNode] Mode: UnionPositions
  //          |                 \_[16] [Predicate] y1 >= 8
  //          |                 |  \_Recurring Node --> [14]
  //          |                 \_[17] [Predicate] y2 = 9
  //          |                    \_Recurring Node --> [14]
  //          \_[18] [MockTable]

  const auto mock_node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "x1"}, {DataType::Int, "x2"}});
  const auto mock_node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "y1"}, {DataType::Int, "y2"}});
  const auto mock_node_c = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "z1"}});

  const auto mock_node_a_x1 = mock_node_a->get_column("x1"s);
  const auto mock_node_a_x2 = mock_node_a->get_column("x2"s);
  const auto mock_node_b_y1 = mock_node_b->get_column("y1"s);
  const auto mock_node_b_y2 = mock_node_b->get_column("y2"s);
  const auto mock_node_c_z1 = mock_node_c->get_column("z1"s);

  const auto sum_expression = LQPExpression::create_aggregate_function(
  AggregateFunction::Sum, {LQPExpression::create_column(mock_node_a_x1)}, "sum_a");

  const auto aggregate_node_a = AggregateNode::make(std::vector<std::shared_ptr<LQPExpression>>{sum_expression},
                                                      std::vector<LQPColumnReference>{mock_node_a_x2});
  aggregate_node_a->set_left_input(mock_node_a);

  const auto sum_mock_node_a_x1 = aggregate_node_a->get_column("sum_a"s);

  const auto predicate_node_a = PredicateNode::make(sum_mock_node_a_x1, PredicateCondition::Equals, 5);
  const auto predicate_node_b = PredicateNode::make(mock_node_b_y1, PredicateCondition::Equals, 6);
  const auto predicate_node_c = PredicateNode::make(mock_node_b_y2, PredicateCondition::Equals, 7);
  const auto predicate_node_d = PredicateNode::make(mock_node_b_y1, PredicateCondition::GreaterThanEquals, 8);
  const auto predicate_node_e = PredicateNode::make(mock_node_b_y2, PredicateCondition::LessThan, 200);
  const auto predicate_node_f = PredicateNode::make(mock_node_b_y1, PredicateCondition::GreaterThan, 32);
  const auto predicate_node_g = PredicateNode::make(sum_mock_node_a_x1, PredicateCondition::LessThanEquals, mock_node_b_y1);
  const auto predicate_node_h = PredicateNode::make(mock_node_a_x2, PredicateCondition::LessThanEquals, mock_node_c_z1);
  const auto predicate_node_i = PredicateNode::make(mock_node_b_y2, PredicateCondition::Equals, 9);
  const auto predicate_node_j = PredicateNode::make(mock_node_b_y2, PredicateCondition::Between, 1, 42);

  const auto inner_join_node_a = JoinNode::make(JoinMode::Inner, LQPColumnReferencePair{mock_node_a_x2, mock_node_b_y2}, PredicateCondition::Equals);
  const auto cross_join_node_a = std::make_shared<JoinNode>(JoinMode::Cross);

  const auto union_node_a = UnionNode::make(UnionMode::Positions);
  const auto union_node_b = UnionNode::make(UnionMode::Positions);

  const auto projection_node_a = ProjectionNode::make(std::vector<std::shared_ptr<LQPExpression>>{
  LQPExpression::create_column(mock_node_c_z1), LQPExpression::create_column(mock_node_b_y1)});

  const auto lqp = projection_node_a;

  /**
   * Wire up LQP
   */
  projection_node_a->set_left_input(predicate_node_j);
  predicate_node_j->set_left_input(predicate_node_h);
  predicate_node_h->set_left_input(cross_join_node_a);
  cross_join_node_a->set_left_input(predicate_node_g);
  cross_join_node_a->set_right_input(mock_node_c);
  predicate_node_g->set_left_input(predicate_node_f);
  predicate_node_f->set_left_input(inner_join_node_a);
  inner_join_node_a->set_left_input(predicate_node_a);
  inner_join_node_a->set_right_input(predicate_node_e);
  predicate_node_e->set_left_input(union_node_a);
  union_node_a->set_left_input(predicate_node_c);
  union_node_a->set_right_input(union_node_b);
  predicate_node_c->set_left_input(predicate_node_b);
  predicate_node_b->set_left_input(mock_node_b);
  union_node_b->set_left_input(predicate_node_d);
  union_node_b->set_right_input(predicate_node_i);
  predicate_node_d->set_left_input(mock_node_b);
  predicate_node_i->set_left_input(mock_node_b);
  predicate_node_a->set_left_input(aggregate_node_a);

  /**
   * Actual tests
   */
  const auto query_blocks_root = query_blocks_from_lqp(lqp);  // NOLINT

  const auto plan_block_a = std::dynamic_pointer_cast<PlanBlock>(query_blocks_root);
  ASSERT_TRUE(plan_block_a);
  ASSERT_EQ(plan_block_a->sub_blocks.size(), 1u);

  const auto predicate_join_block = std::dynamic_pointer_cast<PredicateJoinBlock>(plan_block_a->sub_blocks.at(0));
  ASSERT_TRUE(predicate_join_block);

  ASSERT_EQ(predicate_join_block->sub_blocks.size(), 3u);

  const auto stored_table_block_a = std::dynamic_pointer_cast<StoredTableBlock>(predicate_join_block->sub_blocks.at(0));
  ASSERT_TRUE(stored_table_block_a);

  const auto plan_block_b = std::dynamic_pointer_cast<PlanBlock>(predicate_join_block->sub_blocks.at(1));
  ASSERT_TRUE(plan_block_b);
  ASSERT_EQ(plan_block_b->sub_blocks.size(), 1u);
  ASSERT_EQ(plan_block_b->lqp, aggregate_node_a);
  EXPECT_EQ(plan_block_b->lqp->left_input(), nullptr);

  const auto stored_table_block_b = std::dynamic_pointer_cast<StoredTableBlock>(predicate_join_block->sub_blocks.at(2));
  ASSERT_TRUE(stored_table_block_b);

  const auto stored_table_block_c = std::dynamic_pointer_cast<StoredTableBlock>(plan_block_b->sub_blocks.at(0));
  ASSERT_TRUE(stored_table_block_c);

  /**
   * Look for the predicates in the PredicateJoinBlock
   */
  ASSERT_EQ(predicate_join_block->predicates.size(), 9u);
  EXPECT_EQ(to_string(predicate_join_block->predicates.at(0)), "y2 >= 1");
  EXPECT_EQ(to_string(predicate_join_block->predicates.at(1)), "x2 <= z1");
  EXPECT_EQ(to_string(predicate_join_block->predicates.at(2)), "sum_a <= y1");
  EXPECT_EQ(to_string(predicate_join_block->predicates.at(3)), "x2 = y2");
  EXPECT_EQ(to_string(predicate_join_block->predicates.at(4)), "y2 < 200");
  EXPECT_EQ(to_string(predicate_join_block->predicates.at(5)), "y1 > 32");
  EXPECT_EQ(to_string(predicate_join_block->predicates.at(6)), "sum_a = 5");
  EXPECT_EQ(to_string(predicate_join_block->predicates.at(7)), "y2 <= 42");
  EXPECT_EQ(to_string(predicate_join_block->predicates.at(8)), "(y2 = 7 AND y1 = 6) OR (y1 >= 8 OR y2 = 9)");

  /**
   * Test the StoredTableBlocks
   */
  EXPECT_EQ(stored_table_block_a->node, mock_node_b);
  EXPECT_EQ(stored_table_block_b->node, mock_node_c);
  EXPECT_EQ(stored_table_block_c->node, mock_node_a);

  /**
   * plan_block_b
   */
}

}  // namespace opossum
