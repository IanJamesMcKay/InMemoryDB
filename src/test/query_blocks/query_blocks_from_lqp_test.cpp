#include "gtest/gtest.h"

#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "query_blocks/abstract_query_block.hpp"
#include "query_blocks/predicates_block.hpp"
#include "query_blocks/outer_join_block.hpp"
#include "query_blocks/stored_table_block.hpp"
#include "query_blocks/query_blocks_from_lqp.hpp"
#include "optimizer/join_ordering/join_plan_predicate.hpp"
#include "utils/load_table.hpp"
#include "storage/storage_manager.hpp"

using namespace std::string_literals;

namespace opossum {

class QueryBlocksFromLQPTest : public ::testing::Test {
 public:
  void SetUp() {
    StorageManager::get().add_table("int_float", load_table("src/test/tables/int_float.tbl"));
    StorageManager::get().add_table("int_float2", load_table("src/test/tables/int_float2.tbl"));

    int_float = StoredTableNode::make("int_float");
    int_float2 = StoredTableNode::make("int_float2");

    int_float_a = int_float->get_column("a"s);
    int_float_b = int_float->get_column("b"s);
    int_float2_a = int_float2->get_column("a"s);
    int_float2_b = int_float2->get_column("b"s);
  }

  std::shared_ptr<StoredTableNode> int_float, int_float2;
  LQPColumnReference int_float_a, int_float_b, int_float2_a, int_float2_b;
};

TEST_F(QueryBlocksFromLQPTest, InnerJoinSimple) {
  // clang-format off
  const auto lqp = JoinNode::make(JoinMode::Inner, LQPColumnReferencePair{int_float_a, int_float2_a}, PredicateCondition::Equals,  // NOLINT
    int_float, int_float2);
  // clang-format on

  const auto query_block = query_blocks_from_lqp(lqp);

  const auto predicates_block = std::dynamic_pointer_cast<PredicateBlock>(query_block);
  ASSERT_TRUE(predicates_block);
  ASSERT_EQ(predicates_block->predicates.size(), 1u);
  const auto predicate_a = std::dynamic_pointer_cast<JoinPlanAtomicPredicate>(predicates_block->predicates.at(0));
  ASSERT_TRUE(predicate_a);
  EXPECT_EQ(predicate_a->predicate_condition, PredicateCondition::Equals);
  EXPECT_EQ(predicate_a->left_operand, int_float_a);
  EXPECT_EQ(predicate_a->right_operand, AllParameterVariant(int_float_b));

  ASSERT_EQ(predicates_block->sub_blocks.size(), 2u);
  const auto stored_table_block_a = std::dynamic_pointer_cast<StoredTableBlock>(predicates_block->sub_blocks.at(0));
  ASSERT_TRUE(stored_table_block_a);
  EXPECT_EQ(stored_table_block_a->stored_table_node, int_float);
  const auto stored_table_block_b = std::dynamic_pointer_cast<StoredTableBlock>(predicates_block->sub_blocks.at(1));
  ASSERT_TRUE(stored_table_block_b);
  EXPECT_EQ(stored_table_block_b->stored_table_node, int_float2);
}

}  // namespace opossum
