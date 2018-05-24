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

  bool has_stored_table_sub_block(const std::shared_ptr<AbstractQueryBlock>& query_block,
                     const std::shared_ptr<StoredTableNode>& node) const {
    for (const auto& sub_block : query_block->sub_blocks) {
      if (sub_block->type != QueryBlockType::StoredTable) continue;
      const auto stored_table_block = std::static_pointer_cast<StoredTableBlock>(sub_block);
      if (stored_table_block->node == node) return true;
    }
    return false;
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
  EXPECT_EQ(predicate_a->right_operand, AllParameterVariant(int_float2_a));

  // Test StoredTableNodes - NOTE that the order is dependant on the hash function and we therefore can't test it
  EXPECT_EQ(predicates_block->sub_blocks.size(), 2u);
  EXPECT_TRUE(has_stored_table_sub_block(predicates_block, int_float));
  EXPECT_TRUE(has_stored_table_sub_block(predicates_block, int_float2));
}

}  // namespace opossum
