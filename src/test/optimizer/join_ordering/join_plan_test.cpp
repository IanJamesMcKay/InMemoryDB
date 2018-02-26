#include "gtest/gtest.h"

#include "optimizer/join_ordering/join_plan_join_node.hpp"
#include "optimizer/join_ordering/join_plan_vertex_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "utils/load_table.hpp"
#include "storage/storage_manager.hpp"

using namespace std::string_literals;

namespace opossum {

class JoinPlanTest: public ::testing::Test {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("table_a", load_table("src/test/tables/int_int.tbl"));
    StorageManager::get().add_table("table_b", load_table("src/test/tables/int_int2.tbl"));
    StorageManager::get().add_table("table_c", load_table("src/test/tables/int.tbl"));
    
    _node_a = std::make_shared<StoredTableNode>("table_a");
    _node_b = std::make_shared<StoredTableNode>("table_b");
    _node_c = std::make_shared<StoredTableNode>("table_c");

    _node_a_a = _node_a->get_column("a"s);
    _node_b_a = _node_b->get_column("a"s);
    _node_b_b = _node_b->get_column("b"s);
    _node_c_a = _node_c->get_column("a"s);

    _predicate_b_0 = std::make_shared<JoinPlanAtomicPredicate>(_node_b_a, PredicateCondition::Equals, 5);
    _predicate_b_1 = std::make_shared<JoinPlanAtomicPredicate>(_node_b_a, PredicateCondition::Equals, _node_b_b);
    _predicate_ab = std::make_shared<JoinPlanAtomicPredicate>(_node_a_a, PredicateCondition::GreaterThanEquals, _node_b_b);

    _vertex_node_a = std::make_shared<JoinPlanVertexNode>(_node_a, std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>{});
    _vertex_node_b = std::make_shared<JoinPlanVertexNode>(_node_b, std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>{_predicate_b_0, _predicate_b_1});
    _vertex_node_c = std::make_shared<JoinPlanVertexNode>(_node_c, std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>{});

    _join_node_a = std::make_shared<JoinPlanJoinNode>(_vertex_node_a, _vertex_node_b, std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>{_predicate_ab});
    _join_node_b = std::make_shared<JoinPlanJoinNode>(_join_node_a, _vertex_node_c, std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>{});

    _join_plan = _join_node_b;
  }

  void TearDown() override {
    StorageManager::get().reset();
  }

  std::shared_ptr<AbstractJoinPlanNode> _join_plan;
  std::shared_ptr<JoinPlanVertexNode> _vertex_node_a, _vertex_node_b, _vertex_node_c;
  std::shared_ptr<JoinPlanJoinNode> _join_node_a, _join_node_b;

  std::shared_ptr<AbstractJoinPlanPredicate> _predicate_b_0, _predicate_b_1, _predicate_ab;

  std::shared_ptr<StoredTableNode> _node_a, _node_b, _node_c;

  LQPColumnReference _node_a_a, _node_b_a, _node_b_b, _node_c_a;
};

TEST_F(JoinPlanTest, ContainsVertex) {
  EXPECT_TRUE(_vertex_node_a->contains_vertex(_node_a));
  EXPECT_FALSE(_vertex_node_a->contains_vertex(_node_b));
  EXPECT_TRUE(_vertex_node_b->contains_vertex(_node_b));
  EXPECT_FALSE(_vertex_node_b->contains_vertex(_node_c));
  EXPECT_TRUE(_vertex_node_c->contains_vertex(_node_c));
  EXPECT_FALSE(_vertex_node_c->contains_vertex(_node_a));
  EXPECT_TRUE(_join_plan->contains_vertex(_node_a));
  EXPECT_TRUE(_join_plan->contains_vertex(_node_b));
  EXPECT_TRUE(_join_plan->contains_vertex(_node_c));
  EXPECT_TRUE(_join_node_a->contains_vertex(_node_a));
  EXPECT_TRUE(_join_node_a->contains_vertex(_node_b));
  EXPECT_FALSE(_join_node_a->contains_vertex(_node_c));
}

TEST_F(JoinPlanTest, FindColumnID) {
  ASSERT_TRUE(_vertex_node_a->find_column_id(_node_a_a));
  EXPECT_EQ(*_vertex_node_a->find_column_id(_node_a_a), ColumnID(0));

  EXPECT_FALSE(_vertex_node_a->find_column_id(_node_b_a));

  ASSERT_TRUE(_join_plan->find_column_id(_node_a_a));
  ASSERT_TRUE(_join_plan->find_column_id(_node_b_a));
  ASSERT_TRUE(_join_plan->find_column_id(_node_c_a));
  EXPECT_EQ(*_join_plan->find_column_id(_node_a_a), ColumnID(0));
  EXPECT_EQ(*_join_plan->find_column_id(_node_b_a), ColumnID(2));
  EXPECT_EQ(*_join_plan->find_column_id(_node_c_a), ColumnID(4));
}

TEST_F(JoinPlanTest, OutputColumnCount) {
  EXPECT_EQ(_join_plan->output_column_count(), 5u);
  EXPECT_EQ(_join_node_a->output_column_count(), 4u);
  EXPECT_EQ(_vertex_node_a->output_column_count(), 2u);
}

}  // namespace opossum