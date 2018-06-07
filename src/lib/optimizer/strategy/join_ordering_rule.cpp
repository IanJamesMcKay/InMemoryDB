#include "join_ordering_rule.hpp"

#include "optimizer/join_ordering/abstract_join_ordering_algorithm.hpp"
#include "optimizer/join_ordering/dp_ccp.hpp"
#include "optimizer/join_ordering/join_graph_builder.hpp"
#include "query_blocks/abstract_query_block.hpp"
#include "query_blocks/query_blocks_from_lqp.hpp"
#include "query_blocks/predicate_join_block.hpp"

namespace opossum {

JoinOrderingRule::JoinOrderingRule(const std::shared_ptr<AbstractJoinOrderingAlgorithm>& join_ordering_algorithm)
    : _join_ordering_algorithm(join_ordering_algorithm) {}

std::string JoinOrderingRule::name() const { return "Join Ordering Rule"; }

bool JoinOrderingRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root) {
  Assert(root->type() == LQPNodeType::Root, "Call JoinOrderingRule with LogicalQueryPlanRootNode");

  const auto root_block = query_blocks_from_lqp(root->left_input());

  const auto optimized_lqp = _apply_to_blocks(root_block);
  root->set_left_input(optimized_lqp);
}

std::shared_ptr<AbstractLQPNode> JoinOrderingRule::_apply_to_blocks(const std::shared_ptr<AbstractQueryBlock>& block) {
  auto sub_block_lqps = std::vector<std::shared_ptr<AbstractLQPNode>>{};
  sub_block_lqps.resize(block->sub_blocks.size());

  for (const auto& sub_block : block->sub_blocks) {
    sub_block_lqps.emplace_back(_apply_to_blocks(sub_block));
  }

  switch (block->type) {
    case QueryBlockType::Predicates:
      return _apply_to_predicate_block(std::static_pointer_cast<PredicateBlock>(block), sub_block_lqps);
    case QueryBlockType::StoredTable:
      return std::static_pointer_cast<StoredTableBlock>(block)->stored_table_node;

    default:
      Fail("Not yet implemented");
  }

}

std::shared_ptr<AbstractLQPNode> JoinOrderingRule::_apply_to_predicate_block(
  const std::shared_ptr<PredicateBlock>& predicate_block,
  const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices) {

  const auto join_graph = JoinGraph::from_vertices_and_predicates(vertices, predicate_block->predicates);
  const auto join_plan = (*_join_ordering_algorithm)(join_graph);
  return join_plan.lqp;
}

}  // namespace opossum
