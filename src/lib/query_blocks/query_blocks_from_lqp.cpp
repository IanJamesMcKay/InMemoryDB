#include "query_blocks_from_lqp.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "query_blocks/predicates_block.hpp"
#include "query_blocks/stored_table_block.hpp"
#include "query_blocks/outer_join_block.hpp"
#include "query_blocks/aggregate_block.hpp"

namespace {

using namespace opossum;

void traverse_for_predicates(const std::shared_ptr<AbstractLQPNode>& lqp,
                             std::vector<std::shared_ptr<AbstractQueryBlock>>& sub_blocks,
                             std::vector<std::shared_ptr<AbstractJoinPlanPredicate>>& predicates) {

}

std::shared_ptr<PredicateBlock> build_predicate_block(const std::shared_ptr<AbstractLQPNode>& lqp) {
  std::vector<std::shared_ptr<AbstractQueryBlock>> sub_blocks;
  std::vector<std::shared_ptr<AbstractJoinPlanPredicate>> predicates;

  traverse_for_predicates(lqp, sub_blocks, predicates);

  return std::make_shared<PredicateBlock>(sub_blocks, predicates);
}

}  // namespace

namespace opossum {

std::shared_ptr<AbstractQueryBlock> query_blocks_from_lqp(const std::shared_ptr<AbstractLQPNode>& lqp) {
  switch (lqp->type()) {
    case LQPNodeType::Predicate:
      build_predicate_block(lqp);
  }
}

}  // namespace opossum
