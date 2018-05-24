#include "query_blocks_from_lqp.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "query_blocks/predicates_block.hpp"
#include "query_blocks/stored_table_block.hpp"
#include "query_blocks/outer_join_block.hpp"
#include "query_blocks/aggregate_block.hpp"
#include "utils/assert.hpp"

namespace {

using namespace opossum;

/**
 * A subgraph in the LQP consisting of UnionNodes and PredicateNodes can be translated into a single complex predicate
 *
 *         ______________UnionNode________
 *        /                               \
 *    PredicateNode(a > 5)            PredicateNode(a < 3)
 *       |                                 |
 *       |                            PredicateNode(b > 12)
 *       \____________AggregateNode_______/
 *                           |
 *                         [...]
 *
 *  Represents the JoinPlanPredicate "a > 5 OR (a < 3 AND b > 12)". The AggregateNode is the Predicate's "base_node"
 *  i.e. the node on which's output the Predicate operates on.
 *
 *  _parse_predicate() and _parse_union() perform this conversion, calling each other recursively
 */
struct PredicateParseResult {
  PredicateParseResult(const std::shared_ptr<AbstractLQPNode>& base_node,
                       const std::shared_ptr<const AbstractJoinPlanPredicate>& predicate)
  : base_node(base_node), predicate(predicate) {}

  std::shared_ptr<AbstractLQPNode> base_node;
  std::shared_ptr<const AbstractJoinPlanPredicate> predicate;
};

PredicateParseResult gather_predicate_from_union(const std::shared_ptr<UnionNode>& union_node);

bool is_predicate_block_node(const LQPNodeType node_type) {
  return node_type != LQPNodeType::Join && node_type != LQPNodeType::Union && node_type != LQPNodeType::Predicate;
}

PredicateParseResult gather_predicate(
const std::shared_ptr<AbstractLQPNode> &node) {
  if (node->type() == LQPNodeType::Predicate) {
    const auto predicate_node = std::static_pointer_cast<const PredicateNode>(node);

    std::shared_ptr<const AbstractJoinPlanPredicate> left_predicate;

    if (predicate_node->value2()) {
      DebugAssert(predicate_node->predicate_condition() == PredicateCondition::Between, "Expected between");

      left_predicate = std::make_shared<JoinPlanLogicalPredicate>(
      std::make_shared<JoinPlanAtomicPredicate>(predicate_node->column_reference(),
                                                PredicateCondition::GreaterThanEquals, predicate_node->value()),
      JoinPlanPredicateLogicalOperator::And,
      std::make_shared<JoinPlanAtomicPredicate>(predicate_node->column_reference(),
                                                PredicateCondition::LessThanEquals, *predicate_node->value2()));
    } else {
      left_predicate = std::make_shared<JoinPlanAtomicPredicate>(
      predicate_node->column_reference(), predicate_node->predicate_condition(), predicate_node->value());
    }

    const auto base_node = predicate_node->left_input();

    if (base_node->output_count() > 1) {
      return {base_node, left_predicate};
    } else {
      const auto parse_result_right = gather_predicate(base_node);

      const auto and_predicate = std::make_shared<JoinPlanLogicalPredicate>(
      left_predicate, JoinPlanPredicateLogicalOperator::And, parse_result_right.predicate);

      return {parse_result_right.base_node, and_predicate};
    }
  } else if (node->type() == LQPNodeType::Union) {
    return gather_predicate_from_union(std::static_pointer_cast<UnionNode>(node));
  } else {
    Assert(node->left_input() && !node->right_input(), "");
    return gather_predicate(node->left_input());
  }
}

PredicateParseResult gather_predicate_from_union(
const std::shared_ptr<UnionNode>& union_node) {
  DebugAssert(union_node->left_input() && union_node->right_input(),
              "UnionNode needs both inputs set in order to be parsed");

  const auto parse_result_left = gather_predicate(union_node->left_input());
  const auto parse_result_right = gather_predicate(union_node->right_input());

  DebugAssert(parse_result_left.base_node == parse_result_right.base_node, "Invalid OR not having a single base node");

  const auto or_predicate = std::make_shared<JoinPlanLogicalPredicate>(
  parse_result_left.predicate, JoinPlanPredicateLogicalOperator::Or, parse_result_right.predicate);

  return {parse_result_left.base_node, or_predicate};
}

void gather_predicate_block(const std::shared_ptr<AbstractLQPNode>& node,
                            std::vector<std::shared_ptr<AbstractQueryBlock>>& sub_blocks,
                            std::vector<std::shared_ptr<AbstractJoinPlanPredicate>>& predicates) {
  // Makes it possible to call traverse() on inputs without checking whether they exist first.
  if (!node) {
    return;
  }

  if (is_predicate_block_node(node->type())) {
    sub_blocks.emplace_back(query_blocks_from_lqp(node));
    return;
  }

  switch (node->type()) {
    case LQPNodeType::Join: {
      /**
       * Cross joins are simply being traversed past. Outer joins are hard to address during JoinOrdering and until we
       * do, outer join predicates are not included in the JoinGraph.
       * The outer join node is added as a vertex and traversal stops at this point.
       */

      const auto join_node = std::static_pointer_cast<JoinNode>(node);

      if (join_node->join_mode() == JoinMode::Inner) {
        predicates.emplace_back(std::make_shared<JoinPlanAtomicPredicate>(
        join_node->join_column_references()->first, *join_node->predicate_condition(),
        join_node->join_column_references()->second));
      }

      if (join_node->join_mode() == JoinMode::Inner || join_node->join_mode() == JoinMode::Cross) {
        gather_predicate_block(node->left_input(), sub_blocks, predicates);
        gather_predicate_block(node->right_input(), sub_blocks, predicates);
      } else {
        sub_blocks.emplace_back(query_blocks_from_lqp(node));
      }
    } break;

    case LQPNodeType::Predicate: {
      /**
       * BETWEEN PredicateNodes are turned into two predicates, because JoinPlanPredicates do not support BETWEEN. All
       * other PredicateConditions produce exactly one JoinPlanPredicate
       */

      const auto predicate_node = std::static_pointer_cast<PredicateNode>(node);

      if (predicate_node->value2()) {
        DebugAssert(predicate_node->predicate_condition() == PredicateCondition::Between, "Expected between");

        predicates.emplace_back(std::make_shared<JoinPlanAtomicPredicate>(
        predicate_node->column_reference(), PredicateCondition::GreaterThanEquals, predicate_node->value()));

        predicates.emplace_back(std::make_shared<JoinPlanAtomicPredicate>(
        predicate_node->column_reference(), PredicateCondition::LessThanEquals, *predicate_node->value2()));
      } else {
        predicates.emplace_back(std::make_shared<JoinPlanAtomicPredicate>(
        predicate_node->column_reference(), predicate_node->predicate_condition(), predicate_node->value()));
      }

      gather_predicate_block(node->left_input(), sub_blocks, predicates);
    } break;

    case LQPNodeType::Union: {
      /**
       * A UnionNode is the entry point to disjunction, which is parsed starting from _parse_union(). Normal traversal
       * is commenced from the node "below" the Union.
       */

      const auto union_node = std::static_pointer_cast<UnionNode>(node);

      if (union_node->union_mode() == UnionMode::Positions) {
        const auto parse_result = gather_predicate_from_union(union_node);

        gather_predicate_block(parse_result.base_node, sub_blocks, predicates);
        predicates.emplace_back(parse_result.predicate);
      } else {
        sub_blocks.emplace_back(query_blocks_from_lqp(node));
      }
    } break;

    default: { Fail("Node type not suited for JoinGraph"); }
  }
}

std::shared_ptr<PredicateBlock> build_predicate_block(const std::shared_ptr<AbstractLQPNode>& lqp) {
  std::vector<std::shared_ptr<AbstractQueryBlock>> sub_blocks;
  std::vector<std::shared_ptr<AbstractJoinPlanPredicate>> predicates;

  gather_predicate_block(lqp, sub_blocks, predicates);

  return std::make_shared<PredicateBlock>(sub_blocks, predicates);
}

std::shared_ptr<PredicateBlock> build_outer_join_block(const std::shared_ptr<AbstractLQPNode>& lqp) {
  return {};
}

std::shared_ptr<PredicateBlock> build_aggregate_block(const std::shared_ptr<AbstractLQPNode>& lqp) {
  return {};
}

std::shared_ptr<PredicateBlock> build_stored_table_block(const std::shared_ptr<AbstractLQPNode>& lqp) {
  return {};
}

std::shared_ptr<PredicateBlock> build_finalizing_block(const std::shared_ptr<AbstractLQPNode>& lqp) {
  return {};
}

}  // namespace

namespace opossum {

std::shared_ptr<AbstractQueryBlock> query_blocks_from_lqp(const std::shared_ptr<AbstractLQPNode>& lqp) {
  switch (lqp->type()) {
    case LQPNodeType::Predicate: case LQPNodeType::Union: return build_predicate_block(lqp);
    case LQPNodeType::Join: {
      const auto join_node = std::static_pointer_cast<JoinNode>(lqp);

      switch (join_node->join_mode()) {
        case JoinMode::Inner: case JoinMode::Cross:
          return build_predicate_block(lqp);
        default:
          return build_outer_join_block(lqp);
      }
    }
    case LQPNodeType::Sort: case LQPNodeType::Projection: case LQPNodeType::Limit: return build_finalizing_block(lqp);
    case LQPNodeType::StoredTable: return build_stored_table_block(lqp);
    case LQPNodeType::Aggregate: return build_aggregate_block(lqp);
    case LQPNodeType::Validate:
      Assert(lqp->left_input() && !lqp->right_input(), "Validate is expected to have a left input")
      return query_blocks_from_lqp(lqp->left_input());


    default:
      Fail("Couldn't turn LQP into QueryBlock");
  }

}

}  // namespace opossum
