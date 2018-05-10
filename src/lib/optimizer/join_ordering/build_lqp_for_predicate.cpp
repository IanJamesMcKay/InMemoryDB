#include "build_lqp_for_predicate.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "join_plan_predicate.hpp"
#include "join_plan_node.hpp"

namespace opossum {

std::shared_ptr<AbstractLQPNode> build_lqp_for_predicate(const AbstractJoinPlanPredicate& predicate, const std::shared_ptr<AbstractLQPNode>& input_node) {
  switch (predicate.type()) {
    case JoinPlanPredicateType::Atomic: {
      const auto& atomic_predicate = static_cast<const JoinPlanAtomicPredicate&>(predicate);

      return PredicateNode::make(atomic_predicate.left_operand,
                                 atomic_predicate.predicate_condition,
                                 atomic_predicate.right_operand,
                                 input_node);
    }
    case JoinPlanPredicateType::LogicalOperator: {
      const auto& logical_operator_predicate = static_cast<const JoinPlanLogicalPredicate&>(predicate);
      const auto left = build_lqp_for_predicate(*logical_operator_predicate.left_operand, input_node);

      switch (logical_operator_predicate.logical_operator) {
        case JoinPlanPredicateLogicalOperator::And: {
          return build_lqp_for_predicate(*logical_operator_predicate.right_operand, left);
        }
        case JoinPlanPredicateLogicalOperator::Or: {
          auto right = build_lqp_for_predicate(*logical_operator_predicate.right_operand, input_node);
          return UnionNode::make(UnionMode::Positions, left, right);
        }
      }
    }
  }

  Fail("Should be unreachable, but clang doesn't realize");
}

}