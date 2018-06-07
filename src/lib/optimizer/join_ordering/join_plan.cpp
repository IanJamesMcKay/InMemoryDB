#include "join_plan.hpp"

namespace opossum {

JoinPlan::JoinPlan(const std::shared_ptr<AbstractLQPNode>& lqp,
         const std::shared_ptr<PredicateJoinBlock>& query_block,
         const Cost plan_cost) {}

}  // namespace opossum
