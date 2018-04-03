#include "join_plan_node.hpp"

namespace opossum {

JoinPlanNode::JoinPlanNode(const std::shared_ptr<AbstractLQPNode>& lqp, const Cost plan_cost):
  lqp(lqp), plan_cost(plan_cost)
{

}

}  // namespace opossum
