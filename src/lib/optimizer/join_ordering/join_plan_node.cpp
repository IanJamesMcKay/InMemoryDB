#include "join_plan_node.hpp"

namespace opossum {

JoinPlanNode::JoinPlanNode(const std::shared_ptr<AbstractLQPNode>& lqp, const Cost plan_cost, const BaseJoinGraph& join_graph):
  lqp(lqp), plan_cost(plan_cost), join_graph(join_graph)
{

}

}  // namespace opossum
