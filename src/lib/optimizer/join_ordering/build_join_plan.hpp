#pragma once

#include <memory>
#include <vector>

namespace opossum {

class AbstractCostModel;
class AbstractJoinPlanNode;
class AbstractLQPNode;
class AbstractJoinPlanPredicate;
class JoinPlanJoinNode;
class JoinPlanVertexNode;

std::shared_ptr<JoinPlanJoinNode> build_join_plan_join_node(
    const AbstractCostModel& cost_model,
    const std::shared_ptr<const AbstractJoinPlanNode>& left_child,
    const std::shared_ptr<const AbstractJoinPlanNode>& right_child,
    const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates);

std::shared_ptr<JoinPlanVertexNode> build_join_plan_vertex_node(
    const AbstractCostModel& cost_model,
    const std::shared_ptr<AbstractLQPNode>& vertex_node,
    std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> predicates);

}  // namespace opossum
