#pragma once

#include <memory>
#include <vector>

#include "join_plan.hpp"

namespace opossum {

class AbstractCardinalityEstimator;
class AbstractCostModel;
class AbstractJoinPlanNode;
class AbstractLQPNode;
class AbstractJoinPlanPredicate;
class JoinPlanJoinNode;
class JoinPlanVertexNode;

JoinPlan build_join_plan_join_node(
    const AbstractCostModel& cost_model,
    const JoinPlan& left_input,
    const JoinPlan& right_input,
    const std::vector<std::shared_ptr<AbstractJoinPlanPredicate>>& predicates,
    const AbstractCardinalityEstimator& cardinality_estimator);

JoinPlan build_join_plan_vertex_node(
    const AbstractCostModel& cost_model,
    const std::shared_ptr<AbstractLQPNode>& vertex_node,
    const std::vector<std::shared_ptr<AbstractJoinPlanPredicate>>& predicates,
    const AbstractCardinalityEstimator& cardinality_estimator);

}  // namespace opossum
