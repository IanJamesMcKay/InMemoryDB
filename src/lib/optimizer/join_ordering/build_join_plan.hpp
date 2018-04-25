#pragma once

#include <memory>
#include <vector>

#include "join_plan_node.hpp"

namespace opossum {

class AbstractCardinalityEstimator;
class AbstractCostModel;
class AbstractJoinPlanNode;
class AbstractLQPNode;
class AbstractJoinPlanPredicate;
class JoinPlanJoinNode;
class JoinPlanVertexNode;

JoinPlanNode build_join_plan_join_node(
    const AbstractCostModel& cost_model,
    const JoinPlanNode& left_input,
    const JoinPlanNode& right_input,
    const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates, const TableStatisticsCache& statistics_cache);

JoinPlanNode build_join_plan_vertex_node(
    const AbstractCostModel& cost_model,
    const std::shared_ptr<AbstractLQPNode>& vertex_node,
    std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> predicates, const AbstractCardinalityEstimator& cardinality_estimator);

}  // namespace opossum
