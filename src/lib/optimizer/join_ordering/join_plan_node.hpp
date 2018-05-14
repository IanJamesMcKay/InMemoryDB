#pragma once

#include <memory>

#include "base_join_graph.hpp"
#include "cost_model/cost.hpp"

namespace opossum {

class AbstractLQPNode;

struct JoinPlanNode final {
  JoinPlanNode() = default;
  JoinPlanNode(const std::shared_ptr<AbstractLQPNode>& lqp, const Cost plan_cost, const BaseJoinGraph& join_graph);

  std::shared_ptr<AbstractLQPNode> lqp;
  Cost plan_cost{0};
  BaseJoinGraph join_graph;
};

}  // namespace opossum
