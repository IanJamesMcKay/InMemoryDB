#pragma once

#include <memory>

#include "base_join_graph.hpp"
#include "cost_model/cost.hpp"

namespace opossum {

class AbstractLQPNode;

struct JoinPlanNode final {
  JoinPlanNode(const std::shared_ptr<AbstractLQPNode>& lqp, const Cost plan_cost, const BaseJoinGraph& join_graph);

  std::shared_ptr<AbstractLQPNode> lqp;
  Cost plan_cost;
  BaseJoinGraph join_graph;
};

}  // namespace opossum
