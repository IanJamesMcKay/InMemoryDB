#pragma once

#include <memory>

#include "cost.hpp"

namespace opossum {

class AbstractLQPNode;

struct JoinPlanNode final {
  JoinPlanNode(const std::shared_ptr<AbstractLQPNode>& lqp, const Cost plan_cost);

  std::shared_ptr<AbstractLQPNode> lqp;
  Cost plan_cost;
};

}  // namespace opossum
