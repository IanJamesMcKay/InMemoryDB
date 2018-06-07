#pragma once

#include <memory>

#include "cost_model/cost.hpp"

namespace opossum {

class PredicateJoinBlock;
class AbstractLQPNode;

struct JoinPlan {
  JoinPlan() = default;
  JoinPlan(const std::shared_ptr<AbstractLQPNode>& lqp,
           const std::shared_ptr<PredicateJoinBlock>& query_block,
           const Cost plan_cost = 0);

  std::shared_ptr<AbstractLQPNode> lqp;
  std::shared_ptr<PredicateJoinBlock> query_block;
  Cost plan_cost{0};
};

}  // namespace opossum
