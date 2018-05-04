#pragma once

#include <memory>

#include "cost.hpp"

namespace opossum {

class AbstractLQPNode;
class CardinalityEstimator;
class AbstractCostModel;

Cost cost_lqp(const std::shared_ptr<AbstractLQPNode>& lqp, const AbstractCostModel& cost_model);

}  // namespace opossum