#pragma once

#include <memory>

namespace opossum {

class AbstractLQPNode;
class AbstractJoinPlanPredicate;

std::shared_ptr<AbstractLQPNode> build_lqp_for_predicate(const AbstractJoinPlanPredicate& predicate, const std::shared_ptr<AbstractLQPNode>& input_node);

}  // namespace opossum
