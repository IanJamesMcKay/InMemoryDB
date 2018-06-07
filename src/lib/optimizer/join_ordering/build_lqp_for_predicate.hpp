#pragma once

#include <vector>
#include <memory>

namespace opossum {

class AbstractLQPNode;
class AbstractJoinPlanPredicate;

/**
 * @param nodes         Optional, collects the nodes added to build this predicate
 */
std::shared_ptr<AbstractLQPNode> build_lqp_for_predicate(const AbstractJoinPlanPredicate& predicate,
                                                         const std::shared_ptr<AbstractLQPNode>& input_node,
                                                         const std::shared_ptr<std::vector<std::shared_ptr<AbstractLQPNode>>>& nodes = {});

}  // namespace opossum
