#pragma once

#include <vector>
#include <memory>

namespace opossum {

class AbstractLQPNode;
class AbstractJoinPlanPredicate;

struct BaseJoinGraph final {
  std::vector<std::shared_ptr<AbstractLQPNode>> vertices;
  std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> predicates;
};

} // namespace opossum
