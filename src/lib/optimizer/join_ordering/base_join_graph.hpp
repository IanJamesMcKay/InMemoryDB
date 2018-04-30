#pragma once

#include <vector>
#include <memory>

namespace opossum {

class AbstractLQPNode;
class AbstractJoinPlanPredicate;
class LQPColumnReference;

struct BaseJoinGraph final {
  std::vector<std::shared_ptr<AbstractLQPNode>> vertices;
  std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> predicates;

  std::shared_ptr<AbstractLQPNode> find_vertex(const LQPColumnReference& column_reference);
};

} // namespace opossum
