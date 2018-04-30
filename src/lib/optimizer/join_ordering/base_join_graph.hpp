#pragma once

#include <vector>
#include <memory>

namespace opossum {

class AbstractLQPNode;
class AbstractJoinPlanPredicate;
class LQPColumnReference;

struct BaseJoinGraph final {
  static BaseJoinGraph from_joined_graphs(const BaseJoinGraph& left, const BaseJoinGraph& right);

  BaseJoinGraph() = default;
  BaseJoinGraph(const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices, const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates);

  std::shared_ptr<AbstractLQPNode> find_vertex(const LQPColumnReference& column_reference) const;

  std::vector<std::shared_ptr<AbstractLQPNode>> vertices;
  std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> predicates;
};

} // namespace opossum
