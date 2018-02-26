#pragma once

#include <memory>
#include <vector>

#include "abstract_join_plan_node.hpp"

namespace opossum {

class JoinPlanVertexNode final : public AbstractJoinPlanNode {
 public:
  explicit JoinPlanVertexNode(const std::shared_ptr<AbstractLQPNode>& vertex_node,
                              const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates);

  std::shared_ptr<AbstractLQPNode> lqp_node() const;
  const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates() const;

  bool contains_vertex(const std::shared_ptr<AbstractLQPNode>& node) const override;
  std::optional<ColumnID> find_column_id(const LQPColumnReference& column_reference) const override;
  std::shared_ptr<AbstractLQPNode> to_lqp() const override;
  size_t output_column_count() const override;
  std::string description() const override;

 private:
  const std::shared_ptr<AbstractLQPNode> _lqp_node;
  std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> _predicates;
};

}  // namespace opossum
