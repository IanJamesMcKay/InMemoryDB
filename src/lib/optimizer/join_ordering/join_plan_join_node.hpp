#pragma once

#include "abstract_join_plan_node.hpp"
#include "cost_model/cost.hpp"
#include "join_plan_predicate.hpp"

namespace opossum {

class AbstractCostModel;

class JoinPlanJoinNode final : public AbstractJoinPlanNode {
 public:
  JoinPlanJoinNode(const std::shared_ptr<const AbstractJoinPlanNode>& left_child,
                   const std::shared_ptr<const AbstractJoinPlanNode>& right_child,
                   const std::shared_ptr<TableStatistics>& statistics,
                   const std::shared_ptr<JoinPlanAtomicPredicate>& primary_join_predicate,
                   const std::vector<std::shared_ptr<AbstractJoinPlanPredicate>>& secondary_predicates,
                   const Cost node_cost);

  std::shared_ptr<JoinPlanAtomicPredicate> primary_join_predicate() const;
  const std::vector<std::shared_ptr<AbstractJoinPlanPredicate>>& secondary_predicates() const;

  bool contains_vertex(const std::shared_ptr<AbstractLQPNode>& node) const override;
  std::optional<ColumnID> find_column_id(const LQPColumnReference& column_reference) const override;
  std::shared_ptr<AbstractLQPNode> to_lqp() const override;
  size_t output_column_count() const override;
  std::string description() const override;

 private:
  std::shared_ptr<JoinPlanAtomicPredicate> _primary_join_predicate;
  std::vector<std::shared_ptr<AbstractJoinPlanPredicate>> _secondary_predicates;
};

}  // namespace opossum
