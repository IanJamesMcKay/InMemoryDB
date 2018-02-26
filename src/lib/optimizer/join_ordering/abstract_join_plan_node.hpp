#pragma once

#include <iostream>
#include <memory>
#include <optional>
#include <ostream>

#include "join_graph.hpp"
#include "join_plan_predicate_estimate.hpp"
#include "optimizer/table_statistics.hpp"

namespace opossum {

class AbstractLQPNode;
class JoinVertex;
class JoinEdge;
class JoinPlanPredicate;

enum class JoinPlanNodeType { Vertex, Join };

class AbstractJoinPlanNode : public std::enable_shared_from_this<AbstractJoinPlanNode> {
 public:
  explicit AbstractJoinPlanNode(const JoinPlanNodeType type);
  virtual ~AbstractJoinPlanNode() = default;

  JoinPlanNodeType type() const;
  std::shared_ptr<TableStatistics> statistics() const;
  float node_cost() const;
  float plan_cost() const;
  std::shared_ptr<const AbstractJoinPlanNode> left_child() const;
  std::shared_ptr<const AbstractJoinPlanNode> right_child() const;

  void print(std::ostream& stream = std::cout) const;

  virtual bool contains_vertex(const std::shared_ptr<AbstractLQPNode>& node) const = 0;
  virtual std::optional<ColumnID> find_column_id(const LQPColumnReference& column_reference) const = 0;
  virtual std::shared_ptr<AbstractLQPNode> to_lqp() const = 0;
  virtual size_t output_column_count() const = 0;
  virtual std::string description() const = 0;

 protected:
  JoinPlanPredicateEstimate _estimate_predicate(const std::shared_ptr<const AbstractJoinPlanPredicate>& predicate,
                                                const std::shared_ptr<TableStatistics>& statistics) const;
  std::shared_ptr<AbstractLQPNode> _insert_predicate(
      const std::shared_ptr<AbstractLQPNode>& lqp,
      const std::shared_ptr<const AbstractJoinPlanPredicate>& predicate) const;

  void _order_predicates(std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates, const std::shared_ptr<TableStatistics>& statistics) const;

  float _node_cost{0.0f};
  float _plan_cost{0.0f};
  std::shared_ptr<TableStatistics> _statistics;
  std::shared_ptr<const AbstractJoinPlanNode> _left_child;
  std::shared_ptr<const AbstractJoinPlanNode> _right_child;

 private:
  void _print_impl(std::ostream& out, std::vector<bool>& levels,
                   std::unordered_map<std::shared_ptr<const AbstractJoinPlanNode>, size_t>& id_by_node,
                   size_t& id_counter) const;

  const JoinPlanNodeType _type;
};

}  // namespace opossum
