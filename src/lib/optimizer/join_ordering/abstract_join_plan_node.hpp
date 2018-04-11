#pragma once

#include <iostream>
#include <memory>
#include <optional>
#include <ostream>

#include "cost_model/cost.hpp"
#include "join_graph.hpp"
#include "statistics/table_statistics.hpp"

namespace opossum {

class AbstractLQPNode;
class JoinVertex;
class JoinEdge;
class JoinPlanPredicate;

enum class JoinPlanNodeType { Vertex, Join };

class AbstractJoinPlanNode : public std::enable_shared_from_this<AbstractJoinPlanNode> {
 public:
  AbstractJoinPlanNode(const JoinPlanNodeType type, const Cost node_cost,
                       const std::shared_ptr<TableStatistics>& statistics,
                       const std::shared_ptr<const AbstractJoinPlanNode>& left_child = nullptr,
                       const std::shared_ptr<const AbstractJoinPlanNode>& right_child = nullptr);
  virtual ~AbstractJoinPlanNode() = default;

  JoinPlanNodeType type() const;
  std::shared_ptr<TableStatistics> statistics() const;
  Cost node_cost() const;
  Cost plan_cost() const;
  std::shared_ptr<const AbstractJoinPlanNode> left_child() const;
  std::shared_ptr<const AbstractJoinPlanNode> right_child() const;

  void print(std::ostream& stream = std::cout) const;

  virtual bool contains_vertex(const std::shared_ptr<AbstractLQPNode>& node) const = 0;
  virtual std::optional<ColumnID> find_column_id(const LQPColumnReference& column_reference) const = 0;
  virtual std::shared_ptr<AbstractLQPNode> to_lqp() const = 0;
  virtual size_t output_column_count() const = 0;
  virtual std::string description() const = 0;

 protected:
  std::shared_ptr<AbstractLQPNode> _insert_predicate(
  const std::shared_ptr<AbstractLQPNode>& lqp,
  const std::shared_ptr<const AbstractJoinPlanPredicate>& predicate) const;

  const JoinPlanNodeType _type;
  Cost _node_cost{0.0f};
  std::shared_ptr<TableStatistics> _statistics;
  std::shared_ptr<const AbstractJoinPlanNode> _left_child;
  std::shared_ptr<const AbstractJoinPlanNode> _right_child;
};

}  // namespace opossum
