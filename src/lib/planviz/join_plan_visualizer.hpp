#pragma once

#include <memory>

#include "optimizer/join_ordering/join_graph.hpp"
#include "optimizer/join_ordering/abstract_join_plan_node.hpp"
#include "planviz/abstract_visualizer.hpp"

namespace opossum {

class JoinPlanVisualizer : public AbstractVisualizer<std::shared_ptr<const AbstractJoinPlanNode>> {
 public:
  using AbstractVisualizer<std::shared_ptr<const AbstractJoinPlanNode>>::AbstractVisualizer;

 protected:
  void _build_graph(const std::shared_ptr<const AbstractJoinPlanNode>& node) override;

 private:
  void _build_edge(const std::shared_ptr<const AbstractJoinPlanNode>& parent, const std::shared_ptr<const AbstractJoinPlanNode>& child);
};

}  // namespace opossum
