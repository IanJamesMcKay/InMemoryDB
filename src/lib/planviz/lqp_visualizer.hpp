#pragma once

#include <boost/algorithm/string.hpp>
#include <iomanip>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "cost_model/cost.hpp"
#include "statistics/table_statistics.hpp"
#include "planviz/abstract_visualizer.hpp"

namespace opossum {

class AbstractCostModel;

class LQPVisualizer : public AbstractVisualizer<std::vector<std::shared_ptr<AbstractLQPNode>>> {
 public:
  LQPVisualizer();

  LQPVisualizer(GraphvizConfig graphviz_config, VizGraphInfo graph_info, VizVertexInfo vertex_info,
                VizEdgeInfo edge_info);

  /**
   * Set a cost model and LQP nodes will receive an annotation about their cost according to that model
   */
  void set_cost_model(const std::shared_ptr<AbstractCostModel>& cost_model);

 protected:
  void _build_graph(const std::vector<std::shared_ptr<AbstractLQPNode>>& lqp_roots) override;

  void _build_subtree(const std::shared_ptr<AbstractLQPNode>& node,
                      std::unordered_set<std::shared_ptr<const AbstractLQPNode>>& visualized_nodes);

  void _build_dataflow(const std::shared_ptr<AbstractLQPNode>& from, const std::shared_ptr<AbstractLQPNode>& to);

  // Optional - if set nodes will be annotated accordingly
  std::shared_ptr<AbstractCostModel> _cost_model;
};

}  // namespace opossum
