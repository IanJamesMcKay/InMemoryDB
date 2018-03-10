#include "join_plan_visualizer.hpp"

#include "optimizer/join_ordering/join_plan_join_node.hpp"
#include "optimizer/join_ordering/join_plan_vertex_node.hpp"
#include "viz_record_layout.hpp"

namespace opossum {

void JoinPlanVisualizer::_build_graph(const std::shared_ptr<const AbstractJoinPlanNode>& node) {
  if (!node) {
    return;
  }

  VizRecordLayout record_layout;

  switch (node->type()) {
    case JoinPlanNodeType::Join: {
      const auto join_node = std::static_pointer_cast<const JoinPlanJoinNode>(node);

      if (join_node->primary_join_predicate()) {
        record_layout.add_label("Inner Join");
        std::stringstream stream;
        join_node->primary_join_predicate()->print(stream);
        record_layout.add_label(stream.str());
      } else {
        record_layout.add_label("Cross Join");
      }

      if (!join_node->secondary_predicates().empty()) {
        auto& secondary_predicates_layout = record_layout.add_sublayout().add_label("Secondary Predicates");
        for (const auto& secondary_predicate : join_node->secondary_predicates()) {
          std::stringstream stream;
          secondary_predicate->print(stream);
          secondary_predicates_layout.add_label(stream.str());
        }
      }
    } break;

    case JoinPlanNodeType::Vertex: {
      const auto vertex_node = std::static_pointer_cast<const JoinPlanVertexNode>(node);

      record_layout.add_label(vertex_node->lqp_node()->description());

      for (const auto& predicate : vertex_node->predicates()) {
        std::stringstream stream;
        predicate->print(stream);
        record_layout.add_label(stream.str());
      }
    } break;
  }

  std::stringstream stream;
  stream << "Node cost: " << node->node_cost() << "; Plan cost: " << node->plan_cost()
         << "; Row count: " << node->statistics()->row_count();
  record_layout.add_label(stream.str());

  VizVertexInfo viz_vertex_info;
  viz_vertex_info.shape = "record";
  viz_vertex_info.label = record_layout.to_label_string();
  _add_vertex(node, viz_vertex_info);

  if (node->left_child()) {
    _build_graph(node->left_child());
    _build_edge(node, node->left_child());
  }

  if (node->right_child()) {
    _build_graph(node->right_child());
    _build_edge(node, node->right_child());
  }
}

void JoinPlanVisualizer::_build_edge(const std::shared_ptr<const AbstractJoinPlanNode>& parent,
                                     const std::shared_ptr<const AbstractJoinPlanNode>& child) {
  VizEdgeInfo viz_edge_info;
  viz_edge_info.dir = "forward";

  _add_edge(child, parent, viz_edge_info);
}

}  // namespace opossum
