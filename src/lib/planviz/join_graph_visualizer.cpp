#include "join_graph_visualizer.hpp"

#include <sstream>

#include "optimizer/join_ordering/join_edge.hpp"
#include "viz_record_layout.hpp"

namespace opossum {

void JoinGraphVisualizer::_build_graph(const std::shared_ptr<const JoinGraph>& graph) {
  for (auto vertex_idx = size_t{0}; vertex_idx < graph->vertices.size(); ++vertex_idx) {
    boost::dynamic_bitset<> vertex_bit{graph->vertices.size()};
    vertex_bit.set(vertex_idx);

    const auto& vertex = graph->vertices[vertex_idx];
    const auto predicates = graph->find_predicates(vertex_bit);

    VizRecordLayout layout;
    layout.add_label(vertex->description());

    if (!predicates.empty()) {
      auto& predicates_layout = layout.add_sublayout();

      for (const auto& predicate : predicates) {
        std::stringstream predicate_stream;
        predicate->print(predicate_stream);
        predicates_layout.add_label(predicate_stream.str());
      }
    }

    VizVertexInfo vertex_info = _default_vertex;
    vertex_info.label = layout.to_label_string();
    vertex_info.shape = "record";

    _add_vertex(vertex, vertex_info);
  }

  for (const auto& edge : graph->edges) {
    const auto num_vertices = edge->vertex_set.count();

    if (num_vertices <= 1) continue;

    if (num_vertices == 2) {
      const auto first_vertex_idx = edge->vertex_set.find_first();
      const auto second_vertex_idx = edge->vertex_set.find_next(first_vertex_idx);

      const auto first_vertex = graph->vertices[first_vertex_idx];
      const auto second_vertex = graph->vertices[second_vertex_idx];

      std::stringstream label_stream;
      for (const auto& predicate : edge->predicates) {
        predicate->print(label_stream);
        label_stream << "\n";
      }

      VizEdgeInfo edge_info;
      edge_info.color = _random_color();
      edge_info.font_color = edge_info.color;
      edge_info.dir = "none";
      edge_info.label = label_stream.str();

      _add_edge(first_vertex, second_vertex, edge_info);
    } else {
      std::stringstream vertex_label_stream;
      for (size_t predicate_idx{0}; predicate_idx < edge->predicates.size(); ++predicate_idx) {
        const auto& predicate = edge->predicates[predicate_idx];
        predicate->print(vertex_label_stream);
        if (predicate_idx + 1 < edge->predicates.size()) {
          vertex_label_stream << "\n";
        }
      }

      VizVertexInfo vertex_info = _default_vertex;
      vertex_info.label = vertex_label_stream.str();
      vertex_info.color = _random_color();
      vertex_info.font_color = vertex_info.color;
      vertex_info.shape = "diamond";
      _add_vertex(edge, vertex_info);

      for (auto current_vertex_idx = edge->vertex_set.find_first(); current_vertex_idx != boost::dynamic_bitset<>::npos;
           current_vertex_idx = edge->vertex_set.find_next(current_vertex_idx)) {
        VizEdgeInfo edge_info;
        edge_info.dir = "none";
        edge_info.font_color = vertex_info.color;
        edge_info.color = vertex_info.color;
        _add_edge(graph->vertices[current_vertex_idx], edge, edge_info);
      }
    }
  }
}

}  // namespace opossum
