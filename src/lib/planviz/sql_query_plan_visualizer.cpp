#include <memory>
#include <string>
#include <utility>
#include <operators/get_table.hpp>
#include <storage/storage_manager.hpp>

#include "cost_model/cost_feature_operator_proxy.hpp"
#include "cost_model/cost_feature_lqp_node_proxy.hpp"
#include "planviz/abstract_visualizer.hpp"
#include "planviz/sql_query_plan_visualizer.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "cost_model/abstract_cost_model.hpp"
#include "statistics/table_statistics.hpp"
#include "sql/sql_query_plan.hpp"
#include "utils/format_duration.hpp"
#include "utils/format_integer.hpp"
#include "viz_record_layout.hpp"

namespace opossum {

SQLQueryPlanVisualizer::SQLQueryPlanVisualizer() : AbstractVisualizer() {}

SQLQueryPlanVisualizer::SQLQueryPlanVisualizer(GraphvizConfig graphviz_config, VizGraphInfo graph_info,
                                               VizVertexInfo vertex_info, VizEdgeInfo edge_info)
    : AbstractVisualizer(std::move(graphviz_config), std::move(graph_info), std::move(vertex_info),
                         std::move(edge_info)) {}

void SQLQueryPlanVisualizer::set_cost_model(const std::shared_ptr<AbstractCostModel>& cost_model) {
  _cost_model = cost_model;
}

void SQLQueryPlanVisualizer::_build_graph(const SQLQueryPlan& plan) {
  std::unordered_set<std::shared_ptr<const AbstractOperator>> visualized_ops;
  for (const auto& root : plan.tree_roots()) {
    _build_subtree(root, visualized_ops);
  }
}

void SQLQueryPlanVisualizer::_build_subtree(
    const std::shared_ptr<const AbstractOperator>& op,
    std::unordered_set<std::shared_ptr<const AbstractOperator>>& visualized_ops) {
  // Avoid drawing dataflows/ops redundantly in diamond shaped PQPs
  if (visualized_ops.find(op) != visualized_ops.end()) return;
  visualized_ops.insert(op);

  _add_operator(op);

  if (op->input_left() != nullptr) {
    auto left = op->input_left();
    _build_subtree(left, visualized_ops);
    _build_dataflow(left, op);
  }

  if (op->input_right() != nullptr) {
    auto right = op->input_right();
    _build_subtree(right, visualized_ops);
    _build_dataflow(right, op);
  }
}

void SQLQueryPlanVisualizer::_build_dataflow(const std::shared_ptr<const AbstractOperator>& from,
                                             const std::shared_ptr<const AbstractOperator>& to) {
  VizEdgeInfo info = _default_edge;

  if (const auto& output = from->get_output()) {
    std::stringstream stream;

    stream << std::to_string(output->row_count()) + " row(s)/";
    stream << std::to_string(output->chunk_count()) + " chunk(s)/";
    stream << format_bytes(output->estimate_memory_usage());

    info.label = stream.str();

    info.pen_width = std::fmax(1, std::ceil(std::log10(output->row_count()) / 2));
  }

  _add_edge(from, to, info);
}

void SQLQueryPlanVisualizer::_add_operator(const std::shared_ptr<const AbstractOperator>& op) {
  VizVertexInfo info = _default_vertex;

  if (op->type() == OperatorType::GetTable) {
    const auto get_table = std::static_pointer_cast<const GetTable>(op);
    const auto table = StorageManager::get().get_table(get_table->table_name());

    info.shape = "ellipse";
    info.label = get_table->table_name() + "\n" + std::to_string(table->row_count()) + " rows";
  } else {
    info.shape = "record";
    auto label = op->description(DescriptionMode::MultiLine);

    VizRecordLayout layout;
    layout.add_label(label);

    /**
     * Optimizer info
     */
    if (op->lqp_node()) {
      auto &optimizer_info = layout.add_sublayout();

      auto &comparisons = optimizer_info.add_sublayout();

      auto &row_count_info = comparisons.add_sublayout();
      row_count_info.add_label("RowCount");
      if (op->lqp_node()->optimizer_info) {
        if (op->lqp_node()->optimizer_info->estimated_cardinality) {
          row_count_info.add_label(
          format_integer(static_cast<size_t>(*op->lqp_node()->optimizer_info->estimated_cardinality)) + " est");
        } else {
          row_count_info.add_label("no est");
        }
      } else {
        row_count_info.add_label("-");
      }
      if (op->get_output()) row_count_info.add_label(format_integer(op->get_output()->row_count()) + " aim");

      auto &node_cost_info = comparisons.add_sublayout();
      node_cost_info.add_label("Node Cost");

      if (op->lqp_node()->optimizer_info) {
        node_cost_info.add_label(format_integer(static_cast<int>(op->lqp_node()->optimizer_info->estimated_node_cost)) + " est");
      } else {
        node_cost_info.add_label("-");
      }

      const auto target_cost = _cost_model->get_reference_operator_cost(
      std::const_pointer_cast<AbstractOperator>(op));
      if (target_cost > 0) {
        node_cost_info.add_label(format_integer(static_cast<int>(target_cost)) + " aim");
      } else {
        node_cost_info.add_label("-");
      }

      auto &plan_cost_info = comparisons.add_sublayout();
      plan_cost_info.add_label("Plan Cost");

      if (op->lqp_node()->optimizer_info) {
        plan_cost_info.add_label(format_integer(static_cast<int>(op->lqp_node()->optimizer_info->estimated_plan_cost)) + " est");
      } else {
        plan_cost_info.add_label("-");
      }
    }

    info.label = layout.to_label_string();
  }
  _add_vertex(op, info);
}

}  // namespace opossum
