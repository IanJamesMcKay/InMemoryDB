#include "cost_feature_join_plan_proxy.hpp"

#include <cmath>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "optimizer/join_ordering/base_join_graph.hpp"
#include "statistics/abstract_cardinality_estimator.hpp"
#include "statistics/column_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "resolve_type.hpp"

namespace opossum {

CostFeatureJoinPlanProxy::CostFeatureJoinPlanProxy(const OperatorType operator_type,
                                                   const std::shared_ptr<AbstractCardinalityEstimator>& cardinality_estimator,
                                                   const std::shared_ptr<AbstractJoinPlanPredicate>& predicate,
                                                   const std::shared_ptr<BaseJoinGraph>& left_input_graph,
                                                   const std::shared_ptr<BaseJoinGraph>& right_input_graph):
  _operator_type(operator_type), _cardinality_estimator(cardinality_estimator), _predicate(predicate), _left_input_graph(left_input_graph), _right_input_graph(right_input_graph) {

}

CostFeatureVariant CostFeatureJoinPlanProxy::_extract_feature_impl(const CostFeature cost_feature) const  {
  switch (cost_feature) {
    case CostFeature::LeftInputRowCount:
      Assert(_left_input_graph, "Node doesn't have left input");
      return _cardinality_estimator->estimate(*_left_input_graph);
    case CostFeature::RightInputRowCount:
      Assert(_right_input_graph, "Node doesn't have left input");
      return _cardinality_estimator->estimate(*_right_input_graph);
    case CostFeature::LeftInputIsReferences:
      Assert(_left_input_graph, "Node doesn't have left input");
      return _left_input_graph->predicates.empty() && _left_input_graph->vertices.size() == 1;
    case CostFeature::RightInputIsReferences:
      Assert(_right_input_graph, "Node doesn't have left input");
      return _right_input_graph->predicates.empty() && _right_input_graph->vertices.size() == 1;
    case CostFeature::OutputRowCount: {
      if (!_left_input_graph) return 0.0f;

      BaseJoinGraph join_graph;

      join_graph.vertices = _left_input_graph->vertices;
      join_graph.predicates = _left_input_graph->predicates;
      join_graph.predicates.emplace_back(_predicate);

      if (_right_input_graph) {
        join_graph.vertices.insert(join_graph.vertices.end(), _right_input_graph->vertices.begin(), _right_input_graph->vertices.end());
        join_graph.predicates.insert(join_graph.predicates.end(), _right_input_graph->predicates.begin(), _right_input_graph->predicates.end());
      }

      return _cardinality_estimator->estimate(join_graph);
    }

    case CostFeature::LeftDataType:
    case CostFeature::RightDataType: {
      auto column_reference = LQPColumnReference{};

      if (_node->type() == LQPNodeType::Join) {
        const auto join_node = std::static_pointer_cast<JoinNode>(_node);
        const auto column_references = join_node->join_column_references();
        Assert(column_references, "No columns referenced in this JoinMode");

        column_reference = cost_feature == CostFeature::LeftDataType ?
                           column_references->first :
                           column_references->second;
      } else if (_node->type() == LQPNodeType::Predicate) {
        const auto predicate_node = std::static_pointer_cast<PredicateNode>(_node);
        if (cost_feature == CostFeature::LeftDataType) {
          column_reference = predicate_node->column_reference();
        } else {
          if (predicate_node->value().type() == typeid(AllTypeVariant)) {
            return data_type_from_all_type_variant(boost::get<AllTypeVariant>(predicate_node->value()));
          } else {
            Assert(predicate_node->value().type() == typeid(LQPColumnReference), "Expected LQPColumnReference");
            column_reference = boost::get<LQPColumnReference>(predicate_node->value());
          }
        }
      } else {
        Fail("CostFeature not defined for LQPNodeType");
      }

      auto column_id = _node->get_output_column_id(column_reference);
      return _node->get_statistics()->column_statistics().at(column_id)->data_type();
    }

    case CostFeature::PredicateCondition:
      if (_node->type() == LQPNodeType::Join) {
        const auto predicate_condition = std::static_pointer_cast<JoinNode>(_node)->predicate_condition();
        Assert(predicate_condition, "No PredicateCondition in this JoinMode");
        return *predicate_condition;
      } else if (_node->type() == LQPNodeType::Predicate) {
        return std::static_pointer_cast<PredicateNode>(_node)->predicate_condition();
      } else {
        Fail("CostFeature not defined for LQPNodeType");
      }

    case CostFeature::RightOperandIsColumn:
      if (_node->type() == LQPNodeType::Predicate) {
        return is_lqp_column_reference(std::static_pointer_cast<PredicateNode>(_node)->value());
      } else {
        Fail("CostFeature not defined for LQPNodeType");
      }

    default:
      break;
  }
  Fail("Feature extraction failed. Maybe the Feature should be handled in AbstractCostFeatureProxy?");
}

}  // namespace opossum
