#include "cost_feature_join_plan_proxy.hpp"

#include <cmath>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "optimizer/join_ordering/base_join_graph.hpp"
#include "optimizer/join_ordering/join_plan_predicate.hpp"
#include "operators/abstract_operator.hpp"
#include "statistics/abstract_cardinality_estimator.hpp"
#include "statistics/column_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "resolve_type.hpp"

namespace opossum {

GenericInputCostFeatures::GenericInputCostFeatures(const float row_count, const bool is_references):
  row_count(row_count), is_references(is_references) {}


GenericPredicateCostFeatures::GenericPredicateCostFeatures(const DataType left_data_type,
                             const DataType right_data_type,
                             const bool right_is_column,
                             const PredicateCondition predicate_condition): left_data_type(left_data_type), right_data_type(right_data_type), right_is_column(right_is_column), predicate_condition(predicate_condition) {}


CostFeatureGenericProxy CostFeatureGenericProxy::from_join_plan_predicate(const std::shared_ptr<const JoinPlanAtomicPredicate> &predicate,
                                                                            const BaseJoinGraph &input_join_graph,
                                                                            const std::shared_ptr<AbstractCardinalityEstimator> &cardinality_estimator) {
  const auto left_operand_vertex = input_join_graph.find_vertex(predicate->left_operand);
  const auto left_operand_column_id = left_operand_vertex->get_output_column_id(predicate->left_operand);
  const auto left_operand_data_type = left_operand_vertex->get_statistics()->column_statistics().at(left_operand_column_id)->data_type();

  GenericInputCostFeatures input_features{
    left_operand_data_type,
    cardinality_estimator->estimate(input_join_graph.vertices, input_join_graph.predicates),
    input_join_graph.vertices.size() > 1 || !input_join_graph.predicates.empty()
  };

  std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> output_graph_predicates(input_join_graph.predicates.begin(), input_join_graph.predicates.end());
  output_graph_predicates.emplace_back(predicate);

  return {
     OperatorType::TableScan,
     input_features,
     std::nullopt,
     predicate->predicate_condition,
     cardinality_estimator->estimate(input_join_graph.vertices, output_graph_predicates)
  };
}

CostFeatureGenericProxy CostFeatureGenericProxy::from_join_plan_predicate(const std::shared_ptr<const JoinPlanAtomicPredicate> &predicate,
                                                                            const BaseJoinGraph &left_input_join_graph,
                                                                            const BaseJoinGraph &right_input_join_graph,
                                                                            const std::shared_ptr<AbstractCardinalityEstimator> &cardinality_estimator) {
  const auto left_operand_vertex = left_input_join_graph.find_vertex(predicate->left_operand);
  const auto left_operand_column_id = left_operand_vertex->get_output_column_id(predicate->left_operand);
  const auto left_operand_data_type = left_operand_vertex->get_statistics()->column_statistics().at(left_operand_column_id)->data_type();

  Assert(is_lqp_column_reference(predicate->right_operand), "Expected a column refernce");

  const auto right_operand_column_reference = boost::get<LQPColumnReference>(predicate->right_operand);
  const auto right_operand_vertex = right_input_join_graph.find_vertex(right_operand_column_reference);
  const auto right_operand_column_id = right_operand_vertex->get_output_column_id(predicate->right_operand);
  const auto right_operand_data_type = right_operand_vertex->get_statistics()->column_statistics().at(right_operand_column_id)->data_type();

  GenericInputCostFeatures left_input_features{
    left_operand_data_type,
    cardinality_estimator->estimate(left_input_join_graph.vertices, left_input_join_graph.predicates),
    left_input_join_graph.vertices.size() > 1 || !left_input_join_graph.predicates.empty()
  };
  
  GenericInputCostFeatures right_input_features{
    right_operand_data_type,
    cardinality_estimator->estimate(right_input_join_graph.vertices, right_input_join_graph.predicates),
    right_input_join_graph.vertices.size() > 1 || !right_input_join_graph.predicates.empty()
  };

  std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> output_graph_predicates(left_input_join_graph.predicates.begin(), left_input_join_graph.predicates.end());
  output_graph_predicates.insert(output_graph_predicates.end(), right_input_join_graph.predicates.begin(), right_input_join_graph.predicates.end());

  std::vector<std::shared_ptr<AbstractLQPNode>> output_graph_nodes(left_input_join_graph.vertices.begin(), left_input_join_graph.vertices.end());
  output_graph_nodes.insert(output_graph_nodes.end(), right_input_join_graph.vertices.begin(), right_input_join_graph.vertices.end());

  return {
    OperatorType::TableScan,
    left_input_features,
    right_input_features,
    predicate->predicate_condition,
    cardinality_estimator->estimate(output_graph_nodes, output_graph_predicates),
  };
}

CostFeatureGenericProxy CostFeatureGenericProxy::from_cross_join(const BaseJoinGraph &left_input_join_graph,
                                                                 const BaseJoinGraph &right_input_join_graph,
                                                                 const std::shared_ptr<AbstractCardinalityEstimator> &cardinality_estimator) {
  GenericInputCostFeatures left_input_features{
    cardinality_estimator->estimate(left_input_join_graph.vertices, left_input_join_graph.predicates),
    left_input_join_graph.vertices.size() > 1 || !left_input_join_graph.predicates.empty()
  };

  GenericInputCostFeatures right_input_features{
    cardinality_estimator->estimate(right_input_join_graph.vertices, right_input_join_graph.predicates),
    right_input_join_graph.vertices.size() > 1 || !right_input_join_graph.predicates.empty()
  };

  std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> output_graph_predicates(left_input_join_graph.predicates.begin(), left_input_join_graph.predicates.end());
  output_graph_predicates.insert(output_graph_predicates.end(), right_input_join_graph.predicates.begin(), right_input_join_graph.predicates.end());

  std::vector<std::shared_ptr<AbstractLQPNode>> output_graph_nodes(left_input_join_graph.vertices.begin(), left_input_join_graph.vertices.end());
  output_graph_nodes.insert(output_graph_nodes.end(), right_input_join_graph.vertices.begin(), right_input_join_graph.vertices.end());

  return {
    OperatorType::Product,
    left_input_features,
    right_input_features,
    cardinality_estimator->estimate(output_graph_nodes, output_graph_predicates),
  };
}

CostFeatureGenericProxy::CostFeatureGenericProxy(
const OperatorType operator_type,
const GenericInputCostFeatures& left_input_features,
const std::optional<GenericInputCostFeatures>& right_input_features,
const PredicateCondition predicate_condition,
const float output_row_count):
  _operator_type(operator_type), _left_input_features(left_input_features), _right_input_features(right_input_features), _predicate_condition(predicate_condition), _output_row_count(output_row_count) {

 }

CostFeatureVariant  CostFeatureGenericProxy::_extract_feature_impl(const CostFeature cost_feature) const  {
  switch (cost_feature) {
    case CostFeature::LeftInputRowCount:
      _left_input_features.row_count;

    case CostFeature::RightInputRowCount:
      Assert(_right_input_features, "Node doesn't have right input");
      return _right_input_features->row_count;

    case CostFeature::LeftInputIsReferences:
      _left_input_features.is_references;

    case CostFeature::RightInputIsReferences:
      Assert(_right_input_features, "Node doesn't have right input");
      return _right_input_features->is_references;

    case CostFeature::OutputRowCount:
      return _output_row_count;

    case CostFeature::LeftDataType:
      return _left_input_features.data_type;

    case CostFeature::RightDataType:
      Assert(_right_input_features, "Node doesn't have right input");
      return _right_input_features->data_type;

    case CostFeature::PredicateCondition:
      return _predicate_condition;

    case CostFeature::RightOperandIsColumn:
      Assert(_right_input_features, "Node doesn't have right input");
      return _right_input_features->is_column;

    default:
      break;
  }
  Fail("Feature extraction failed. Maybe the Feature should be handled in AbstractCostFeatureProxy?");
}

}  // namespace opossum
