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

GenericInputCostFeatures GenericInputCostFeatures::from_join_graph(const BaseJoinGraph &input_join_graph, const AbstractCardinalityEstimator &cardinality_estimator) {
  return {
    cardinality_estimator.estimate(input_join_graph.vertices, input_join_graph.predicates),
    input_join_graph.vertices.size() > 1 || !input_join_graph.predicates.empty()
  };
}

GenericPredicateCostFeatures GenericPredicateCostFeatures::from_join_plan_predicate(const std::shared_ptr<const JoinPlanAtomicPredicate> &predicate,
                                                             const BaseJoinGraph &input_join_graph,
                                                             const AbstractCardinalityEstimator &cardinality_estimator) {
  const auto left_operand_vertex = input_join_graph.find_vertex(predicate->left_operand);
  const auto left_operand_column_id = left_operand_vertex->get_output_column_id(predicate->left_operand);
  const auto left_operand_data_type = left_operand_vertex->get_statistics()->column_statistics().at(left_operand_column_id)->data_type();

  auto right_operand_data_type = DataType::Null;
  auto right_operand_is_column = false;

  if (is_lqp_column_reference(predicate->right_operand)) {
    const auto right_operand_column_reference = boost::get<LQPColumnReference>(predicate->right_operand);
    const auto right_operand_vertex = input_join_graph.find_vertex(right_operand_column_reference);
    const auto right_operand_column_id = right_operand_vertex->get_output_column_id(right_operand_column_reference);
    right_operand_data_type = right_operand_vertex->get_statistics()->column_statistics().at(right_operand_column_id)->data_type();
    right_operand_is_column = true;
  } else {
    right_operand_data_type = data_type_from_all_type_variant(boost::get<AllTypeVariant>(predicate->right_operand));
  }

  return {
  left_operand_data_type, right_operand_data_type, right_operand_is_column, predicate->predicate_condition
  };
}

GenericInputCostFeatures::GenericInputCostFeatures(const float row_count, const bool is_references):
  row_count(row_count), is_references(is_references) {}


GenericPredicateCostFeatures::GenericPredicateCostFeatures(const DataType left_data_type,
                             const DataType right_data_type,
                             const bool right_is_column,
                             const PredicateCondition predicate_condition):
  left_data_type(left_data_type), right_data_type(right_data_type), right_is_column(right_is_column), predicate_condition(predicate_condition) {}


CostFeatureGenericProxy CostFeatureGenericProxy::from_join_plan_predicate(const std::shared_ptr<const JoinPlanAtomicPredicate> &predicate,
                                                                            const BaseJoinGraph &input_join_graph,
                                                                            const AbstractCardinalityEstimator &cardinality_estimator) {
  const auto input_features = GenericInputCostFeatures::from_join_graph(input_join_graph, cardinality_estimator);
  const auto predicate_features = GenericPredicateCostFeatures::from_join_plan_predicate(predicate, input_join_graph, cardinality_estimator);

  std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> output_graph_predicates(input_join_graph.predicates.begin(), input_join_graph.predicates.end());
  output_graph_predicates.emplace_back(predicate);

  return {
     OperatorType::TableScan,
     input_features,
     std::nullopt,
     predicate_features,
     cardinality_estimator.estimate(input_join_graph.vertices, output_graph_predicates)
  };
}

CostFeatureGenericProxy CostFeatureGenericProxy::from_join_plan_predicate(const std::shared_ptr<const JoinPlanAtomicPredicate> &predicate,
                                                                            const BaseJoinGraph &left_input_join_graph,
                                                                            const BaseJoinGraph &right_input_join_graph,
                                                                            const AbstractCardinalityEstimator &cardinality_estimator) {
  std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> joined_graph_predicates(left_input_join_graph.predicates.begin(), left_input_join_graph.predicates.end());
  joined_graph_predicates.insert(joined_graph_predicates.end(), right_input_join_graph.predicates.begin(), right_input_join_graph.predicates.end());

  std::vector<std::shared_ptr<AbstractLQPNode>> joined_graph_vertices(left_input_join_graph.vertices.begin(), left_input_join_graph.vertices.end());
  joined_graph_vertices.insert(joined_graph_vertices.end(), right_input_join_graph.vertices.begin(), right_input_join_graph.vertices.end());

  const auto joined_graph = BaseJoinGraph{joined_graph_vertices, joined_graph_predicates};

  const auto left_input_features = GenericInputCostFeatures::from_join_graph(left_input_join_graph, cardinality_estimator);
  const auto right_input_features = GenericInputCostFeatures::from_join_graph(right_input_join_graph, cardinality_estimator);
  const auto predicate_features = GenericPredicateCostFeatures::from_join_plan_predicate(predicate, joined_graph, cardinality_estimator);

  auto output_graph = joined_graph;
  output_graph.predicates.emplace_back(predicate);

  return {
    predicate_features.predicate_condition == PredicateCondition::Equals ? OperatorType::JoinHash : OperatorType::JoinSortMerge,
    left_input_features,
    right_input_features,
    predicate_features,
    cardinality_estimator.estimate(output_graph.vertices, output_graph.predicates),
  };
}

CostFeatureGenericProxy CostFeatureGenericProxy::from_cross_join(const BaseJoinGraph &left_input_join_graph,
                                                                 const BaseJoinGraph &right_input_join_graph,
                                                                 const AbstractCardinalityEstimator &cardinality_estimator) {
  const auto left_input_features = GenericInputCostFeatures::from_join_graph(left_input_join_graph, cardinality_estimator);
  const auto right_input_features = GenericInputCostFeatures::from_join_graph(right_input_join_graph, cardinality_estimator);

  std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> joined_graph_predicates(left_input_join_graph.predicates.begin(), left_input_join_graph.predicates.end());
  joined_graph_predicates.insert(joined_graph_predicates.end(), right_input_join_graph.predicates.begin(), right_input_join_graph.predicates.end());

  std::vector<std::shared_ptr<AbstractLQPNode>> joined_graph_vertices(left_input_join_graph.vertices.begin(), left_input_join_graph.vertices.end());
  joined_graph_vertices.insert(joined_graph_vertices.end(), right_input_join_graph.vertices.begin(), right_input_join_graph.vertices.end());

  return {
    OperatorType::Product,
    left_input_features,
    right_input_features,
    std::nullopt,
    cardinality_estimator.estimate(joined_graph_vertices, joined_graph_predicates),
  };
}

CostFeatureGenericProxy CostFeatureGenericProxy::from_union(const BaseJoinGraph &output_join_graph,
                                          const BaseJoinGraph &left_input_join_graph,
                                          const BaseJoinGraph &right_input_join_graph,
                                          const AbstractCardinalityEstimator &cardinality_estimator) {
  const auto left_input_features = GenericInputCostFeatures::from_join_graph(left_input_join_graph, cardinality_estimator);
  const auto right_input_features = GenericInputCostFeatures::from_join_graph(right_input_join_graph, cardinality_estimator);

  return {
    OperatorType::UnionPositions,
    left_input_features,
    right_input_features,
    std::nullopt,
    cardinality_estimator.estimate(output_join_graph.vertices, output_join_graph.predicates)
  };
}

CostFeatureGenericProxy::CostFeatureGenericProxy(
  const OperatorType operator_type,
  const GenericInputCostFeatures& left_input_features,
  const std::optional<GenericInputCostFeatures>& right_input_features,
  const std::optional<GenericPredicateCostFeatures>& predicate_features,
  const float output_row_count):
  _operator_type(operator_type),
  _left_input_features(left_input_features),
  _right_input_features(right_input_features),
  _predicate_features(predicate_features),
  _output_row_count(output_row_count) {

 }

CostFeatureVariant  CostFeatureGenericProxy::_extract_feature_impl(const CostFeature cost_feature) const  {
  switch (cost_feature) {
    case CostFeature::LeftInputRowCount:
      return _left_input_features.row_count;

    case CostFeature::RightInputRowCount:
      Assert(_right_input_features, "Proxy doesn't have right input");
      return _right_input_features->row_count;

    case CostFeature::LeftInputIsReferences:
      return _left_input_features.is_references;

    case CostFeature::RightInputIsReferences:
      Assert(_right_input_features, "Proxy doesn't have right input");
      return _right_input_features->is_references;

    case CostFeature::OutputRowCount:
      return _output_row_count;

    case CostFeature::LeftDataType:
      Assert(_predicate_features, "Proxy wraps no predicate");
      return _predicate_features->left_data_type;

    case CostFeature::RightDataType:
      Assert(_predicate_features, "Proxy wraps no predicate");
      return _predicate_features->right_data_type;

    case CostFeature::PredicateCondition:
      Assert(_predicate_features, "Proxy wraps no predicate");
      return _predicate_features->predicate_condition;

    case CostFeature::RightOperandIsColumn:
      Assert(_predicate_features, "Proxy wraps no predicate");
      return _predicate_features->right_is_column;

    case CostFeature::OperatorType:
      return _operator_type;

    default:
      break;
  }
  Fail("Feature extraction failed. Maybe the Feature should be handled in AbstractCostFeatureProxy?");
}

}  // namespace opossum
