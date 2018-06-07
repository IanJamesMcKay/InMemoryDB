#include "cost_feature_lqp_node_proxy.hpp"

#include <cmath>

#include "operators/abstract_operator.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "statistics/abstract_cardinality_estimator.hpp"
#include "statistics/column_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/table.hpp"
#include "storage/storage_manager.hpp"
#include "resolve_type.hpp"

namespace opossum {

CostFeatureLQPNodeProxy::CostFeatureLQPNodeProxy(const std::shared_ptr<AbstractLQPNode>& node,
                                                 const std::shared_ptr<AbstractCardinalityEstimator>& cardinality_estimator) : _node(node), _cardinality_estimator(cardinality_estimator) {}

CostFeatureVariant CostFeatureLQPNodeProxy::_extract_feature_impl(const CostFeature cost_feature) const {
  switch (cost_feature) {
    case CostFeature::LeftInputRowCount:
      Assert(_node->left_input(), "Node doesn't have left input");
      return _cardinality_estimator->estimate(_node->left_input());
    case CostFeature::RightInputRowCount:
      Assert(_node->right_input(), "Node doesn't have left input");
      return _cardinality_estimator->estimate(_node->right_input());
    case CostFeature::LeftInputIsReferences:
      Assert(_node->left_input(), "Node doesn't have left input");
      return _node->left_input()->get_statistics()->table_type() == TableType::References;
    case CostFeature::RightInputIsReferences:
      Assert(_node->right_input(), "Node doesn't have left input");
      return _node->right_input()->get_statistics()->table_type() == TableType::References;
    case CostFeature::OutputRowCount:
      return _cardinality_estimator->estimate(_node);

    case CostFeature::OperatorType: {
      /**
       * The following switch makes assumptions about the concrete Operator that the LQPTranslator will choose.
       * TODO(anybody) somehow ask the LQPTranslator about this instead of making assumptions.
       */

      switch (_node->type()) {
        case LQPNodeType::Predicate: return OperatorType::TableScan;

        case LQPNodeType::Join: {
          const auto join_node = std::static_pointer_cast<JoinNode>(_node);

          if (join_node->join_mode() == JoinMode::Cross) {
            return OperatorType::Product;
          } else if (join_node->join_mode() == JoinMode::Inner &&
                     join_node->predicate_condition() == PredicateCondition::Equals) {
            return OperatorType::JoinHash;
          } else {
            return OperatorType::JoinSortMerge;
          }
        }

        case LQPNodeType::Union: {
          return OperatorType::UnionPositions;
        }

        default:
          return OperatorType::Mock;
      }
    }

    case CostFeature::LeftDataType:
    case CostFeature::RightDataType: {
      auto column_reference = LQPColumnReference{};

      if (_node->type() == LQPNodeType::Join) {
        const auto join_node = std::static_pointer_cast<JoinNode>(_node);
        const auto column_references = join_node->join_column_references();
        Assert(column_references, "No columns referenced in this JoinMode");

        column_reference =
            cost_feature == CostFeature::LeftDataType ? column_references->first : column_references->second;
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

      /**
       * Awkwardly obtain the column's DataType. When #882 is in, this will be much simpler.
       * TODO(moritz) clean up once #882 is in
       */
      if (column_reference.original_node()->type() == LQPNodeType::StoredTable) {
        const auto table_name = std::static_pointer_cast<const StoredTableNode>(column_reference.original_node())->table_name();
        return StorageManager::get().get_table(table_name)->column_data_type(column_reference.original_column_id());

      } else if (column_reference.original_node()->type() == LQPNodeType::Mock) {
        const auto constructor_arguments = std::static_pointer_cast<const MockNode>(column_reference.original_node())->constructor_arguments();
        return boost::get<std::shared_ptr<TableStatistics>>(constructor_arguments)->column_statistics().at(column_reference.original_column_id())->data_type();

      } else {
        Fail("Until #882, the data type can only be obtained from non-derived columns");
      }
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
