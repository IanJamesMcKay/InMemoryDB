#include "cost_model_naive.hpp"

#include "abstract_cost_feature_proxy.hpp"
#include "operators/abstract_operator.hpp"

namespace opossum {

std::string CostModelNaive::name() const {
  return "CostModelNaive";
}

Cost CostModelNaive::get_reference_operator_cost(const std::shared_ptr<AbstractOperator>& op) const {
  return estimate_operator_cost(op);
}

Cost CostModelNaive::_cost_model_impl(const OperatorType operator_type, const AbstractCostFeatureProxy& feature_proxy) const {
  switch (operator_type) {
    case OperatorType::JoinHash: return 1.2f * feature_proxy.extract_feature(CostFeature::MajorInputRowCount).scalar();
    case OperatorType::TableScan: return feature_proxy.extract_feature(CostFeature::LeftInputRowCount).scalar();
    case OperatorType::JoinSortMerge:
      return feature_proxy.extract_feature(CostFeature::LeftInputRowCountLogN).scalar() + feature_proxy.extract_feature(CostFeature::RightInputRowCount).scalar();
    case OperatorType::Product: return feature_proxy.extract_feature(CostFeature::InputRowCountProduct).scalar();
    case OperatorType::UnionPositions:
      return feature_proxy.extract_feature(CostFeature::LeftInputRowCountLogN).scalar() + feature_proxy.extract_feature(CostFeature::RightInputRowCount).scalar();

    default:
      return 0.0f;
  }
}

}  // namespace opossum
