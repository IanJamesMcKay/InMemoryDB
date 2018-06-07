#include "cost_feature.hpp"

#include "utils/assert.hpp"
#include "constant_mappings.hpp"

namespace opossum {

bool CostFeatureVariant::boolean() const {
  Assert(value.type() == typeid(bool), "CostFeatureVariant doesn't contain a bool");
  return boost::get<bool>(value);
}

float CostFeatureVariant::scalar() const {
  Assert(value.type() == typeid(float), "CostFeatureVariant doesn't contain a float");
  return boost::get<float>(value);
}

DataType CostFeatureVariant::data_type() const {
  Assert(value.type() == typeid(DataType), "CostFeatureVariant doesn't contain a DataType");
  return boost::get<DataType>(value);
}

PredicateCondition CostFeatureVariant::predicate_condition() const {
  Assert(value.type() == typeid(PredicateCondition), "CostFeatureVariant doesn't contain a PredicateCondition");
  return boost::get<PredicateCondition>(value);
}

OperatorType CostFeatureVariant::operator_type() const {
  Assert(value.type() == typeid(OperatorType), "CostFeatureVariant doesn't contain a OperatorType");
  return boost::get<OperatorType>(value);
}

std::string CostFeatureVariant::to_string() const {

  if (value.type() == typeid(float)) {
    return std::to_string(boost::get<float>(value));

  } else if (value.type() == typeid(DataType)) {
    return data_type_to_string.left.at(boost::get<DataType>(value));

  } else if (value.type() == typeid(PredicateCondition)) {
    return predicate_condition_to_string.left.at(boost::get<PredicateCondition>(value));

  } else if (value.type() == typeid(OperatorType)) {
    return operator_type_to_string.left.at(boost::get<OperatorType>(value));

  } else if (value.type() == typeid(bool)) {
    return std::to_string(boost::get<bool>(value));

  }

  Fail("");
}

}  // namespace opossum
