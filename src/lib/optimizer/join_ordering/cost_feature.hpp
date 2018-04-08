#pragma once

#include <map>

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

enum class CostFeature {
  /**
   * Numerical features
   */

  LeftInputRowCount, RightInputRowCount,
  InputRowCountProduct,
  LeftInputReferenceRowCount, RightInputReferenceRowCount,
  LeftInputRowCountLogN, RightInputRowCountLogN,
  MajorInputRowCount, MinorInputRowCount,
  MajorInputReferenceRowCount, MinorInputReferenceRowCount,
  OutputRowCount, OutputDereferenceRowCount,

  /**
   * Categorical features
   */
  LeftDataType, RightDataType,
  PredicateCondition,

  /**
   * Boolean features
   */
  LeftInputIsReferences, RightInputIsReferences,
  RightOperandIsColumn, LeftInputIsMajor
};

using CostFeatureWeights = std::map<CostFeature, float>;

namespace detail {

using CostFeatureVariant = boost::variant<float, DataType, PredicateCondition, bool>;

}

/**
 * Wraps detail::CostFeatureVariant and provides getter for the member types of the variants that perform type checking
 * at runtime
 */
struct CostFeatureVariant {
 public:
  CostFeatureVariant(const detail::CostFeatureVariant& value); // NOLINT - implicit conversion is intended

  bool boolean() const;
  float scalar() const;
  DataType data_type() const;
  PredicateCondition predicate_condition() const;

  detail::CostFeatureVariant value;
};

}  // namespace opossum
