#pragma once

#include <map>

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

/**
 * List of features usable in AbstractCostModels.
 *
 * Using enum to provide the unified "AbstractCostFeatureProxy" to access the same features for LQPs and PQPs.
 * Also, this makes it easy to specify Cost formulas from data only, as e.g. CostModelLinear does.
 */
enum class CostFeature {
  /**
   * Numerical features
   */
  LeftInputRowCount, RightInputRowCount,
  InputRowCountProduct, // LeftInputRowCount * RightInputRowCount
  LeftInputReferenceRowCount, RightInputReferenceRowCount, // *InputRowCount if the input is References, 0 otherwise
  LeftInputRowCountLogN, RightInputRowCountLogN, // *InputRowCount * log(*InputRowCount)
  MajorInputRowCount, MinorInputRowCount, // Major = Input with more rows, Minor = Input with less rows
  MajorInputReferenceRowCount, MinorInputReferenceRowCount,
  OutputRowCount,
  OutputDereferenceRowCount, // If input is References, then OutputRowCount. 0 otherwise.

  /**
   * Categorical features
   */
  LeftDataType, RightDataType, // Only valid for Predicated Joins, TableScans
  PredicateCondition, // Only valid for Predicated Joins, TableScans

  /**
   * Boolean features
   */
  LeftInputIsReferences, RightInputIsReferences, // *Input is References
  RightOperandIsColumn, // Only valid for TableScans
  LeftInputIsMajor // LeftInputRowCount > RightInputRowCount
};

using CostFeatureWeights = std::map<CostFeature, float>;

namespace detail {

using CostFeatureVariant = boost::variant<float, DataType, PredicateCondition, bool>;

}

/**
 * Wraps detail::CostFeatureVariant and provides getters for the member types of the variants that perform type checking
 * at runtime.
 */
struct CostFeatureVariant {
 public:
  template<typename T> CostFeatureVariant(const T& value): value(value) {} // NOLINT - implicit conversion is intended

  CostFeatureVariant(const detail::CostFeatureVariant& value); // NOLINT - implicit conversion is intended

  bool boolean() const;
  float scalar() const;
  DataType data_type() const;
  PredicateCondition predicate_condition() const;

  detail::CostFeatureVariant value;
};

}  // namespace opossum
