#pragma once

#include <map>

namespace opossum {

enum class CostFeature {
  LeftInputRowCount, RightInputRowCount,
  LeftInputReferenceCount, RightInputReferenceCount,
  LeftInputRowCountLogN, RightInputRowCountLogN,
  MajorInputRowCount, MinorInputRowCount,
  OutputRowCount
};

using CostFeatureWeights = std::map<CostFeature, float>;

}  // namespace opossum
