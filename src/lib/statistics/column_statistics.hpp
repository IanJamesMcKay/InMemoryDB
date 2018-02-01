#pragma once

#include "abstract_column_statistics.hpp"

namespace opossum {

template<typename T>
class ColumnStatistics : public AbstractColumnStatistics {
 public:
  ColumnStatistics(const float null_value_ratio, const float distinct_count, const T min, const T max);

  T min() const;

  T max() const;

  SingleColumnEstimation estimate_predicate(
  const PredicateCondition predicate_condition, const AllTypeVariant &value,
  const std::optional<AllTypeVariant> &value2 = std::nullopt) const override;

  SingleColumnEstimation estimate_predicate(
  const PredicateCondition predicate_condition, const ValuePlaceholder &value,
  const std::optional<AllTypeVariant> &value2 = std::nullopt) const override;

  TwoColumnEstimation estimate_predicate(
  const PredicateCondition predicate_condition,
  const std::shared_ptr<AbstractColumnStatistics> &right_abstract_column_statistics,
  const std::optional<AllTypeVariant> &value2 = std::nullopt) const override;

 private:
  const T _min;
  const T _max;
}


}  // namespace opossum
