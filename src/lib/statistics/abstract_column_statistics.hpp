#pragma once

#include <memory>

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

class AbstractColumnStatistics;

/**
 * Return type of selectivity functions for operations on one column.
 */
struct SingleColumnEstimation {
  float selectivity{0.0f};
  std::shared_ptr<AbstractColumnStatistics> column_statistics;
};

/**
 * Return type of selectivity functions for operations on two columns.
 */
struct TwoColumnEstimation {
  float selectivity{0.0f};
  std::shared_ptr<AbstractColumnStatistics> left_column_statistics;
  std::shared_ptr<AbstractColumnStatistics> right_column_statistics;
};

class AbstractColumnStatistics : public std::enable_shared_from_this<AbstractColumnStatistics> {
 public:
  AbstractColumnStatistics(const DataType data_type, const float null_value_ratio, const float distinct_count);
  virtual ~AbstractColumnStatistics() = default;

  /**
   * @defgroup Member access
   * @{
   */
  DataType data_type() const;
  float null_value_ratio() const;
  float non_null_value_ratio() const;
  float distinct_count() const;

  void set_null_value_ratio(const float null_value_ratio);
  /**
   * @}
   */

  /**
   * Clones the derived object and returns a base class pointer to it.
   */
  virtual std::shared_ptr<AbstractColumnStatistics> clone() const = 0;

  /**
   * @defgroup Statistical estimation for operations such as predicates
   * @{
   */
  /**
   * Estimate selectivity for predicate with constants.
   * Predict result of a table scan with constant values.
   */
  virtual SingleColumnEstimation estimate_predicate(
  const PredicateCondition predicate_condition, const AllTypeVariant& value,
  const std::optional<AllTypeVariant>& value2 = std::nullopt) const = 0;

  /**
   * Estimate selectivity for predicate with prepared statements.
   * In comparison to predicates with constants, value is not known yet.
   * Therefore, when necessary, default selectivity values are used for predictions.
   */
  virtual SingleColumnEstimation estimate_predicate(
  const PredicateCondition predicate_condition, const ValuePlaceholder& value,
  const std::optional<AllTypeVariant>& value2 = std::nullopt) const = 0;

  /**
   * Estimate selectivity for predicate on columns.
   * In comparison to predicates with constants, value is another column.
   * For predicate "col_left < col_right", selectivity is calculated in column statistics of col_left with parameters
   * predicate_condition = "<" and right_base_column_statistics = col_right statistics.
   * @return Selectivity and two new column statistics.
   */
  virtual TwoColumnEstimation estimate_predicate(
  const PredicateCondition predicate_condition,
  const std::shared_ptr<AbstractColumnStatistics>& right_abstract_column_statistics,
  const std::optional<AllTypeVariant>& value2 = std::nullopt) const = 0;
  /**
   * @}
   */

 private:
  const DataType _data_type;
  const float _null_value_ratio;
  const float _distinct_count;
};

}  // namespace opossum
