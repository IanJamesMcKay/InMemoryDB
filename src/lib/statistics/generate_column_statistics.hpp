#pragma once

#include <unordered_set>

#include "abstract_column_statistics.hpp"
#include "column_statistics.hpp"
#include "resolve_type.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/table.hpp"

namespace opossum {

template<typename ColumnDataType>
class ColumnStatisticsGenerator final {
 public:
  // If (table.row_count() / min(table.row_count(), sample_count_hint)) > FULL_SCAN_THRESHOLD then the entire Table
  // will be scanned, since that's likely faster than creating ChunkOffsetList for sampled scanning
  static constexpr auto FULL_SCAN_THRESHOLD = 0.4f;

  ColumnStatisticsGenerator(const Table& table,
                            const ColumnID column_id,
                            const size_t sample_count_hint);

  struct Result final {
    // The actual number of samples taken
    size_t sample_count{0};

    std::unordered_set<ColumnDataType> distinct_set;
    size_t null_value_count{0};
    ColumnDataType min;
    ColumnDataType max;
  };

  Result operator()();

 private:
  void init();
  template<typename Value>
  void process_sample(const Value& value);

  const Table& _table;
  const ColumnID _column_id;
  const size_t _sample_count_hint;

  Result _result;
};

/**
 * Generate the statistics of a single column. Used by generate_table_statistics()
 * @param table                 the Table that the Column is in
 * @param column_id             the index of the Column in the Table
 * @param sample_count_hint     hint at how many rows the algorithm should look at. Actual number may differ. 0 to
 *                              scan entire table.
 */
template <typename ColumnDataType>
std::shared_ptr<AbstractColumnStatistics> generate_column_statistics(const Table& table,
                                                                     const ColumnID column_id,
                                                                     const size_t sample_count_hint = 0) {
  auto sample = ColumnStatisticsGenerator<ColumnDataType>{table, column_id, sample_count_hint}();

  // Early out since code below relies on there being at least one sample
  if (sample.sample_count == 0) {
    Assert(table.empty(), "No samples should only be taken when the Table is empty()");
    return std::make_shared<ColumnStatistics<ColumnDataType>>(0.0f, 0.0f, sample.min, sample.max);
  }

  auto distinct_count = sample.distinct_set.size();
  const auto null_value_ratio = static_cast<float>(sample.null_value_count) / sample.sample_count;

  auto constexpr EXTRAPOLATION_THRESHOLD = 0.8f;
  auto constexpr EXTRAPOLATION_SAMPLE_RATIO = 0.1f;
  auto constexpr EXTRAPOLATION_MIN_ROW_COUNT = 20;

  const auto ratio_of_rows_sampled = static_cast<float>(table.row_count()) / sample.sample_count;

  if (const auto extrapolation_sample_row_count = sample.sample_count * EXTRAPOLATION_SAMPLE_RATIO;
      ratio_of_rows_sampled < EXTRAPOLATION_THRESHOLD && extrapolation_sample_row_count > EXTRAPOLATION_MIN_ROW_COUNT) {

    const auto extrapolation_sample = ColumnStatisticsGenerator<ColumnDataType>{table, column_id, static_cast<size_t>(extrapolation_sample_row_count)}();

    // We might have - accidentally and unlikely - sampled too many or just null values in which case we can't do
    // extrapolation
    if (!extrapolation_sample.distinct_set.empty() && extrapolation_sample_row_count > EXTRAPOLATION_MIN_ROW_COUNT) {
      const auto distinct_ratio = std::max(1.0f, static_cast<float>(distinct_count) /
                                                 extrapolation_sample.distinct_set.size());
      distinct_count *= distinct_ratio;
    }
  }

  return std::make_shared<ColumnStatistics<ColumnDataType>>(null_value_ratio, distinct_count, sample.min, sample.max);
}

}  // namespace opossum
