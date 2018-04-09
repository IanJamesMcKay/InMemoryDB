#pragma once

#include <unordered_set>

#include "abstract_column_statistics.hpp"
#include "column_statistics.hpp"
#include "resolve_type.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/table.hpp"

namespace opossum {

/**
 * Generate the statistics of a single column. Used by generate_table_statistics()
 */
template <typename ColumnDataType>
std::shared_ptr<AbstractColumnStatistics> generate_column_statistics(const Table& table, const ColumnID column_id, const size_t max_sample_count) {
  std::unordered_set<ColumnDataType> distinct_set;

  auto null_value_count = size_t{0};

  auto min = std::numeric_limits<ColumnDataType>::max();
  auto max = std::numeric_limits<ColumnDataType>::lowest();

  const auto sample_count = std::min(max_sample_count, table.row_count());
  auto row_idx = size_t{1};
  auto prev_sample_idx = size_t{0};

  for (ChunkID chunk_id{0}; chunk_id < table.chunk_count(); ++chunk_id) {
    const auto base_column = table.get_chunk(chunk_id)->get_column(column_id);

    resolve_column_type<ColumnDataType>(*base_column, [&](auto& column) {
      auto iterable = create_iterable_from_column<ColumnDataType>(column);
      iterable.for_each([&](const auto& column_value) {
        ++row_idx;
        const auto current_sample_idx = (row_idx * sample_count) / table.row_count();
        if (current_sample_idx == prev_sample_idx) return;
        prev_sample_idx = current_sample_idx;

        if (column_value.is_null()) {
          ++null_value_count;
        } else {
          distinct_set.insert(column_value.value());
          min = std::min(min, column_value.value());
          max = std::max(max, column_value.value());
        }
      });
    });
  }

  const auto null_value_ratio = table.row_count() > 0 ? static_cast<float>(null_value_count) / static_cast<float>(table.row_count()) : 0.0f;
  const auto sample_ratio = static_cast<float>(table.row_count()) / sample_count;
  const auto distinct_count = static_cast<float>(distinct_set.size()) * sample_ratio;

  if (distinct_count == 0.0f) {
    min = std::numeric_limits<ColumnDataType>::min();
    max = std::numeric_limits<ColumnDataType>::max();
  }

  return std::make_shared<ColumnStatistics<ColumnDataType>>(null_value_ratio, distinct_count, min, max);
}

template <>
std::shared_ptr<AbstractColumnStatistics> generate_column_statistics<std::string>(const Table& table,
                                                                                  const ColumnID column_id, const size_t max_sample_count);

}  // namespace opossum
