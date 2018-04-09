#include "generate_table_statistics.hpp"

#include <unordered_set>

#include "abstract_column_statistics.hpp"
#include "column_statistics.hpp"
#include "generate_column_statistics.hpp"
#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/table.hpp"
#include "table_statistics.hpp"

namespace opossum {

TableStatistics generate_table_statistics(const Table& table, const size_t max_sample_count) {
  std::vector<std::shared_ptr<const AbstractColumnStatistics>> column_statistics;
  column_statistics.reserve(table.column_count());

  for (ColumnID column_id{0}; column_id < table.column_count(); ++column_id) {
    const auto column_data_type = table.column_data_types()[column_id];

    resolve_data_type(column_data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      column_statistics.emplace_back(generate_column_statistics<ColumnDataType>(table, column_id, max_sample_count));
    });
  }

  return {table.type(), static_cast<float>(table.row_count()), std::move(column_statistics)};
}

//TableStatistics generate_sampled_table_statistics(const Table& table, const size_t max_sample_count) {
//  /**
//   * Create a sampled view on the table
//   */
//  Table sampled_table{table.column_definitions(), TableType::Data};
//
//  const auto num_samples = std::min(table.row_count(), max_sample_count);
//  auto row_idx = size_t{1};
//  auto sample_idx = size_t{0};
//
//  std::vector<std::shared_ptr<BaseColumn>> sampled_columns(table.column_count());
//  for (auto column_id = ColumnID{0}; column_id < table.column_count(); ++column_id) {
//    for (auto chunk_id = ChunkID{0}; chunk_id < table.chunk_count(); ++chunk_id) {
//      resolve_data_and_column_type(table.get_chunk(chunk_id)->get_column(column_id), [&](const auto& column) {
//        using ColumnDataType = typename decltype(type)::type;
//
//        auto iterable = create_iterable_from_column<ColumnDataType>(column);
//
//        ChunkOffset chunk_offset{0};
//        iterable.for_each([&](const auto& value) {
//          if ((row_idx * num_samples) /  > (row_idx ))
//          ++row_idx;
//        });
//      });
//    }
//  }
//
//  for (auto chunk_id = ChunkID{0}; chunk_id < table.chunk_count(); ++chunk_id) {
//
//  }
//
//  /**
//   * Generate statistics for that sampled view
//   */
//  auto statistics = generate_table_statistics(sampled_table);
//
//  /**
//   * Extrapolate statistics to unsampled table
//   */
//  const auto sample_ratio = static_cast<float>(table.row_count()) / num_samples;
//  statistics.set_row_count(table.row_count());
//  for (const auto& column_statistics : statistics.column_statistics()) {
//    // We know we are safe to manipulate these ColumnStatistics because we just created them.
//    std::const_pointer_cast<AbstractColumnStatistics>(column_statistics)->set_distinct_count(column_statistics->distinct_count() * sample_ratio);
//  }
//
//  return statistics;
//}

}  // namespace opossum
