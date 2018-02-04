#include "table_statistics_generator.hpp"

#include <set>

#include "storage/table.hpp"
#include "abstract_column_statistics.hpp"
#include "storage/iterables/create_iterable_from_column.hpp"
#include "column_statistics.hpp"
#include "table_statistics.hpp"
#include "resolve_type.hpp"

namespace opossum {

TableStatisticsGenerator::TableStatisticsGenerator(const std::shared_ptr<const Table>& table):
  _table(table) {}

TableStatistics TableStatisticsGenerator::operator()() {
  std::vector<std::shared_ptr<const AbstractColumnStatistics>> column_statistics;

  for (ColumnID column_id{0}; column_id < _table->column_count(); ++column_id) {
    const auto column_data_type = _table->column_types()[column_id];

    resolve_data_type(column_data_type, [&](auto type) {
      using Type = decltype(type);

      std::set<Type> min_max_set;
      auto null_value_count = size_t{0};

      for (ChunkID chunk_id{0}; chunk_id < _table->chunk_count(); ++chunk_id) {
        const auto base_column = _table->get_chunk(chunk_id)->get_column(column_id);

        resolve_column_type(*base_column, [](const auto& column) {
          auto iterable = create_iterable_from_column(column);
          iterable.for_each([&](const auto& column_value) {
            if (column_value.is_null()) {
              ++null_value_count;
            } else {
              min_max_set.insert(column_value.value());
            }
          });
        });
      }

      const auto null_value_ratio = float{null_value_count} / float{_table->row_count()};
      const auto distinct_count = float{min_max_set.size()};
      const auto min = min_max_set.empty() ? std::numeric_limits<float>::min() : *min_max_set.begin();
      const auto max = min_max_set.empty() ? std::numeric_limits<float>::max() : *min_max_set.rbegin();

      column_statistics.emplace_back(std::make_shared<ColumnStatistics<Type>>(null_value_ratio, distinct_count, min, max));
    });
  }

  return {_table->row_count(), std::move(column_statistics)};
}

}  // namespace opossum