#include "table_generator2.hpp"

#include <algorithm>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "tbb/concurrent_vector.h"

#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"
#include "resolve_type.hpp"

#include "types.hpp"

namespace opossum {

template<typename T> struct UniformDistribution {};
template<> struct UniformDistribution<float> { using type = std::uniform_real_distribution<float>; };
template<> struct UniformDistribution<double> { using type = std::uniform_real_distribution<double>; };
template<> struct UniformDistribution<int32_t> { using type = std::uniform_int_distribution<int32_t>; };
template<> struct UniformDistribution<int64_t> { using type = std::uniform_int_distribution<int64_t>; };

TableGenerator2::TableGenerator2(const TableGenerator2ColumnDefinitions& column_definitions, const size_t num_rows_per_chunk, const size_t chunk_count):
  _column_definitions(column_definitions), _num_rows_per_chunk(num_rows_per_chunk), _chunk_count(chunk_count)
{}

std::shared_ptr<Table> TableGenerator2::generate_table() const {
  TableColumnDefinitions table_column_definitions;
  for (size_t i = 0; i < _column_definitions.size(); i++) {
    auto column_name = std::string(1, static_cast<char>(static_cast<int>('a') + i));
    table_column_definitions.emplace_back(column_name, _column_definitions[i].data_type);
  }
  const auto table = std::make_shared<Table>(table_column_definitions, TableType::Data, _num_rows_per_chunk);

  for (ChunkID chunk_id{0}; chunk_id < _chunk_count; ++chunk_id) {
    ChunkColumns columns;

    for (ColumnID column_id{0}; column_id < _column_definitions.size(); ++column_id) {
      resolve_data_type(_column_definitions[column_id].data_type, [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;
        if constexpr(std::is_integral_v<ColumnDataType> || std::is_floating_point_v<ColumnDataType>) {
          const auto min_value = boost::get<ColumnDataType>(_column_definitions[column_id].min_value);
          const auto max_value = boost::get<ColumnDataType>(_column_definitions[column_id].max_value);

          std::default_random_engine engine;
          typename UniformDistribution<ColumnDataType>::type dist(min_value, max_value);

          pmr_concurrent_vector<ColumnDataType> values;
          values.reserve(_num_rows_per_chunk);

          for (ChunkOffset chunk_offset{0}; chunk_offset < _num_rows_per_chunk; ++chunk_offset) {
            values.push_back(dist(engine));
          }

          columns.emplace_back(std::make_shared<ValueColumn<ColumnDataType>>(std::move(values)));
        }
      });
    }

    table->append_chunk(columns);
  }

  return table;
}
}  // namespace opossum
