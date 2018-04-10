#include "generate_column_statistics.hpp"

#include "storage/table.hpp"

namespace opossum {

template<typename ColumnDataType>
ColumnStatisticsGenerator<ColumnDataType>::ColumnStatisticsGenerator(const Table& table,
                                                                     const ColumnID column_id,
                                                                     const size_t sample_count_hint):
  _table(table),
  _column_id(column_id),
  // sample_count_hint == 0 means "sample entire table"
  _sample_count_hint(sample_count_hint > 0 ? sample_count_hint : table.row_count()) {
}

template<typename ColumnDataType>
typename ColumnStatisticsGenerator<ColumnDataType>::Result ColumnStatisticsGenerator<ColumnDataType>::operator()() {
  init();

  // Early out if there are no Rows in the table. Code below relies on _table.row_count() != 0
  if (_table.empty()) {
    return _result;
  }

  const auto sampling_ratio_requested = static_cast<float>(std::min(_table.row_count(), _sample_count_hint)) / _table.row_count();

  for (ChunkID chunk_id{0}; chunk_id < _table.chunk_count(); ++chunk_id) {
    const auto base_column = _table.get_chunk(chunk_id)->get_column(_column_id);

    resolve_column_type<ColumnDataType>(*base_column, [&](auto& column) {
      auto iterable = create_iterable_from_column<ColumnDataType>(column);

      if (sampling_ratio_requested > FULL_SCAN_THRESHOLD) {
        // Sample the entire Chunk
        iterable.for_each([&](const auto& column_value) {
          process_sample(column_value);
        });
        _result.sample_count += column.size();
      } else {
        // Determine the number of samples we're gonna take from this Chunk, making sure its at least one and max
        // the number of rows in this Chunk.
        const auto sample_count_this_chunk = std::min(column.size(),
                                                      static_cast<size_t>(std::floor(static_cast<float>(column.size()) / _table.row_count()) *  _sample_count_hint)
        + 1);

        const auto sample_row_step = static_cast<float>(column.size()) / sample_count_this_chunk;

        ChunkOffsetsList chunk_sample_list;
        chunk_sample_list.reserve(sample_count_this_chunk);
        for (auto row_idx_float = float(0); row_idx_float < column.size(); row_idx_float += sample_row_step) {
          ChunkOffsetMapping mapping;
          mapping.into_referenced = static_cast<ChunkOffset>(row_idx_float);
          mapping.into_referencing = INVALID_CHUNK_OFFSET;
          chunk_sample_list.emplace_back(mapping);
        }

        iterable.for_each(&chunk_sample_list, [&](const auto& column_value) {
          process_sample(column_value);
        });

        _result.sample_count += chunk_sample_list.size();
      }
    });
  }

  return _result;
}

template<typename ColumnDataType>
void ColumnStatisticsGenerator<ColumnDataType>::init() {
  _result.min = std::numeric_limits<ColumnDataType>::max();
  _result.max = std::numeric_limits<ColumnDataType>::lowest();
}

template<>
void ColumnStatisticsGenerator<std::string>::init() {}

template<typename ColumnDataType>
template<typename Value>
void ColumnStatisticsGenerator<ColumnDataType>::process_sample(const Value& value) {
  if (value.is_null()) {
    ++_result.null_value_count;
  } else {
    _result.distinct_set.insert(value.value());
    _result.min = std::min(_result.min, value.value());
    _result.max = std::max(_result.max, value.value());
  }
}

template<>
template<typename Value>
void ColumnStatisticsGenerator<std::string>::process_sample(const Value& value) {
  if (value.is_null()) {
    ++_result.null_value_count;
  } else {
    if (_result.distinct_set.empty()) {
      _result.min = value.value();
      _result.max = value.value();
    } else {
      _result.min = std::min(_result.min, value.value());
      _result.max = std::max(_result.max, value.value());
    }

    _result.distinct_set.insert(value.value());
  }
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(ColumnStatisticsGenerator);
}  // namespace opossum
