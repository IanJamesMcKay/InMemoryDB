#include "equal_num_elements_histogram.hpp"

#include <memory>
#include <numeric>

#include "storage/table.hpp"
#include "storage/value_column.hpp"

namespace opossum {

template <typename T>
void EqualNumElementsHistogram<T>::_generate(const ColumnID column_id, const size_t max_num_buckets) {
  const auto result = this->_get_value_counts(column_id);

  if (result->row_count() == 0u) {
    return;
  }

  // TODO(tim): fix
  DebugAssert(result->chunk_count() == 1, "Multiple chunks are currently not supported.");

  // If there are fewer distinct values than the number of desired buckets use that instead.
  const auto distinct_count = result->row_count();
  const auto num_buckets = distinct_count < max_num_buckets ? static_cast<size_t>(distinct_count) : max_num_buckets;

  // Split values evenly among buckets.
  _distinct_count_per_bucket = distinct_count / num_buckets;
  _num_buckets_with_extra_value = distinct_count % num_buckets;

  const auto distinct_column =
          std::static_pointer_cast<const ValueColumn<T>>(result->get_chunk(ChunkID{0})->get_column(ColumnID{0}))->values();
  const auto count_column =
          std::static_pointer_cast<const ValueColumn<int64_t>>(result->get_chunk(ChunkID{0})->get_column(ColumnID{1}))
                  ->values();

  auto begin_index = 0ul;
  for (BucketID bucket_index = 0; bucket_index < num_buckets; bucket_index++) {
    auto end_index = begin_index + _distinct_count_per_bucket - 1;
    if (bucket_index < _num_buckets_with_extra_value) {
      end_index++;
    }

    _mins.emplace_back(*(distinct_column.begin() + begin_index));
    _maxs.emplace_back(*(distinct_column.begin() + end_index));
    _counts.emplace_back(
            std::accumulate(count_column.begin() + begin_index, count_column.begin() + end_index + 1, uint64_t{0}));

    begin_index = end_index + 1;
  }
}

template <>
void EqualNumElementsHistogram<std::string>::_generate(const ColumnID column_id, const size_t max_num_buckets) {
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(EqualNumElementsHistogram);

}  // namespace opossum
