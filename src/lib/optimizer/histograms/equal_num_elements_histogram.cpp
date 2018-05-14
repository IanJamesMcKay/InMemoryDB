#include "equal_num_elements_histogram.hpp"

#include <memory>
#include <numeric>

#include "storage/table.hpp"
#include "storage/value_column.hpp"

namespace opossum {

template <typename T>
HistogramType EqualNumElementsHistogram<T>::histogram_type() const {
  return HistogramType::EqualNumElements;
}

template <typename T>
uint64_t EqualNumElementsHistogram<T>::bucket_count_distinct(const BucketID index) {
  return _values_per_bucket;
}

template <typename T>
void EqualNumElementsHistogram<T>::generate(const ColumnID column_id, const size_t max_num_buckets) {
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
  _values_per_bucket = distinct_count / num_buckets;
  const auto num_buckets_with_extra_value = distinct_count % num_buckets;

  // TODO(tim): fix
  DebugAssert(num_buckets_with_extra_value == 0, "Only evenly distributed buckets are supported right now.");

  const auto distinct_column =
      std::static_pointer_cast<const ValueColumn<T>>(result->get_chunk(ChunkID{0})->get_column(ColumnID{0}))->values();
  const auto count_column =
      std::static_pointer_cast<const ValueColumn<int64_t>>(result->get_chunk(ChunkID{0})->get_column(ColumnID{1}))
          ->values();

  for (size_t bucket_index = 0; bucket_index < num_buckets; bucket_index++) {
    const auto begin_index = bucket_index * _values_per_bucket;
    const auto end_index = (bucket_index + 1) * _values_per_bucket - 1;

    this->_mins.emplace_back(*(distinct_column.begin() + begin_index));
    this->_maxs.emplace_back(*(distinct_column.begin() + end_index));
    this->_counts.emplace_back(
        std::accumulate(count_column.begin() + begin_index, count_column.begin() + end_index + 1, uint64_t{0}));
  }
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(EqualNumElementsHistogram);

}  // namespace opossum
