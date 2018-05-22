#include "equal_width_histogram.hpp"

#include <memory>
#include <numeric>

#include "storage/table.hpp"
#include "storage/value_column.hpp"

namespace opossum {

template <typename T>
HistogramType EqualWidthHistogram<T>::histogram_type() const {
  return HistogramType::EqualWidth;
}

template <typename T>
size_t EqualWidthHistogram<T>::num_buckets() const {
  return _counts.size();
}

template <typename T>
BucketID EqualWidthHistogram<T>::bucket_for_value(const T value) const {
  if (value < _min || value > _max) {
    return INVALID_BUCKET_ID;
  }

  if (_num_buckets_with_larger_range == 0u || value <= bucket_max(_num_buckets_with_larger_range - 1u)) {
    // All buckets up to that point have the exact same width (i.e., number of distinct values), so we can use index 0.
    return (value - _min) / bucket_count_distinct(0u);
  }

  // All buckets after that point have the exact same width as well, so we use that as the new base and add it up.
  return _num_buckets_with_larger_range +
         (value - bucket_min(_num_buckets_with_larger_range)) / bucket_count_distinct(_num_buckets_with_larger_range);
}

template <typename T>
BucketID EqualWidthHistogram<T>::lower_bound_for_value(const T value) const {
  if (value < _min) {
    return 0u;
  }

  return bucket_for_value(value);
}

template <typename T>
BucketID EqualWidthHistogram<T>::upper_bound_for_value(const T value) const {
  if (value < _min) {
    return 0u;
  }

  const auto index = bucket_for_value(value);
  return index < num_buckets() - 2 ? index + 1 : INVALID_BUCKET_ID;
}

template <typename T>
T EqualWidthHistogram<T>::bucket_min(const BucketID index) const {
  DebugAssert(index < this->num_buckets(), "Index is not a valid bucket.");
  const auto base_index = _min + index * bucket_count_distinct(index);
  if (index < _num_buckets_with_larger_range) {
    return base_index;
  }
  return base_index + _num_buckets_with_larger_range;
}

template <typename T>
T EqualWidthHistogram<T>::bucket_max(const BucketID index) const {
  DebugAssert(index < this->num_buckets(), "Index is not a valid bucket.");

  // If it's the last bucket, return max.
  if (index == this->num_buckets() - 1) {
    return _max;
  }

  // Otherwise it is the value just before the minimum of the next bucket.
  return bucket_min(index + 1) - 1;
}

template <typename T>
uint64_t EqualWidthHistogram<T>::bucket_count(const BucketID index) const {
  DebugAssert(index < _counts.size(), "Index is not a valid bucket.");
  return _counts[index];
}

template <typename T>
uint64_t EqualWidthHistogram<T>::total_count() const {
  return std::accumulate(_counts.begin(), _counts.end(), 0ul);
}

template <typename T>
uint64_t EqualWidthHistogram<T>::bucket_count_distinct(const BucketID index) const {
  DebugAssert(index < this->num_buckets(), "Index is not a valid bucket.");
  const auto count_distinct = (_max - _min + 1) / this->num_buckets();
  return count_distinct + (index < _num_buckets_with_larger_range ? 1 : 0);
}

template <typename T>
void EqualWidthHistogram<T>::_generate(const ColumnID column_id, const size_t max_num_buckets) {
  const auto result = this->_get_value_counts(column_id);

  if (result->row_count() == 0u) {
    return;
  }

  // TODO(tim): fix
  DebugAssert(result->chunk_count() == 1, "Multiple chunks are currently not supported.");

  // If there are fewer distinct values than the number of desired buckets use that instead.
  const auto distinct_count = result->row_count();
  const auto num_buckets = distinct_count < max_num_buckets ? static_cast<size_t>(distinct_count) : max_num_buckets;

  const auto distinct_column =
      std::static_pointer_cast<const ValueColumn<T>>(result->get_chunk(ChunkID{0})->get_column(ColumnID{0}));
  const auto count_column =
      std::static_pointer_cast<const ValueColumn<int64_t>>(result->get_chunk(ChunkID{0})->get_column(ColumnID{1}));

  // Buckets shall have the same range.
  _min = distinct_column->get(0);
  _max = distinct_column->get(result->row_count() - 1);
  const T bucket_width = (_max - _min + 1) / num_buckets;
  _num_buckets_with_larger_range = (_max - _min + 1) % num_buckets;

  auto current_begin = distinct_column->values().begin();
  auto current_end = distinct_column->values().begin();
  T begin_value = _min;
  for (size_t bucket_index = 0; bucket_index < num_buckets; bucket_index++) {
    T end_value = begin_value + bucket_width - 1;

    if (bucket_index < _num_buckets_with_larger_range) {
      end_value++;
    }

    while (current_end + 1 != distinct_column->values().end() && *(current_end + 1) <= end_value) {
      current_end++;
    }

    const auto begin_index = std::distance(distinct_column->values().begin(), current_begin);
    const auto end_index = std::distance(distinct_column->values().begin(), current_end);

    _counts.emplace_back(std::accumulate(count_column->values().begin() + begin_index,
                                         count_column->values().begin() + end_index + 1, uint64_t{0}));

    current_begin = current_end + 1;
    begin_value = end_value + 1;
  }
}

// These histograms only make sense for data types for which there is a discreet number of elements in a range.
template class EqualWidthHistogram<int32_t>;
template class EqualWidthHistogram<int64_t>;

}  // namespace opossum
