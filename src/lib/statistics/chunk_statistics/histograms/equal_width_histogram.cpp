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
T EqualWidthHistogram<T>::_bucket_width([[maybe_unused]] const BucketID index) const {
  DebugAssert(index < _num_buckets(), "Index is not a valid bucket.");

  // TODO(tim): support strings
  const auto base_width = this->_next_value(_max - _min) / this->_num_buckets();

  if constexpr (std::is_integral_v<T>) {
    return base_width + (index < _num_buckets_with_larger_range ? 1 : 0);
  }

  return base_width;
}

template <typename T>
size_t EqualWidthHistogram<T>::_num_buckets() const {
  return _counts.size();
}

template <typename T>
BucketID EqualWidthHistogram<T>::_bucket_for_value(const T value) const {
  if (value < _min || value > _max) {
    return INVALID_BUCKET_ID;
  }

  if (_num_buckets_with_larger_range == 0u || value <= _bucket_max(_num_buckets_with_larger_range - 1u)) {
    // All buckets up to that point have the exact same width, so we can use index 0.
    return (value - _min) / _bucket_width(0u);
  }

  // All buckets after that point have the exact same width as well, so we use that as the new base and add it up.
  return _num_buckets_with_larger_range +
         (value - _bucket_min(_num_buckets_with_larger_range)) / _bucket_width(_num_buckets_with_larger_range);
}

template <typename T>
BucketID EqualWidthHistogram<T>::_lower_bound_for_value(const T value) const {
  if (value < _min) {
    return 0u;
  }

  return _bucket_for_value(value);
}

template <typename T>
BucketID EqualWidthHistogram<T>::_upper_bound_for_value(const T value) const {
  if (value < _min) {
    return 0u;
  }

  const auto index = _bucket_for_value(value);
  return index < _num_buckets() - 2 ? index + 1 : INVALID_BUCKET_ID;
}

template <typename T>
T EqualWidthHistogram<T>::_bucket_min(const BucketID index) const {
  DebugAssert(index < _num_buckets(), "Index is not a valid bucket.");

  const auto base_min = _min + index * _bucket_width(index);

  if constexpr (std::is_integral_v<T>) {
    return base_min + std::min(index, _num_buckets_with_larger_range);
  }

  if constexpr (std::is_floating_point_v<T>) {
    return base_min;
  }

  // TODO(tim): support strings
  Fail("Histogram type not yet supported.");
}

template <typename T>
T EqualWidthHistogram<T>::_bucket_max(const BucketID index) const {
  DebugAssert(index < _num_buckets(), "Index is not a valid bucket.");

  // If it's the last bucket, return max.
  if (index == _num_buckets() - 1) {
    return _max;
  }

  // Otherwise it is the value just before the minimum of the next bucket.
  return this->_previous_value(_bucket_min(index + 1));
}

template <typename T>
uint64_t EqualWidthHistogram<T>::_bucket_count(const BucketID index) const {
  DebugAssert(index < _counts.size(), "Index is not a valid bucket.");
  return _counts[index];
}

template <typename T>
uint64_t EqualWidthHistogram<T>::_total_count() const {
  return std::accumulate(_counts.begin(), _counts.end(), 0ul);
}

template <typename T>
uint64_t EqualWidthHistogram<T>::_bucket_count_distinct(const BucketID index) const {
  DebugAssert(index < _distinct_counts.size(), "Index is not a valid bucket.");
  return _distinct_counts[index];
}

template <typename T>
void EqualWidthHistogram<T>::_generate(const ColumnID column_id, const size_t max_num_buckets) {
  const auto result = this->_get_value_counts(column_id);

  if (result->row_count() == 0u) {
    return;
  }

  // TODO(tim): fix
  DebugAssert(result->chunk_count() == 1, "Multiple chunks are currently not supported.");

  const auto distinct_column =
      std::static_pointer_cast<const ValueColumn<T>>(result->get_chunk(ChunkID{0})->get_column(ColumnID{0}));
  const auto count_column =
      std::static_pointer_cast<const ValueColumn<int64_t>>(result->get_chunk(ChunkID{0})->get_column(ColumnID{1}));

  // Buckets shall have the same range.
  _min = distinct_column->get(0);
  _max = distinct_column->get(distinct_column->size() - 1u);

  const T base_width = _max - _min;
  // TODO(tim): this only works for numerical types, support strings
  const T bucket_width = this->_next_value(base_width) / max_num_buckets;

  if constexpr (std::is_integral_v<T>) {
    _num_buckets_with_larger_range = (base_width + 1) % max_num_buckets;
  } else {
    _num_buckets_with_larger_range = 0u;
  }

  T current_begin_value = _min;
  auto current_begin_it = distinct_column->values().begin();
  auto current_begin_index = 0l;
  for (auto current_bucket_id = 0u; current_bucket_id < max_num_buckets; current_bucket_id++) {
    T next_begin_value = current_begin_value + bucket_width;
    T current_end_value = this->_previous_value(next_begin_value);

    if constexpr (std::is_integral_v<T>) {
      if (current_bucket_id < _num_buckets_with_larger_range) {
        current_end_value++;
        next_begin_value++;
      }
    }

    // TODO(tim): think about replacing with binary search (same for other hists)
    auto next_begin_it = current_begin_it;
    while (next_begin_it != distinct_column->values().end() && *next_begin_it <= current_end_value) {
      next_begin_it++;
    }

    const auto next_begin_index = std::distance(distinct_column->values().begin(), next_begin_it);
    _counts.emplace_back(std::accumulate(count_column->values().begin() + current_begin_index,
                                         count_column->values().begin() + next_begin_index, uint64_t{0}));
    _distinct_counts.emplace_back(next_begin_index - current_begin_index);

    current_begin_value = next_begin_value;
    current_begin_index = next_begin_index;
  }
}

// These histograms only make sense for data types for which there is a discreet number of elements in a range.
template class EqualWidthHistogram<int32_t>;
template class EqualWidthHistogram<int64_t>;
template class EqualWidthHistogram<float>;
template class EqualWidthHistogram<double>;

}  // namespace opossum
