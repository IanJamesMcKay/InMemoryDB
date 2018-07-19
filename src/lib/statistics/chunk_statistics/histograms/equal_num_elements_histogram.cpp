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
size_t EqualNumElementsHistogram<T>::_num_buckets() const {
  return _counts.size();
}

template <typename T>
BucketID EqualNumElementsHistogram<T>::_bucket_for_value(const T value) const {
  T cleaned_value = value;

  if constexpr (std::is_same_v<T, std::string>) {
    cleaned_value = value.substr(0, this->_string_prefix_length);
  }

  const auto it = std::lower_bound(_maxs.begin(), _maxs.end(), cleaned_value);
  const auto index = static_cast<BucketID>(std::distance(_maxs.begin(), it));

  if (it == _maxs.end() || cleaned_value < _bucket_min(index) || cleaned_value > _bucket_max(index)) {
    return INVALID_BUCKET_ID;
  }

  return index;
}

template <typename T>
BucketID EqualNumElementsHistogram<T>::_lower_bound_for_value(const T value) const {
  const auto it = std::lower_bound(_maxs.begin(), _maxs.end(), value);
  const auto index = static_cast<BucketID>(std::distance(_maxs.begin(), it));

  if (it == _maxs.end()) {
    return INVALID_BUCKET_ID;
  }

  return index;
}

template <typename T>
BucketID EqualNumElementsHistogram<T>::_upper_bound_for_value(const T value) const {
  const auto it = std::upper_bound(_maxs.begin(), _maxs.end(), value);
  const auto index = static_cast<BucketID>(std::distance(_maxs.begin(), it));

  if (it == _maxs.end()) {
    return INVALID_BUCKET_ID;
  }

  return index;
}

template <typename T>
T EqualNumElementsHistogram<T>::_bucket_min(const BucketID index) const {
  DebugAssert(index < _mins.size(), "Index is not a valid bucket.");
  return _mins[index];
}

template <typename T>
T EqualNumElementsHistogram<T>::_bucket_max(const BucketID index) const {
  DebugAssert(index < _maxs.size(), "Index is not a valid bucket.");
  return _maxs[index];
}

template <typename T>
uint64_t EqualNumElementsHistogram<T>::_bucket_count(const BucketID index) const {
  DebugAssert(index < _counts.size(), "Index is not a valid bucket.");
  return _counts[index];
}

template <typename T>
uint64_t EqualNumElementsHistogram<T>::_bucket_count_distinct(const BucketID index) const {
  return _distinct_count_per_bucket + (index < _num_buckets_with_extra_value ? 1 : 0);
}

template <typename T>
uint64_t EqualNumElementsHistogram<T>::_total_count() const {
  return std::accumulate(_counts.begin(), _counts.end(), 0ul);
}

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

EXPLICITLY_INSTANTIATE_DATA_TYPES(EqualNumElementsHistogram);

}  // namespace opossum
