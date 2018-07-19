#include "abstract_equal_num_elements_histogram.hpp"

#include <memory>
#include <numeric>

#include "storage/table.hpp"
#include "storage/value_column.hpp"

namespace opossum {

template <typename T>
HistogramType AbstractEqualNumElementsHistogram<T>::histogram_type() const {
  return HistogramType::EqualNumElements;
}

template <typename T>
size_t AbstractEqualNumElementsHistogram<T>::_num_buckets() const {
  return _counts.size();
}

template <typename T>
BucketID AbstractEqualNumElementsHistogram<T>::_bucket_for_value(const T value) const {
  T cleaned_value = value;

  const auto it = std::lower_bound(_maxs.begin(), _maxs.end(), cleaned_value);
  const auto index = static_cast<BucketID>(std::distance(_maxs.begin(), it));

  if (it == _maxs.end() || cleaned_value < _bucket_min(index) || cleaned_value > _bucket_max(index)) {
    return INVALID_BUCKET_ID;
  }

  return index;
}

template <typename T>
BucketID AbstractEqualNumElementsHistogram<T>::_lower_bound_for_value(const T value) const {
  const auto it = std::lower_bound(_maxs.begin(), _maxs.end(), value);
  const auto index = static_cast<BucketID>(std::distance(_maxs.begin(), it));

  if (it == _maxs.end()) {
    return INVALID_BUCKET_ID;
  }

  return index;
}

template <typename T>
BucketID AbstractEqualNumElementsHistogram<T>::_upper_bound_for_value(const T value) const {
  const auto it = std::upper_bound(_maxs.begin(), _maxs.end(), value);
  const auto index = static_cast<BucketID>(std::distance(_maxs.begin(), it));

  if (it == _maxs.end()) {
    return INVALID_BUCKET_ID;
  }

  return index;
}

template <typename T>
T AbstractEqualNumElementsHistogram<T>::_bucket_min(const BucketID index) const {
  DebugAssert(index < _mins.size(), "Index is not a valid bucket.");
  return _mins[index];
}

template <typename T>
T AbstractEqualNumElementsHistogram<T>::_bucket_max(const BucketID index) const {
  DebugAssert(index < _maxs.size(), "Index is not a valid bucket.");
  return _maxs[index];
}

template <typename T>
uint64_t AbstractEqualNumElementsHistogram<T>::_bucket_count(const BucketID index) const {
  DebugAssert(index < _counts.size(), "Index is not a valid bucket.");
  return _counts[index];
}

template <typename T>
uint64_t AbstractEqualNumElementsHistogram<T>::_bucket_count_distinct(const BucketID index) const {
  return _distinct_count_per_bucket + (index < _num_buckets_with_extra_value ? 1 : 0);
}

template <typename T>
uint64_t AbstractEqualNumElementsHistogram<T>::_total_count() const {
  return std::accumulate(_counts.begin(), _counts.end(), 0ul);
}

// EXPLICITLY_INSTANTIATE_DATA_TYPES(AbstractEqualNumElementsHistogram);
template class AbstractEqualNumElementsHistogram<int32_t>;
template class AbstractEqualNumElementsHistogram<int64_t>;
template class AbstractEqualNumElementsHistogram<float>;
template class AbstractEqualNumElementsHistogram<double>;
// Used for strings.
template class AbstractEqualNumElementsHistogram<uint64_t>;

}  // namespace opossum
