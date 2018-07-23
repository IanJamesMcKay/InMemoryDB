#include "equal_height_histogram.hpp"

#include <memory>
#include <numeric>

#include "storage/table.hpp"
#include "storage/value_column.hpp"

namespace opossum {

template <typename T>
HistogramType EqualHeightHistogram<T>::histogram_type() const {
  return HistogramType::EqualHeight;
}

template <typename T>
size_t EqualHeightHistogram<T>::_num_buckets() const {
  return _maxs.size();
}

template <typename T>
BucketID EqualHeightHistogram<T>::_bucket_for_value(const T value) const {
  const auto it = std::lower_bound(_maxs.begin(), _maxs.end(), value);
  const auto index = static_cast<BucketID>(std::distance(_maxs.begin(), it));

  if (it == _maxs.end() || value < _min) {
    return INVALID_BUCKET_ID;
  }

  return index;
}

template <typename T>
BucketID EqualHeightHistogram<T>::_lower_bound_for_value(const T value) const {
  const auto it = std::lower_bound(_maxs.begin(), _maxs.end(), value);
  const auto index = static_cast<BucketID>(std::distance(_maxs.begin(), it));

  if (it == _maxs.end()) {
    return INVALID_BUCKET_ID;
  }

  return index;
}

template <typename T>
BucketID EqualHeightHistogram<T>::_upper_bound_for_value(const T value) const {
  const auto it = std::upper_bound(_maxs.begin(), _maxs.end(), value);
  const auto index = static_cast<BucketID>(std::distance(_maxs.begin(), it));

  if (it == _maxs.end()) {
    return INVALID_BUCKET_ID;
  }

  return index;
}

template <typename T>
T EqualHeightHistogram<T>::_bucket_min(const BucketID index) const {
  DebugAssert(index < _maxs.size(), "Index is not a valid bucket.");

  // If it's the first bucket, return _min.
  if (index == 0u) {
    return _min;
  }

  return this->_next_value(this->_bucket_max(index - 1));
}

template <typename T>
T EqualHeightHistogram<T>::_bucket_max(const BucketID index) const {
  DebugAssert(index < _maxs.size(), "Index is not a valid bucket.");
  return _maxs[index];
}

template <typename T>
uint64_t EqualHeightHistogram<T>::_bucket_count(const BucketID index) const {
  DebugAssert(index < this->_num_buckets(), "Index is not a valid bucket.");
  return _count_per_bucket;
}

template <typename T>
uint64_t EqualHeightHistogram<T>::_bucket_count_distinct(const BucketID index) const {
  DebugAssert(index < _distinct_counts.size(), "Index is not a valid bucket.");
  return _distinct_counts[index];
}

template <typename T>
uint64_t EqualHeightHistogram<T>::total_count() const {
  return this->_num_buckets() * _count_per_bucket;
}

template <typename T>
void EqualHeightHistogram<T>::_generate(const ColumnID column_id, const size_t max_num_buckets) {
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

  _min = distinct_column->get(0u);

  auto table = this->_table.lock();
  DebugAssert(table != nullptr, "Corresponding table of histogram is deleted.");

  // Buckets shall have (approximately) the same height.
  _count_per_bucket = table->row_count() / max_num_buckets;

  if (table->row_count() % max_num_buckets > 0u) {
    // Add 1 so that we never create more buckets than requested.
    _count_per_bucket++;
  }

  auto current_begin = 0u;
  auto current_height = 0u;
  for (auto current_end = 0u; current_end < distinct_column->size(); current_end++) {
    current_height += count_column->get(current_end);

    if (current_height >= _count_per_bucket) {
      _maxs.emplace_back(distinct_column->get(current_end));
      _distinct_counts.emplace_back(current_end - current_begin + 1);
      current_height = 0u;
      current_begin = current_end + 1;
    }
  }

  if (current_height > 0u) {
    _maxs.emplace_back(distinct_column->get(distinct_column->size() - 1));
    _distinct_counts.emplace_back(distinct_column->size() - current_begin);
  }
}

// These histograms only make sense for data types for which there is a discreet number of elements in a range.
template class EqualHeightHistogram<int32_t>;
template class EqualHeightHistogram<int64_t>;
template class EqualHeightHistogram<float>;
template class EqualHeightHistogram<double>;

}  // namespace opossum
