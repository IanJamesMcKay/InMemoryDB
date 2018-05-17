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
size_t EqualHeightHistogram<T>::num_buckets() const {
  return _mins.size();
}

template <typename T>
BucketID EqualHeightHistogram<T>::bucket_for_value(const T value) const {
  const auto it = std::upper_bound(_mins.begin(), _mins.end(), value);

  if (it == _mins.begin() || value > _max) {
    return INVALID_BUCKET_ID;
  }

  return static_cast<BucketID>(std::distance(_mins.begin(), it) - 1);
}

template <typename T>
T EqualHeightHistogram<T>::bucket_min(const BucketID index) const {
  DebugAssert(index < _mins.size(), "Index is not a valid bucket.");
  return _mins[index];
}

template <typename T>
T EqualHeightHistogram<T>::bucket_max(const BucketID index) const {
  DebugAssert(index < this->num_buckets(), "Index is not a valid bucket.");

  // If it's the last bucket, return max.
  if (index == this->num_buckets() - 1) {
    return _max;
  }

  // Otherwise it is the value just before the minimum of the next bucket.
  return this->bucket_min(index + 1) - 1;
}

template <typename T>
uint64_t EqualHeightHistogram<T>::bucket_count(const BucketID index) const {
  DebugAssert(index < this->num_buckets(), "Index is not a valid bucket.");
  return _count_per_bucket;
}

template <typename T>
uint64_t EqualHeightHistogram<T>::bucket_count_distinct(const BucketID index) const {
  DebugAssert(index < this->num_buckets(), "Index is not a valid bucket.");
  return bucket_max(index) - this->bucket_min(index) + 1;
}

template <typename T>
void EqualHeightHistogram<T>::_generate(const ColumnID column_id, const size_t max_num_buckets) {
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

  _max = distinct_column->get(result->row_count() - 1);

  // Buckets shall have (approximately) the same height.
  auto table = this->_table.lock();
  DebugAssert(table != nullptr, "Corresponding table of histogram is deleted.");
  // TODO(tim): row_count() is approximate due to MVCC - fix!
  _count_per_bucket = table->row_count() / num_buckets;

  auto current_height = 0u;
  auto current_begin_index = 0u;
  for (auto index = 0u; index < distinct_column->size(); index++) {
    current_height += count_column->get(index);

    if (current_height >= _count_per_bucket) {
      _mins.emplace_back(distinct_column->get(current_begin_index));
      //      _count_distincts.emplace_back(index - current_begin_index + 1);
      current_height = 0u;
      current_begin_index = index + 1;
    }
  }

  if (current_height > 0u) {
    _mins.emplace_back(distinct_column->get(current_begin_index));
    //    _count_distincts.emplace_back(distinct_column->size() - current_begin_index);
  }
}

// These histograms only make sense for data types for which there is a discreet number of elements in a range.
template class EqualHeightHistogram<int32_t>;
template class EqualHeightHistogram<int64_t>;

}  // namespace opossum
