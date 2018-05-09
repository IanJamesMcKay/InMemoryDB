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
size_t EqualWidthHistogram<T>::bucket_for_value(const T value) {
  // All buckets have the same number of distinct values, so we can use index 0.
  return value / bucket_count_distinct(0);
}

template <typename T>
T EqualWidthHistogram<T>::bucket_min(const size_t index) {
  DebugAssert(index < this->num_buckets(), "Index is not a valid bucket.");
  return _min + index * bucket_count_distinct(index);
}

template <typename T>
T EqualWidthHistogram<T>::bucket_max(const size_t index) {
  DebugAssert(index < this->num_buckets(), "Index is not a valid bucket.");
  return bucket_min(index) + bucket_count_distinct(index) - 1;
}

template <typename T>
uint64_t EqualWidthHistogram<T>::bucket_count_distinct(const size_t index) {
  DebugAssert(index < this->num_buckets(), "Index is not a valid bucket.");
  return (_max - _min + 1) / this->num_buckets();
}

template <typename T>
void EqualWidthHistogram<T>::generate(const ColumnID column_id, const size_t max_num_buckets) {
  const auto result = this->_get_value_counts(column_id);

  if (result->row_count() == 0u) {
    return;
  }

  // If there are fewer distinct values than the number of desired buckets use that instead.
  const auto distinct_count = result->row_count();
  const auto num_buckets = distinct_count < max_num_buckets ? static_cast<size_t>(distinct_count) : max_num_buckets;

  const auto distinct_values_column =
      std::static_pointer_cast<const ValueColumn<T>>(result->get_chunk(ChunkID{0})->get_column(ColumnID{0}));
  const auto count_column =
      std::static_pointer_cast<const ValueColumn<int64_t>>(result->get_chunk(ChunkID{0})->get_column(ColumnID{1}));

  // TODO(tim): fix
  DebugAssert(result->chunk_count() == 1, "Multiple chunks are currently not supported.");

  // Buckets shall have the same range.
  _min = distinct_values_column->get(0);
  _max = distinct_values_column->get(result->row_count() - 1);
  const T bucket_width = (_max - _min + 1) / num_buckets;
  const auto num_buckets_with_larger_range = (_max - _min + 1) % num_buckets;

  // TODO(tim): fix
  DebugAssert(num_buckets_with_larger_range == 0, "Only evenly distributed buckets are supported right now.");

  auto current_begin = distinct_values_column->values().begin();
  auto current_end = distinct_values_column->values().begin();
  for (size_t bucket_index = 0; bucket_index < num_buckets; bucket_index++) {
    const T begin_value = _min + bucket_index * bucket_width;
    const T end_value = begin_value + bucket_width - 1;

    while (current_end + 1 != distinct_values_column->values().end() && *(current_end + 1) <= end_value) {
      current_end++;
    }

    const auto begin_index = std::distance(distinct_values_column->values().begin(), current_begin);
    const auto end_index = std::distance(distinct_values_column->values().begin(), current_end);

    this->_counts.emplace_back(std::accumulate(count_column->values().begin() + begin_index,
                                               count_column->values().begin() + end_index + 1, uint64_t{0}));

    current_begin = current_end + 1;
  }
}

// These histograms only make sense for data types for which there is a discreet number of elements in a range.
template class EqualWidthHistogram<int32_t>;
template class EqualWidthHistogram<int64_t>;

}  // namespace opossum
