#pragma once

#include <memory>

#include "abstract_histogram.hpp"
#include "types.hpp"

namespace opossum {

class Table;

template <typename T>
class EqualNumElementsHistogram : public AbstractHistogram<T> {
 public:
  using AbstractHistogram<T>::AbstractHistogram;

  HistogramType histogram_type() const override;

  size_t num_buckets() const override;
  BucketID bucket_for_value(const T value) const override;
  BucketID lower_bound_for_value(const T value) const override;
  BucketID upper_bound_for_value(const T value) const override;

  T bucket_min(const BucketID index) const override;
  T bucket_max(const BucketID index) const override;
  uint64_t bucket_count(const BucketID index) const override;
  uint64_t total_count() const override;

  /**
   * Returns the number of distinct values that are part of this bucket.
   * This number is precise for the state of the table at time of generation.
   */
  uint64_t bucket_count_distinct(const BucketID index) const override;

 protected:
  void _generate(const ColumnID column_id, const size_t max_num_buckets) override;

 private:
  std::vector<T> _mins;
  std::vector<T> _maxs;
  std::vector<uint64_t> _counts;
  uint64_t _distinct_count_per_bucket;
  uint64_t _num_buckets_with_extra_value;
};

}  // namespace opossum
