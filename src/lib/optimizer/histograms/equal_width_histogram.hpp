#pragma once

#include <memory>

#include "optimizer/histograms/abstract_histogram.hpp"
#include "types.hpp"

namespace opossum {

class Table;

template <typename T>
class EqualWidthHistogram : public AbstractHistogram<T> {
 public:
  using AbstractHistogram<T>::AbstractHistogram;

  HistogramType histogram_type() const override;

  size_t num_buckets() const override;
  BucketID bucket_for_value(const T value) const override;

  T bucket_min(const BucketID index) const override;
  T bucket_max(const BucketID index) const override;
  uint64_t bucket_count(const BucketID index) const override;
  uint64_t bucket_count_distinct(const BucketID index) const override;

 protected:
  void _generate(const ColumnID column_id, const size_t max_num_buckets) override;

 private:
  T _min;
  T _max;
  std::vector<uint64_t> _counts;
  uint64_t _num_buckets_with_larger_range;
};

}  // namespace opossum
