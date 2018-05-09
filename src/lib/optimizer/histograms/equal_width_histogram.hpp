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

  void generate(const ColumnID column_id, const size_t max_num_buckets) override;

  HistogramType histogram_type() const override;
  BucketID bucket_for_value(const T value) override;
  T bucket_min(const BucketID index) override;
  T bucket_max(const BucketID index) override;
  uint64_t bucket_count_distinct(const BucketID index) override;

 private:
  T _min;
  T _max;
};

}  // namespace opossum
