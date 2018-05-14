#pragma once

#include <memory>

#include "optimizer/histograms/abstract_histogram.hpp"
#include "types.hpp"

namespace opossum {

class Table;

template <typename T>
class EqualNumElementsHistogram : public AbstractHistogram<T> {
 public:
  using AbstractHistogram<T>::AbstractHistogram;

  void generate(const ColumnID column_id, const size_t max_num_buckets) override;

  HistogramType histogram_type() const override;

  /**
   * Returns the number of distinct values that are part of this bucket.
   * This number is precise for the state of the table at time of generation.
   */
  uint64_t bucket_count_distinct(const BucketID index) override;

 private:
  uint64_t _distinct_count_per_bucket;
  uint64_t _num_buckets_with_extra_value;
};

}  // namespace opossum
