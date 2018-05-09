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
  size_t bucket_for_value(const T value) override;
  T bucket_min(const size_t index) override;
  T bucket_max(const size_t index) override;
  uint64_t bucket_count_distinct(const size_t index) override;

 private:
  T _min;
  T _max;
};

}  // namespace opossum
