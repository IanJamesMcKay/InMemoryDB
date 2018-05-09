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
  uint64_t bucket_count_distinct(const size_t index) override;

 private:
  uint64_t _values_per_bucket;
};

}  // namespace opossum
