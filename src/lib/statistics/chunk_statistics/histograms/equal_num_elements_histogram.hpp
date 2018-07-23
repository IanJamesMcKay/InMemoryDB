#pragma once

#include <memory>

#include "abstract_equal_num_elements_histogram.hpp"
#include "abstract_string_histogram_mixin.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
class EqualNumElementsHistogram : public AbstractEqualNumElementsHistogram<T> {
 public:
  using AbstractEqualNumElementsHistogram<T>::AbstractEqualNumElementsHistogram;

 protected:
  void _generate(const ColumnID column_id, const size_t max_num_buckets) override;
};

template <>
class EqualNumElementsHistogram<std::string> :
        public AbstractEqualNumElementsHistogram<int64_t>, public AbstractStringHistogramMixin {
 public:
  using AbstractStringHistogramMixin::AbstractStringHistogramMixin;

 protected:
  void _generate(const ColumnID column_id, const size_t max_num_buckets) override;
};

}  // namespace opossum
