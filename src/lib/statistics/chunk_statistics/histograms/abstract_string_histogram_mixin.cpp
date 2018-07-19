#include "abstract_string_histogram_mixin.hpp"

#include <algorithm>
#include <memory>
#include <vector>

#include "operators/aggregate.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/table.hpp"

namespace opossum {

// AbstractStringHistogramMixin::AbstractStringHistogramMixin(const uint8_t string_prefix_length)
//     : _string_prefix_length(string_prefix_length) {
//   Assert(std::pow(_supported_characters.length(), string_prefix_length) <= std::pow(2, 64), "Prefix too long.");
// }

uint64_t AbstractStringHistogramMixin::_convert_string_to_number_representation(const std::string& value) const {
  const auto trimmed = value.substr(0, _string_prefix_length);

  uint64_t result = 0;
  for (auto it = trimmed.cbegin(); it < trimmed.cend(); it++) {
    const auto power = _string_prefix_length - std::distance(trimmed.cbegin(), it) - 1;
    result += (*it - _supported_characters.front()) * std::pow(_supported_characters.length(), power);
  }

  return result;
}

}  // namespace opossum
