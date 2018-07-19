#pragma once

#include <memory>
#include <vector>

#include "types.hpp"

namespace opossum {

class Table;

class AbstractStringHistogramMixin {
 // public:
  // AbstractStringHistogramMixin(const uint8_t string_prefix_length);
  // virtual ~AbstractStringHistogramMixin() = default;

 protected:
  uint64_t _convert_string_to_number_representation(const std::string& value) const;

 protected:
  // TODO(tim): figure out how to have this in constructor
  const uint8_t _string_prefix_length = 13;
  const std::string _supported_characters = "abcdefghijklmnopqrstuvwxyz";
};

}  // namespace opossum
