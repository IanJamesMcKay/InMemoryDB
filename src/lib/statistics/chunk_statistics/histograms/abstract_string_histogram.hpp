#pragma once

#include <memory>
#include <vector>

#include "statistics/chunk_statistics/histograms/abstract_histogram.hpp"
#include "types.hpp"

namespace opossum {

class Table;

class AbstractStringHistogram : public AbstractHistogram<uint64_t> {
 public:
  AbstractStringHistogram(const std::shared_ptr<Table> table, const uint8_t string_prefix_length);
  virtual ~AbstractStringHistogram() = default;

 protected:
  uint64_t _convert_string_to_number_representation(const std::string& value) const;

 protected:
  const uint8_t _string_prefix_length;
  const std::string _supported_characters = "abcdefghijklmnopqrstuvwxyz";
};

}  // namespace opossum
