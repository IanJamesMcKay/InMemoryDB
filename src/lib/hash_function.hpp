#pragma once

#include "all_type_variant.hpp"
#include "strong_typedef.hpp"

STRONG_TYPEDEF(uint32_t, HashValue);

namespace opossum {

class HashFunction {
  HashValue calculate_hash(const AllTypeVariant value_to_hash);
};

}  // namespace opossum