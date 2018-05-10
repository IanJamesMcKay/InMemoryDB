#pragma once

// not defined in types.hpp to save your compile time ;)

namespace opossum {

// bloom filter
using BloomFilterSizeType = uint8_t;
constexpr BloomFilterSizeType bloom_filter_size = 100;
using BloomBitset = std::bitset<bloom_filter_size>;
using BloomFilter = std::pair<BloomFilterSizeType, BloomBitset>;
using UserBloomFilter = std::vector<BloomFilter>;
using TableBloomFilter = std::vector<UserBloomFilter>;

}  // namespace opossum
