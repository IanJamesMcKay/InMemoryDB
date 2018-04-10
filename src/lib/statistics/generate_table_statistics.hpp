#pragma once

#include <numeric>
#include <memory>
#include <unordered_set>

#include "table_statistics.hpp"

namespace opossum {

class Table;

/**
 * Generate statistics about a Table by analysing its entire data. This may be slow, use with caution.
 */
TableStatistics generate_table_statistics(const Table& table, const size_t max_sample_count = std::numeric_limits<size_t>::max());

/**
 * Generate statistics about a Table by looking at every `max(1, <table_row_count> / <max_sample_count>`th row
 */
TableStatistics generate_sampled_table_statistics(const Table& table, const size_t sample_count_hint);

}  // namespace opossum
