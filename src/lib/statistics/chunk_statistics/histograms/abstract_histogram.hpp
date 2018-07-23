#pragma once

#include <memory>
#include <vector>

#include "statistics/chunk_statistics/abstract_filter.hpp"
#include "types.hpp"

namespace opossum {

class Table;

template <typename T>
class AbstractHistogram : public AbstractFilter {
  friend class HistogramPrivateTest;

 public:
  AbstractHistogram(const std::shared_ptr<Table> table, const uint8_t string_prefix_length);
  virtual ~AbstractHistogram() = default;

  virtual HistogramType histogram_type() const = 0;

  void generate(const ColumnID column_id, const size_t max_num_buckets);
  float estimate_cardinality(const T value, const PredicateCondition predicate_condition) const;
  bool can_prune(const AllTypeVariant& value, const PredicateCondition predicate_type) const override;

 protected:
  const std::shared_ptr<const Table> _get_value_counts(const ColumnID column_id) const;
  virtual void _generate(const ColumnID column_id, const size_t max_num_buckets) = 0;

  T _lower_end() const;
  T _upper_end() const;

  T _previous_value(const T value) const;
  T _next_value(const T value, const bool overflow = true) const;
  uint64_t _convert_string_to_number_representation(const std::string& value) const;
  std::string _convert_number_representation_to_string(const uint64_t) const;

  virtual T _bucket_width(const BucketID index) const;

  virtual size_t _num_buckets() const = 0;
  virtual BucketID _bucket_for_value(const T value) const = 0;
  virtual BucketID _lower_bound_for_value(const T value) const = 0;
  virtual BucketID _upper_bound_for_value(const T value) const = 0;

  virtual T _bucket_min(const BucketID index) const = 0;
  virtual T _bucket_max(const BucketID index) const = 0;
  virtual uint64_t _bucket_count(const BucketID index) const = 0;
  virtual uint64_t _bucket_count_distinct(const BucketID index) const = 0;
  virtual uint64_t _total_count() const = 0;

  const std::weak_ptr<Table> _table;
  const uint8_t _string_prefix_length;
  const std::string _supported_characters = "abcdefghijklmnopqrstuvwxyz";
};

}  // namespace opossum
