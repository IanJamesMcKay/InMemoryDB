#pragma once

#include <memory>
#include <vector>

#include "statistics/chunk_statistics/abstract_filter.hpp"
#include "types.hpp"

namespace opossum {

class Table;

template <typename T>
class AbstractHistogram : public AbstractFilter {
 public:
  explicit AbstractHistogram(const std::shared_ptr<Table> table);
  virtual ~AbstractHistogram() = default;

  void generate(const ColumnID column_id, const size_t max_num_buckets);
  float estimate_cardinality(const T value, const PredicateCondition predicate_condition) const;
  bool can_prune(const AllTypeVariant& value, const PredicateCondition predicate_type) const override;

  T lower_end() const;
  T upper_end() const;

  virtual T bucket_width(const BucketID index) const;

  virtual HistogramType histogram_type() const = 0;

  virtual size_t num_buckets() const = 0;
  virtual BucketID bucket_for_value(const T value) const = 0;
  virtual BucketID lower_bound_for_value(const T value) const = 0;
  virtual BucketID upper_bound_for_value(const T value) const = 0;

  virtual T bucket_min(const BucketID index) const = 0;
  virtual T bucket_max(const BucketID index) const = 0;
  virtual uint64_t bucket_count(const BucketID index) const = 0;
  virtual uint64_t bucket_count_distinct(const BucketID index) const = 0;
  virtual uint64_t total_count() const = 0;

 protected:
  const std::shared_ptr<const Table> _get_value_counts(const ColumnID column_id) const;
  virtual void _generate(const ColumnID column_id, const size_t max_num_buckets) = 0;

  const std::weak_ptr<Table> _table;
};

}  // namespace opossum
