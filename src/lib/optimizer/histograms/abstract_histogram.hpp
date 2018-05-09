#pragma once

#include <memory>
#include <vector>

#include "types.hpp"

namespace opossum {

class Table;

template <typename T>
class AbstractHistogram {
 public:
  explicit AbstractHistogram(const std::shared_ptr<Table> table);
  virtual ~AbstractHistogram() = default;

  virtual HistogramType histogram_type() const = 0;

  virtual void generate(const ColumnID column_id, const size_t max_num_buckets) = 0;

  float estimate_cardinality(const T value, const PredicateCondition predicate_condition);

  size_t num_buckets() const;
  virtual BucketID bucket_for_value(const T value);

  virtual T bucket_min(const BucketID index);
  virtual T bucket_max(const BucketID index);
  virtual uint64_t bucket_count(const BucketID index);
  virtual uint64_t bucket_count_distinct(const BucketID index);

 protected:
  const std::shared_ptr<const Table> _get_value_counts(const ColumnID column_id) const;

  const std::weak_ptr<Table> _table;
  std::vector<T> _mins;
  std::vector<T> _maxs;
  std::vector<uint64_t> _count_distincts;
  std::vector<uint64_t> _counts;
};

}  // namespace opossum
