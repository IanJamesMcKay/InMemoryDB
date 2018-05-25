#include "abstract_histogram.hpp"

#include <algorithm>
#include <memory>
#include <vector>

#include "operators/aggregate.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/table.hpp"

namespace opossum {

template <typename T>
AbstractHistogram<T>::AbstractHistogram(const std::shared_ptr<Table> table) : _table(table) {}

template <typename T>
const std::shared_ptr<const Table> AbstractHistogram<T>::_get_value_counts(const ColumnID column_id) const {
  auto table = _table.lock();
  DebugAssert(table != nullptr, "Corresponding table of histogram is deleted.");

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  const auto aggregate_args = std::vector<AggregateColumnDefinition>{{std::nullopt, AggregateFunction::Count}};
  auto aggregate = std::make_shared<Aggregate>(table_wrapper, aggregate_args, std::vector<ColumnID>{column_id});
  aggregate->execute();

  auto sort = std::make_shared<Sort>(aggregate, ColumnID{0});
  sort->execute();

  return sort->get_output();
}

template <typename T>
void AbstractHistogram<T>::generate(const ColumnID column_id, const size_t max_num_buckets) {
  DebugAssert(max_num_buckets > 0u, "Cannot generate histogram with less than one bucket.");
  _generate(column_id, max_num_buckets);
}

template <typename T>
T AbstractHistogram<T>::lower_end() const {
  DebugAssert(num_buckets() > 0u, "Called method on histogram before initialization.");
  return bucket_min(0u);
}

template <typename T>
T AbstractHistogram<T>::upper_end() const {
  DebugAssert(num_buckets() > 0u, "Called method on histogram before initialization.");
  return bucket_max(num_buckets() - 1u);
}

template <typename T>
T AbstractHistogram<T>::bucket_width(const BucketID index) const {
  DebugAssert(index < num_buckets(), "Index is not a valid bucket.");

  if constexpr (std::is_integral_v<T>) {
    return bucket_max(index) - bucket_min(index) + 1;
  }

  if constexpr (std::is_floating_point_v<T>) {
    return bucket_max(index) - bucket_min(index);
  }

  Fail("Histogram type not yet supported.");
}

template <typename T>
float AbstractHistogram<T>::estimate_cardinality(const T value, const PredicateCondition predicate_condition) const {
  DebugAssert(num_buckets() > 0u, "Called method on histogram before initialization.");

  switch (predicate_condition) {
    case PredicateCondition::Equals: {
      const auto index = bucket_for_value(value);

      if (index == INVALID_BUCKET_ID) {
        return 0.f;
      }

      return static_cast<float>(bucket_count(index)) / static_cast<float>(bucket_count_distinct(index));
    }
    case PredicateCondition::NotEquals: {
      const auto index = bucket_for_value(value);

      if (index == INVALID_BUCKET_ID) {
        return total_count();
      }

      return total_count() - static_cast<float>(bucket_count(index)) / static_cast<float>(bucket_count_distinct(index));
    }
    case PredicateCondition::LessThan: {
      if (value > upper_end()) {
        return total_count();
      }

      if (value <= lower_end()) {
        return 0.f;
      }

      auto index = bucket_for_value(value);
      auto cardinality = 0.f;

      if (index == INVALID_BUCKET_ID) {
        // The value is within the range of the histogram, but does not belong to a bucket.
        // Therefore, we need to sum up the counts of all buckets with a max < value.
        index = upper_bound_for_value(value);
      } else {
        // The value is within the range of a bucket.
        // We need to sum up all preceding buckets and add the share of the bucket of the value that it covers.

        // Calculate the share of the bucket that the value covers.
        // DebugAssert(!std::is_same_v<T, std::string>, "LessThan estimation for strings is not yet supported.");
        //
        // const auto bucket_share = static_cast<float>(value - bucket_min(index)) / ;
        // cardinality += bucket_share * bucket_count(index);
      }

      // Sum up all buckets before the bucket (or gap) containing the value.
      for (BucketID bucket = 0u; bucket < index; bucket++) {
        cardinality += bucket_count(bucket);
      }

      return cardinality;
    }
    default:
      Fail("Predicate condition not yet supported.");
  }
}

template <typename T>
bool AbstractHistogram<T>::can_prune(const AllTypeVariant& value, const PredicateCondition predicate_type) const {
  DebugAssert(num_buckets() > 0, "Called method on histogram before initialization.");

  const auto t_value = type_cast<T>(value);

  switch (predicate_type) {
    case PredicateCondition::Equals:
      return bucket_for_value(t_value) == INVALID_BUCKET_ID;
    case PredicateCondition::NotEquals:
      return num_buckets() == 1 && bucket_min(0) == t_value && bucket_max(0) == t_value;
    case PredicateCondition::LessThan:
      return t_value <= lower_end();
    case PredicateCondition::LessThanEquals:
      return t_value < lower_end();
    case PredicateCondition::GreaterThanEquals:
      return t_value > upper_end();
    case PredicateCondition::GreaterThan:
      return t_value >= upper_end();
    // TODO(tim): change signature to support two values
    // talk to Moritz about new expression interface first
    // case PredicateCondition::Between:
    //   return can_prune(value, PredicateCondition::GreaterThanEquals) &&
    //           can_prune(value2, PredicateCondition::LessThanEquals);
    default:
      // Rather than failing we simply do not prune for things we cannot yet handle.
      return false;
  }
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(AbstractHistogram);

}  // namespace opossum
