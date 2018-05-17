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
T AbstractHistogram<T>::lower_bound() const {
  DebugAssert(num_buckets() > 0u, "Called method on histogram before initialization.");
  return bucket_min(0u);
}

template <typename T>
T AbstractHistogram<T>::upper_bound() const {
  DebugAssert(num_buckets() > 0u, "Called method on histogram before initialization.");
  return bucket_max(num_buckets() - 1u);
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
    default:
      Fail("Predicate condition not yet supported.");
  }
}

template <typename T>
bool AbstractHistogram<T>::can_prune(const T value, const PredicateCondition predicate_condition) const {
  DebugAssert(num_buckets() > 0, "Called method on histogram before initialization.");

  switch (predicate_condition) {
    case PredicateCondition::Equals:
      return bucket_for_value(value) == INVALID_BUCKET_ID;
    case PredicateCondition::NotEquals:
      return num_buckets() == 1 && bucket_min(0) == value && bucket_max(0) == value;
    case PredicateCondition::LessThan:
      return value <= lower_bound();
    case PredicateCondition::LessThanEquals:
      return value < lower_bound();
    case PredicateCondition::GreaterThanEquals:
      return value > upper_bound();
    case PredicateCondition::GreaterThan:
      return value >= upper_bound();
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
