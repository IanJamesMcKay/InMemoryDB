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
size_t AbstractHistogram<T>::num_buckets() const {
  return _counts.size();
}

template <typename T>
T AbstractHistogram<T>::bucket_min(const size_t index) {
  DebugAssert(static_cast<uint64_t>(index) < _mins.size(), "Index is not a valid bucket.");
  return _mins[index];
}

template <typename T>
T AbstractHistogram<T>::bucket_max(const size_t index) {
  DebugAssert(static_cast<uint64_t>(index) < _maxs.size(), "Index is not a valid bucket.");
  return _maxs[index];
}

template <typename T>
uint64_t AbstractHistogram<T>::bucket_count(const size_t index) {
  DebugAssert(static_cast<uint64_t>(index) < _counts.size(), "Index is not a valid bucket.");
  return _counts[index];
}

template <typename T>
uint64_t AbstractHistogram<T>::bucket_count_distinct(const size_t index) {
  DebugAssert(static_cast<uint64_t>(index) < _count_distincts.size(), "Index is not a valid bucket.");
  return _count_distincts[index];
}

template <typename T>
size_t AbstractHistogram<T>::bucket_for_value(const T value) {
  const auto it = std::lower_bound(_maxs.begin(), _maxs.end(), value);
  return static_cast<size_t>(std::distance(_maxs.begin(), it));
}

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
};

template <typename T>
float AbstractHistogram<T>::estimate_cardinality(const T value, const PredicateCondition predicate_condition) {
  switch (predicate_condition) {
    case PredicateCondition::Equals: {
      const auto index = bucket_for_value(value);

      if (index >= num_buckets() || bucket_min(index) > value || bucket_max(index) < value) {
        return 0.f;
      }

      return static_cast<float>(bucket_count(index)) / static_cast<float>(bucket_count_distinct(index));
    }
    default:
      Fail("Predicate condition not yet supported.");
  }
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(AbstractHistogram);

}  // namespace opossum
