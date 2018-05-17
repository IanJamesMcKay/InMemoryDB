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
  DebugAssert(max_num_buckets > 0, "Cannot generate histogram with less than one bucket.");
  _generate(column_id, max_num_buckets);
}

template <typename T>
float AbstractHistogram<T>::estimate_cardinality(const T value, const PredicateCondition predicate_condition) const {
  DebugAssert(num_buckets() > 0, "Called method on histogram before initialization.");

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

EXPLICITLY_INSTANTIATE_DATA_TYPES(AbstractHistogram);

}  // namespace opossum
