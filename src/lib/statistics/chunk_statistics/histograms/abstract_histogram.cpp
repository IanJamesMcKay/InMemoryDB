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
AbstractHistogram<T>::AbstractHistogram(const std::shared_ptr<Table> table, const uint8_t string_prefix_length)
    : _table(table), _string_prefix_length(string_prefix_length) {
  Assert(std::pow(_supported_characters.length(), string_prefix_length) <= std::pow(2, 64), "Prefix too long.");
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
}

template <typename T>
void AbstractHistogram<T>::generate(const ColumnID column_id, const size_t max_num_buckets) {
  DebugAssert(max_num_buckets > 0u, "Cannot generate histogram with less than one bucket.");
  _generate(column_id, max_num_buckets);
}

template <typename T>
T AbstractHistogram<T>::_lower_end() const {
  DebugAssert(_num_buckets() > 0u, "Called method on histogram before initialization.");
  return _bucket_min(0u);
}

template <typename T>
T AbstractHistogram<T>::_upper_end() const {
  DebugAssert(_num_buckets() > 0u, "Called method on histogram before initialization.");
  return _bucket_max(_num_buckets() - 1u);
}

template <typename T>
T AbstractHistogram<T>::_bucket_width([[maybe_unused]] const BucketID index) const {
  DebugAssert(index < _num_buckets(), "Index is not a valid bucket.");

  if constexpr (std::is_integral_v<T>) {
    return _bucket_max(index) - _bucket_min(index) + 1;
  }

  if constexpr (std::is_floating_point_v<T>) {
    return _bucket_max(index) - _bucket_min(index);
  }

  Fail("Histogram type not yet supported.");
}

template <typename T>
T AbstractHistogram<T>::_previous_value(const T value) const {
  if constexpr (std::is_integral_v<T>) {
    return value - 1;
  }

  if constexpr (std::is_floating_point_v<T>) {
    return std::nextafter(value, value - 1);
  }

  if constexpr (std::is_same_v<T, std::string>) {
    if (value.empty()) {
      return value;
    }

    const auto sub_string = value.substr(0, value.length() - 1);
    const auto last_char = value.back();

    if (last_char == 'a') {
      return sub_string;
    }

    return sub_string + static_cast<char>(last_char - 1);
  }

  Fail("Unsupported data type.");
}

template <typename T>
T AbstractHistogram<T>::_next_value(const T value, const bool overflow) const {
  if constexpr (std::is_integral_v<T>) {
    return value + 1;
  }

  if constexpr (std::is_floating_point_v<T>) {
    return std::nextafter(value, value + 1);
  }

  if constexpr (std::is_same_v<T, std::string>) {
    if (value.empty()) {
      return "a";
    }

    if (value.find_first_not_of(_supported_characters) != std::string::npos) {
      Fail("Unsupported characters.");
    }

    if ((overflow && value.length() < _string_prefix_length) || (value == std::string(_string_prefix_length, 'z'))) {
      return value + 'a';
    }

    const auto last_char = value.back();
    const auto sub_string = value.substr(0, value.length() - 1);

    if (last_char != 'z') {
      return sub_string + static_cast<char>(last_char + 1);
    }

    return _next_value(sub_string, false) + 'a';
  }

  Fail("Unsupported data type.");
}

template <typename T>
uint64_t AbstractHistogram<T>::_convert_string_to_number_representation(const T value) const {
  if constexpr (std::is_same_v<T, std::string>) {
    const auto trimmed = value.substr(0, _string_prefix_length);

    uint64_t result = 0;
    for (auto it = trimmed.cbegin(); it < trimmed.cend(); it++) {
      const auto power = _string_prefix_length - std::distance(trimmed.cbegin(), it) - 1;
      result += (*it - _supported_characters.front()) * std::pow(_supported_characters.length(), power);
    }

    return result;
  }

  Fail("Must not be called outside string histograms.");
}

template <typename T>
float AbstractHistogram<T>::estimate_cardinality(const T value, const PredicateCondition predicate_condition) const {
  DebugAssert(_num_buckets() > 0u, "Called method on histogram before initialization.");

  if constexpr (std::is_same_v<T, std::string>) {
    if (value.find_first_not_of(_supported_characters) != std::string::npos) {
      Fail("Unsupported characters.");
    }
  }

  switch (predicate_condition) {
    case PredicateCondition::Equals: {
      const auto index = _bucket_for_value(value);

      if (index == INVALID_BUCKET_ID) {
        return 0.f;
      }

      return static_cast<float>(_bucket_count(index)) / static_cast<float>(_bucket_count_distinct(index));
    }
    case PredicateCondition::NotEquals: {
      const auto index = _bucket_for_value(value);

      if (index == INVALID_BUCKET_ID) {
        return _total_count();
      }

      return _total_count() -
             static_cast<float>(_bucket_count(index)) / static_cast<float>(_bucket_count_distinct(index));
    }
    case PredicateCondition::LessThan: {
      if (value > _upper_end()) {
        return _total_count();
      }

      if (value <= _lower_end()) {
        return 0.f;
      }

      auto index = _bucket_for_value(value);
      auto cardinality = 0.f;

      if (index == INVALID_BUCKET_ID) {
        // The value is within the range of the histogram, but does not belong to a bucket.
        // Therefore, we need to sum up the counts of all buckets with a max < value.
        index = _upper_bound_for_value(value);
      } else {
        float bucket_share;

        if constexpr (!std::is_same_v<T, std::string>) {
          bucket_share = static_cast<float>(value - _bucket_min(index)) / _bucket_width(index);
        } else {
          /**
           * Calculate range between two strings.
           * This is based on the following assumptions:
           *    - a consecutive byte range, e.g. lower case letters in ASCII
           *    - fixed-length strings
           *
           * Treat the string range similar to the decimal system (base 26 for lower case letters).
           * Characters in the beginning of the string have a higher value than ones at the end.
           * Assign each letter the value of the index in the alphabet (zero-based).
           * Then convert it to a number.
           *
           * Example with fixed-length 4 (possible range: [aaaa, zzzz]):
           *
           *  Number of possible strings: 26**4 = 456,976
           *
           * 1. aaaa - zzzz
           *
           *  repr(aaaa) = 0 * 26**3 + 0 * 26**2 + 0 * 26**1 + 0 * 26**0 = 0
           *  repr(zzzz) = 25 * 26**3 + 25 * 26**2 + 25 * 26**1 + 25 * 26**0 = 456,975
           *  Size of range: repr(zzzz) - repr(aaaa) + 1 = 456,976
           *  Share of the range: 456,976 / 456,976 = 1
           *
           * 2. bhja - mmmm
           *
           *  repr(bhja): 1 * 26**3 + 7 * 26**2 + 9 * 26**1 + 0 * 26**0 = 22,542
           *  repr(mmmm): 12 * 26**3 + 12 * 26**2 + 12 * 26**1 + 12 * 26**0 = 219,348
           *  Size of range: repr(mmmm) - repr(bhja) + 1 = 196,807
           *  Share of the range: 196,807 / 456,976 ~= 0.43
           *
           *  Note that strings shorter than the fixed-length will induce a small error,
           *  because the missing characters will be treated as 'a'.
           *  Since we are dealing with approximations this is acceptable.
           */
          const auto value_repr = _convert_string_to_number_representation(value);
          const auto min_repr = _convert_string_to_number_representation(_bucket_min(index));
          const auto max_repr = _convert_string_to_number_representation(_bucket_max(index));
          // TODO(tim): work with bucket width
          bucket_share = static_cast<float>(value_repr - min_repr) / (max_repr - min_repr + 1);
        }

        cardinality += bucket_share * _bucket_count(index);
      }

      // Sum up all buckets before the bucket (or gap) containing the value.
      for (BucketID bucket = 0u; bucket < index; bucket++) {
        cardinality += _bucket_count(bucket);
      }

      return cardinality;
    }
    case PredicateCondition::LessThanEquals: {
      return estimate_cardinality(value, PredicateCondition::LessThan) +
             estimate_cardinality(value, PredicateCondition::Equals);
    }
    case PredicateCondition::GreaterThanEquals: {
      return estimate_cardinality(value, PredicateCondition::GreaterThan) +
             estimate_cardinality(value, PredicateCondition::Equals);
    }
    case PredicateCondition::GreaterThan: {
      // if (value < _lower_end()) {
      //   return _total_count();
      // }
      //
      // if (value >= _upper_end()) {
      //   return 0.f;
      // }
      //
      // auto index = _bucket_for_value(value);
      // auto cardinality = 0.f;
      //
      // if (index == INVALID_BUCKET_ID) {
      //   // The value is within the range of the histogram, but does not belong to a bucket.
      //   // Therefore, we need to sum up the counts of all buckets with a min > value.
      //   // _upper_bound_for_value will return the first bucket following the gap in which the value is.
      //   index = _upper_bound_for_value(value);
      // } else {
      //   if constexpr (!std::is_same_v<T, std::string>) {
      //     // The value is within the range of a bucket.
      //     // We need to sum up all following buckets and add the share of the bucket of the value that it covers.
      //
      //     // Calculate the share of the bucket that the value covers.
      //     const auto bucket_share = static_cast<float>(_bucket_max(index) - value) / _bucket_width(index);
      //     cardinality += bucket_share * _bucket_count(index);
      //   } else {
      //     Fail("GreaterThan estimation for strings is not yet supported.");
      //   }
      // }
      //
      // // Sum up all buckets after the bucket (or gap) containing the value.
      // for (BucketID bucket = index; bucket < _num_buckets(); bucket++) {
      //   cardinality += _bucket_count(bucket);
      // }
      //
      // return cardinality;
      return _total_count() - estimate_cardinality(value, PredicateCondition::LessThanEquals);
    }
    default:
      Fail("Predicate condition not yet supported.");
  }
}

template <typename T>
bool AbstractHistogram<T>::can_prune(const AllTypeVariant& value, const PredicateCondition predicate_type) const {
  DebugAssert(_num_buckets() > 0, "Called method on histogram before initialization.");

  const auto t_value = type_cast<T>(value);

  switch (predicate_type) {
    case PredicateCondition::Equals:
      return _bucket_for_value(t_value) == INVALID_BUCKET_ID;
    case PredicateCondition::NotEquals:
      return _num_buckets() == 1 && _bucket_min(0) == t_value && _bucket_max(0) == t_value;
    case PredicateCondition::LessThan:
      return t_value <= _lower_end();
    case PredicateCondition::LessThanEquals:
      return t_value < _lower_end();
    case PredicateCondition::GreaterThanEquals:
      return t_value > _upper_end();
    case PredicateCondition::GreaterThan:
      return t_value >= _upper_end();
    // TODO(tim): change signature to support two values
    // talk to Moritz about new expression interface first
    // case PredicateCondition::Between:
    //   return can_prune(value, PredicateCondition::GreaterThanEquals) ||
    //          can_prune(value2, PredicateCondition::LessThanEquals) ||
    //          (bucket_for_value(t_value) == INVALID_BUCKET_ID && _bucket_for_value(t_value2) == INVALID_BUCKET_ID &&
    //           upper_bound_for_value(t_value) == _upper_bound_for_value(t_value2));
    default:
      // Rather than failing we simply do not prune for things we cannot (yet) handle.
      return false;
  }
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(AbstractHistogram);

}  // namespace opossum
