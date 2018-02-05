#include "table_statistics.hpp"

#include "abstract_column_statistics.hpp"

namespace {

// default selectivity factors for assumption of uniform distribution
constexpr float DEFAULT_SELECTIVITY = 0.5f;
constexpr float DEFAULT_LIKE_SELECTIVITY = 0.1f;
// values below are taken from paper "Access path selection in a relational database management system",
// P. Griffiths Selinger, 1979
constexpr float DEFAULT_OPEN_ENDED_SELECTIVITY = 1.f / 3.f;
constexpr float DEFAULT_BETWEEN_SELECTIVITY = 0.25f;

}

namespace opossum {

TableStatistics::TableStatistics(const float row_count, std::vector<std::shared_ptr<const AbstractColumnStatistics>> column_statistics, const size_t invalid_row_count):
  _row_count(row_count), _invalid_row_count(invalid_row_count), _column_statistics(std::move(column_statistics))
{}

float TableStatistics::row_count() const {
  return _row_count;
}

size_t TableStatistics::invalid_row_count() const {
  return _invalid_row_count;
}

const std::vector<std::shared_ptr<const AbstractColumnStatistics>>& TableStatistics::column_statistics() const {
  return _column_statistics;
}

std::shared_ptr<TableStatistics> TableStatistics::estimate_predicate(
const ColumnID column_id, const PredicateCondition predicate_condition, const AllParameterVariant& value,
const std::optional<AllTypeVariant>& value2 = std::nullopt) const {
  // Early out, below code would fail for _row_count == 0
  if (_row_count == 0) return shared_from_this();

  if (predicate_condition == PredicateCondition::Like || predicate_condition == PredicateCondition::NotLike) {
    // Simple heuristic:
    const auto selectivity = predicate_condition == PredicateCondition::Like ? DEFAULT_LIKE_SELECTIVITY : 1.0f - DEFAULT_LIKE_SELECTIVITY;
    return std::make_shared<TableStatistics>(_row_count * selectivity, _column_statistics);
  }

  auto predicate_row_count = _row_count;
  auto predicate_column_statistics = _column_statistics; // Statistics of the main column of the predicate

  const auto left_operand_column_statistics = _column_statistics[column_id];

  if (value.type() == typeid(ColumnID)) {
    const auto value_column_id = boost::get<ColumnID>(value);

    const auto estimation = left_operand_column_statistics->estimate_predicate(predicate_condition, _column_statistics[value_column_id], value2);

    predicate_column_statistics[column_id] = estimation.left_column_statistics;
    predicate_column_statistics[value_column_id] = estimation.right_column_statistics;

    predicate_row_count *= estimation.selectivity;
  } else if (value.type() == typeid(AllTypeVariant)) {
    const auto variant_value = get<AllTypeVariant>(value);

    const auto estimation = left_operand_column_statistics->estimate_predicate(predicate_condition, variant_value, value2);

    predicate_column_statistics[column_id] = estimation.column_statistics;
    predicate_row_count *= estimation.selectivity;
  } else {
    DebugAssert(value.type() == typeid(ValuePlaceholder),
                "AllParameterVariant type is not implemented in statistics component.");
    const auto value_placeholder = boost::get<ValuePlaceholder>(value);

    const auto estimation = left_operand_column_statistics->estimate_predicate(predicate_condition, value_placeholder, value2);

    predicate_column_statistics[column_id] = estimation.column_statistics;
    predicate_row_count *= estimation.selectivity;
  }

  return std::make_shared<TableStatistics>(predicate_row_count, predicate_column_statistics);
}

std::shared_ptr<TableStatistics> TableStatistics::estimate_cross_join(
const std::shared_ptr<const TableStatistics>& right_table_statistics) const {
  auto column_statistics = _column_statistics;
  for (const auto& right_table_column_statistics : right_table_statistics->column_statistics()) {
    column_statistics.emplace_back(right_table_column_statistics);
  }

  const auto row_count = _row_count * right_table_statistics->row_count();

  return std::make_shared<TableStatistics>(row_count, column_statistics);
}

std::shared_ptr<TableStatistics> TableStatistics::estimate_predicated_join(
const std::shared_ptr<const TableStatistics>& right_table_statistics, const JoinMode mode, const ColumnIDPair column_ids,
const PredicateCondition predicate_condition) const {
  /**
    * The approach to calculate the join table statistics is to split the join into a cross join followed by a predicate.
    *
    * This approach allows to reuse the code to copy the column statistics to the join output statistics from the cross
    * join function. The selectivity of the join predicate can then be calculated by the two column predicate function
    * within column statistics. The calculated selectivity can then be applied to the cross join result.
    *
    * For left/right/outer joins the occurring null values will result in changed null value ratios in partial/all column
    * statistics of the join statistics.
    * To calculate the changed ratios, the new total number of null values in a column as well as the join table row count
    * are necessary. Remember that statistics component assumes NULL != NULL semantics.
    *
    * The calculation of null values is shown by following SQL query: SELECT * FROM TABLE_1 OUTER JOIN TABLE_2 ON a = c
    *
    *   TABLE_1         TABLE_2          CROSS_JOIN_TABLE              INNER / LEFT  / RIGHT / OUTER JOIN
    *
    *    a    | b        c    | d         a    | b    | c    | d        a    | b    | c    | d
    *   -------------   --------------   --------------------------    --------------------------
    *    1    | NULL     1    | 30        1    | NULL | 1    | 30       1    | NULL | 1    | 30
    *    2    | 10       NULL | 40        2    | 10   | 1    | 30
    *    NULL | 20                        NULL | 20   | 1    | 30      INNER +0 extra rows
    *                                     1    | NULL | NULL | 40      LEFT  +2 extra rows
    *                                     2    | 10   | NULL | 40      RIGHT +1 extra rows
    *                                     NULL | 20   | NULL | 40      OUTER +3 extra rows (the ones from LEFT & RIGHT)
    *
    * First, the cross join row count is calculated: 3 * 2 = 6
    * Then, the selectivity for non-null values is calculated: 1/2 (50% of the non-null values from column a match the
    * value 1 from column c)
    * Next, the predicate selectivity is calculated: non-null predicate selectivity * left-non-null * right-non-null
    * = 1/2 * 2/3 * 1/2 = 1/6
    * For an inner join, the row count would then be: row count * predicate selectivity = 6 * 1/6 = 1
    *
    * The selectivity calculation call also returns the new column statistics for columns a and c. Both are identical and
    * have a min, max value of 1, distinct count of 1 and a non-null value ratio of 1.
    * These new column statistics replace the old corresponding column statistics in the output table statistics, if the
    * join mode does not specify to keep all values of a column.
    * E.g. the new left column statistics replaces its previous statistics, if join mode is self, inner or right.
    * Vice versa the new right column statistics replaces its previous statistic, if join mode is self, inner or left.
    *
    * For a full outer join, the null values added to columns c and d are the number of null values of column a (= 1)
    * plus the number of non-null values of column a not selected by the predicate (= 1 (value 2 in row 2)).
    * So in total 1 + 1 = 2 null values are added to columns c and d. Column c had already a null value before and,
    * therefore, has now 1 + 2 = 3 null values. Column d did not have null values and now has 0 + 2 = 2 null values.
    * The same calculations also needs to be done for the null value numbers in columns a and b. Since all non-null
    * values in column c are selected by the predicate only the null value number of column c needs to be added to
    * columns a and b: 0 + 1 = 1
    * Columns a and b both have 1 null value before the join and, therefore, both have 1 + 1 = 2 null values after the
    * join.
    *
    * The row count of the join result is calculated by taking the row count of the inner join (= 1) and adding the null
    * value numbers which were added to the columns from the left table (= 1) and the right table (= 2). This results in
    * the row count for the outer join of 1 + 1 + 2 = 4.
    *
    * For a left outer join, the join table would just miss the 4th row. For this join, the number of null values to add
    * to the right columns would still be 1 + 1 = 2. However, no null values are added to the the left columns.
    * This results in a join result row count of 1 + 2 = 3.
    */

  DebugAssert(mode != JoinMode::Self || this == right_table_statistics.get(), "Self Joins should have the same left and right argument.");

  const auto cross_join_statistics = estimate_cross_join(right_table_statistics);
  const auto predicate_statistics = cross_join_statistics->estimate_predicate(column_ids.first, predicate_condition, column_ids.second);

  if (mode == JoinMode::Self || mode == JoinMode::Inner) {
    return predicate_statistics;
  }

  auto row_count = predicate_statistics->row_count();

  auto left_null_value_no = _calculate_added_null_values_for_outer_join(
  right_table_statistics->row_count(), right_column_statistics, stats_container.second_column_statistics->distinct_count());
  // calculate how many null values need to be added to columns from the right table for left/outer joins
  auto right_null_value_no = _calculate_added_null_values_for_outer_join(
  row_count(), left_column_statistics, stats_container.column_statistics->distinct_count());

  if (mode == JoinMode::Left || mode == JoinMode::Outer) {
    row_count += right_null_value_count;
  }
  if (mode == JoinMode::Right || mode == JoinMode::Outer) row_count += left_null_value_count;

  switch (mode) {
    case JoinMode::Left: {
      cross_join_statistics->_column_statistics[new_right_column_id] = stats_container.second_column_statistics;
      row_count += right_null_value_no;
      apply_left_outer_join();
      break;
    }
    case JoinMode::Right: {
      cross_join_statistics->_column_statistics[column_ids.first] = stats_container.column_statistics;
      cross_join_statistics->_row_count += left_null_value_no;
      apply_right_outer_join();
      break;
    }
    case JoinMode::Outer: {
      cross_join_statistics->_row_count += right_null_value_no;
      cross_join_statistics->_row_count += left_null_value_no;
      apply_left_outer_join();
      apply_right_outer_join();
      break;
    }
  }

//
//  // retrieve the two column statistics which are used by the join predicate
//  const auto& left_column_statistics = _column_statistics[column_ids.first];
//  const auto& right_column_statistics = right_table_statistics->_column_statistics[column_ids.second];
//
//  const auto predicate_estimation = left_column_statistics->estimate_predicate(predicate_condition, right_column_statistics);
//
//  // apply predicate selectivity to cross join
//  auto joined_row_count = cross_join_statistics->row_count() * predicate_estimation.selectivity;
//
//  auto joined_column_statistics = predicate_estimation->column_statistics();
//
//  ColumnID new_right_column_id{static_cast<ColumnID::base_type>(_column_statistics.size() + column_ids.second)};

  // calculate how many null values need to be added to columns from the left table for right/outer joins
  auto left_null_value_no = _calculate_added_null_values_for_outer_join(
  right_table_statistics->row_count(), right_column_statistics, stats_container.second_column_statistics->distinct_count());
  // calculate how many null values need to be added to columns from the right table for left/outer joins
  auto right_null_value_no = _calculate_added_null_values_for_outer_join(
  row_count(), left_column_statistics, stats_container.column_statistics->distinct_count());

  // prepare two _adjust_null_value_ratio_for_outer_join calls, executed in the switch statement below

  // a) add null values to columns from the right table for left outer join
  auto apply_left_outer_join = [&]() {
    _adjust_null_value_ratio_for_outer_join(cross_join_statistics->_column_statistics.begin() + _column_statistics.size(),
                                            cross_join_statistics->_column_statistics.end(), right_table_statistics->row_count(),
                                            right_null_value_no, cross_join_statistics->row_count());
  };
  // b) add null values to columns from the left table for right outer
  auto apply_right_outer_join = [&]() {
    _adjust_null_value_ratio_for_outer_join(cross_join_statistics->_column_statistics.begin(),
                                            cross_join_statistics->_column_statistics.begin() + _column_statistics.size(),
                                            row_count(), left_null_value_no, cross_join_statistics->row_count());
  };

  switch (mode) {
    case JoinMode::Self:
    case JoinMode::Inner: {
      cross_join_statistics->_column_statistics[column_ids.first] = stats_container.column_statistics;
      cross_join_statistics->_column_statistics[new_right_column_id] = stats_container.second_column_statistics;
      break;
    }
    case JoinMode::Left: {
      cross_join_statistics->_column_statistics[new_right_column_id] = stats_container.second_column_statistics;
      cross_join_statistics->_row_count += right_null_value_no;
      apply_left_outer_join();
      break;
    }
    case JoinMode::Right: {
      cross_join_statistics->_column_statistics[column_ids.first] = stats_container.column_statistics;
      cross_join_statistics->_row_count += left_null_value_no;
      apply_right_outer_join();
      break;
    }
    case JoinMode::Outer: {
      cross_join_statistics->_row_count += right_null_value_no;
      cross_join_statistics->_row_count += left_null_value_no;
      apply_left_outer_join();
      apply_right_outer_join();
      break;
    }
    default: { Fail("Join mode not implemented."); }
  }

  return std::make_shared
}


}  // namespace opossum