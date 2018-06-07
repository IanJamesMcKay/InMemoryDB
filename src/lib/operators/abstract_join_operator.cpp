#include "abstract_join_operator.hpp"

#include <memory>
#include <string>
#include <utility>

#include "constant_mappings.hpp"

using namespace std::string_literals;

namespace opossum {

AbstractJoinOperator::AbstractJoinOperator(const OperatorType type, const std::shared_ptr<const AbstractOperator> left,
                                           const std::shared_ptr<const AbstractOperator> right, const JoinMode mode,
                                           const ColumnIDPair& column_ids, const PredicateCondition predicate_condition)
    : AbstractReadOnlyOperator(type, left, right),
      _mode(mode),
      _column_ids(column_ids),
      _predicate_condition(predicate_condition) {
  DebugAssert(mode != JoinMode::Cross && mode != JoinMode::Natural,
              "Specified JoinMode not supported by an AbstractJoin, use Product etc. instead.");
}

JoinMode AbstractJoinOperator::mode() const { return _mode; }

const ColumnIDPair& AbstractJoinOperator::column_ids() const { return _column_ids; }

PredicateCondition AbstractJoinOperator::predicate_condition() const { return _predicate_condition; }

const std::string AbstractJoinOperator::description(DescriptionMode description_mode) const {
  Assert(input_left() && input_right(), "Cannot generate description without inputs")

  const auto separator = description_mode == DescriptionMode::MultiLine ? "\\n" : " ";

  return name() + separator + join_mode_to_string.at(_mode) + " Join" + separator + input_left()->qualified_column_name(_column_ids.first) + " " +
         predicate_condition_to_string.left.at(_predicate_condition) + " " + input_right()->qualified_column_name(_column_ids.second);
}

std::string AbstractJoinOperator::qualified_column_name(const ColumnID column_id) const {
  if (!input_left() || !input_right()) return "#"s + std::to_string(column_id);
  if (!input_left()->get_output() || !input_right()->get_output()) return "#"s + std::to_string(column_id);

  if (column_id < input_left()->get_output()->column_count()) {
    return input_left()->qualified_column_name(column_id);
  } else {
    const auto right_column_id = static_cast<ColumnID>(column_id - input_left()->get_output()->column_count());
    Assert(right_column_id < input_right()->get_output()->column_count(), "");
    return input_right()->qualified_column_name(right_column_id);
  }
}

}  // namespace opossum
