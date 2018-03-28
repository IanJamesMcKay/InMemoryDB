#include "cost_model_naive.hpp"

#include <cmath>

#include "optimizer/table_statistics.hpp"

namespace opossum {

Cost CostModelNaive::cost_join_hash(const std::shared_ptr<TableStatistics>& table_statistics_left,
                                    const std::shared_ptr<TableStatistics>& table_statistics_right,
                                    const JoinMode join_mode, const ColumnIDPair& join_column_ids,
                                    const PredicateCondition predicate_condition) const {
  return 1.2f * table_statistics_left->row_count();
}

Cost CostModelNaive::cost_table_scan(const std::shared_ptr<TableStatistics>& table_statistics, const ColumnID column,
                                     const PredicateCondition predicate_condition, const AllParameterVariant& value) const {
  return table_statistics->row_count();
}

Cost CostModelNaive::cost_join_sort_merge(const std::shared_ptr<TableStatistics>& table_statistics_left,
                                          const std::shared_ptr<TableStatistics>& table_statistics_right,
                                          const JoinMode join_mode, const ColumnIDPair& join_column_ids,
                                          const PredicateCondition predicate_condition) const {
  const auto row_count_left = table_statistics_left->row_count();
  const auto row_count_right = table_statistics_right->row_count();

  return row_count_left * std::log(row_count_left) + row_count_right * std::log(row_count_right);
}

Cost CostModelNaive::cost_product(const std::shared_ptr<TableStatistics>& table_statistics_left,
                                  const std::shared_ptr<TableStatistics>& table_statistics_right) const {
  return table_statistics_left->row_count() * table_statistics_right->row_count();
}

Cost CostModelNaive::cost_union_positions(const std::shared_ptr<TableStatistics>& table_statistics_left,
                          const std::shared_ptr<TableStatistics>& table_statistics_right) const {
  return 0.0f; // TODO(moritz)
}
}  // namespace opossum
