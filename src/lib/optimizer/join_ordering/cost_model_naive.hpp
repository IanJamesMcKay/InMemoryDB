#pragma once

#include "abstract_cost_model.hpp"

namespace opossum {

class CostModelNaive : public AbstractCostModel {
 public:
  Cost cost_join_hash(const std::shared_ptr<TableStatistics>& table_statistics_left,
                      const std::shared_ptr<TableStatistics>& table_statistics_right, const JoinMode join_mode,
                      const ColumnIDPair& join_column_ids, const PredicateCondition predicate_condition) const override;

  Cost cost_table_scan(const std::shared_ptr<TableStatistics>& table_statistics, const ColumnID column,
                       const PredicateCondition predicate_condition, const AllParameterVariant& value) const override;

  Cost cost_join_sort_merge(const std::shared_ptr<TableStatistics>& table_statistics_left,
                            const std::shared_ptr<TableStatistics>& table_statistics_right, const JoinMode join_mode,
                            const ColumnIDPair& join_column_ids,
                            const PredicateCondition predicate_condition) const override;

  Cost cost_product(const std::shared_ptr<TableStatistics>& table_statistics_left,
                    const std::shared_ptr<TableStatistics>& table_statistics_right) const override;

  Cost cost_union_positions(const std::shared_ptr<TableStatistics>& table_statistics_left,
                                    const std::shared_ptr<TableStatistics>& table_statistics_right) const override;
};

}  // namespace opossum
