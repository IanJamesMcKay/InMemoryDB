#pragma once

#include <memory>
#include <vector>

#include "types.hpp"
#include "all_type_variant.hpp"
#include "all_parameter_variant.hpp"

namespace opossum {

class AbstractColumnStatistics;

class TableStatistics : public std::enable_shared_from_this<TableStatistics> {
 public:
  TableStatistics(const float row_count, std::vector<std::shared_ptr<const AbstractColumnStatistics>> column_statistics, const size_t invalid_row_count = 0);

  /**
   * @defgroup Member access
   * @{
   */
  float row_count() const;
  size_t invalid_row_count() const;
  const std::vector<std::shared_ptr<const AbstractColumnStatistics>>& column_statistics() const;
  /**
   * @}
   */

  /**
   * Generate table statistics for the operator table scan table scan.
   */
  std::shared_ptr<TableStatistics> estimate_predicate(
  const ColumnID column_id, const PredicateCondition predicate_condition, const AllParameterVariant& value,
  const std::optional<AllTypeVariant>& value2 = std::nullopt) const;

  /**
   * Generate table statistics for a cross join.
   */
  std::shared_ptr<TableStatistics> estimate_cross_join(
  const std::shared_ptr<const TableStatistics>& right_table_statistics) const;

  /**
   * Generate table statistics for predicated joins.
   */
  std::shared_ptr<TableStatistics> estimate_predicated_join(
  const std::shared_ptr<const TableStatistics>& right_table_statistics, const JoinMode mode, const ColumnIDPair column_ids,
  const PredicateCondition predicate_condition) const;

 private:
  const float _row_count;
  size_t _invalid_row_count;
  const std::vector<std::shared_ptr<const AbstractColumnStatistics>> _column_statistics;
};

}  // namespace opossum
