#include "abstract_cost_model_sampler.hpp"

#include "operators/utils/flatten_pqp.hpp"
#include "operators/table_scan.hpp"
#include "sql/sql.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_query_plan.hpp"

namespace opossum {

void AbstractCostModelSampler::sample(const std::shared_ptr<AbstractOperator>& pqp) {
  const auto operators = flatten_pqp(pqp);

  for (const auto& op : operators) {
    if (const auto table_scan = std::dynamic_pointer_cast<TableScan>(op)) {
      _sample_table_scan(*table_scan);
    }

    /*else if (const auto join_hash = std::dynamic_pointer_cast<JoinHash>(op)) {
      visit_join_hash(*join_hash);
    } else if (const auto join_sort_merge = std::dynamic_pointer_cast<JoinSortMerge>(op)) {
      visit_join_sort_merge(*join_sort_merge);
    } else if (const auto product = std::dynamic_pointer_cast<Product>(op)) {
      visit_product(*product);
    } else {
      op->execute();
    }*/
  }
}

}  // namespace opossum
