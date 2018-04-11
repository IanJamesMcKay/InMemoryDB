#include "abstract_cost_model_sampler.hpp"

#include "operators/utils/flatten_pqp.hpp"
#include "operators/product.hpp"
#include "operators/table_scan.hpp"
#include "operators/join_hash.hpp"
#include "operators/union_positions.hpp"
#include "sql/sql.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_query_plan.hpp"

namespace opossum {

void AbstractCostModelSampler::sample(const std::shared_ptr<AbstractOperator>& pqp) {
  const auto operators = flatten_pqp(pqp);

  for (const auto& op : operators) {
    switch (op->type()) {
      case OperatorType::TableScan: _sample_table_scan(*std::static_pointer_cast<TableScan>(op)); break;
      case OperatorType::JoinHash: _sample_join_hash(*std::static_pointer_cast<JoinHash>(op)); break;
      case OperatorType::Product: _sample_product(*std::static_pointer_cast<Product>(op)); break;
      case OperatorType::UnionPositions: _sample_union_positions(*std::static_pointer_cast<UnionPositions>(op)); break;

      default:
        ;
    }
  }
}

}  // namespace opossum
