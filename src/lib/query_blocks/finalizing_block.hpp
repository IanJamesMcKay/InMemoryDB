#pragma once

#include <optional>

#include "abstract_query_block.hpp"
#include "logical_query_plan/lqp_expression.hpp"
#include "logical_query_plan/sort_node.hpp"

namespace opossum {

/**
 * Lacking a better name, this block represents the Projections and Sorting of a Query
 */
class FinalizingBlock : public AbstractQueryBlock {
 public:
  FinalizingBlock(const std::vector<std::shared_ptr<LQPExpression>>& column_expressions,
                  const OrderByDefinitions& order_by_definitions,
                  const std::optional<size_t>& limit,
                  const std::shared_ptr<AbstractQueryBlock>& input);

  const std::vector<std::shared_ptr<LQPExpression>> column_expressions;
  const OrderByDefinitions order_by_definitions;
  const std::optional<size_t> limit;

 protected:
  virtual size_t _shallow_hash_impl() const = 0;
  virtual bool _shallow_deep_equals_impl(const AbstractQueryBlock& rhs) const = 0;
};

}  // namespace opossum
