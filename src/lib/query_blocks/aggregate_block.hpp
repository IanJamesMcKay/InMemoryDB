#pragma once

#include "abstract_query_block.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"

namespace opossum {

class AggregateBlock : public AbstractQueryBlock {
 public:
  AggregateBlock(const std::vector<LQPColumnReference>& group_by_columns, 
                 const std::shared_ptr<AbstractQueryBlock>& input);

  const std::vector<LQPColumnReference> group_by_columns;

 protected:
  size_t _shallow_hash_impl() const override;
  bool _shallow_deep_equals_impl(const AbstractQueryBlock& rhs) const override;
};

}  // namespace opossum