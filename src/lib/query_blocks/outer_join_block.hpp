#pragma once

#include "abstract_query_block.hpp"
#include "logical_query_plan/join_node.hpp"
#include "optimizer/join_ordering/join_plan_predicate.hpp"

namespace opossum {

class OuterJoinBlock : public AbstractQueryBlock {
 public:
  OuterJoinBlock(const JoinMode join_mode,
                 const std::shared_ptr<AbstractJoinPlanPredicate>& predicate,
                 const std::shared_ptr<AbstractQueryBlock>& left_input,
                 const std::shared_ptr<AbstractQueryBlock>& right_input);

  const JoinMode join_mode;
  const std::shared_ptr<AbstractJoinPlanPredicate> predicate;

 protected:
  size_t _shallow_hash_impl() const override;
  bool _shallow_deep_equals_impl(const AbstractQueryBlock& rhs) const override;
};

}