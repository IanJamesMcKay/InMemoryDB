#pragma once

#include <vector>

#include "abstract_rule.hpp"

namespace opossum {

class AbstractQueryBlock;
class AbstractJoinOrderingAlgorithm;
class PredicateBlock;

class JoinOrderingRule : public AbstractRule {
 public:
  explicit JoinOrderingRule(const std::shared_ptr<AbstractJoinOrderingAlgorithm>& join_ordering_algorithm);

  std::string name() const override;

  bool apply_to(const std::shared_ptr<AbstractLQPNode>& root) override;

 private:
  std::shared_ptr<AbstractLQPNode> _apply_to_blocks(const std::shared_ptr<AbstractQueryBlock>& block);
  std::shared_ptr<AbstractLQPNode> _apply_to_predicate_block(const std::shared_ptr<PredicateBlock>& predicate_block, const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices);

  std::shared_ptr<AbstractJoinOrderingAlgorithm> _join_ordering_algorithm;
};

}  // namespace opossum