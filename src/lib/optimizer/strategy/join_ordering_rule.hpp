#pragma once

#include "abstract_rule.hpp"

namespace opossum {

class AbstractJoinOrderingAlgorithm;

class JoinOrderingRule : public AbstractRule {
 public:
  explicit JoinOrderingRule(const std::shared_ptr<AbstractJoinOrderingAlgorithm>& join_ordering_algorithm);

  std::string name() const override;

  bool apply_to(const std::shared_ptr<AbstractLQPNode>& root) override;

 private:
  bool _applicable(const std::shared_ptr<AbstractLQPNode>& node) const;

  std::shared_ptr<AbstractJoinOrderingAlgorithm> _join_ordering_algorithm;
};

}  // namespace opossum