#pragma once

#include "abstract_rule.hpp"

namespace opossum {

class JoinOrderingRule : public AbstractRule {
 public:
  std::string name() const override;

  bool apply_to(const std::shared_ptr<AbstractLQPNode>& root) override;

 private:
  bool _applicable(const std::shared_ptr<AbstractLQPNode>& node) const;
};

}  // namespace opossum