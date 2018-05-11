#pragma once

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "abstract_rule.hpp"

#include "types.hpp"

namespace opossum {

class AbstractLQPNode;

class CardinalityEstimationInstrumentationRule : public AbstractRule {
 public:
  std::string name() const override;
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& node) override;
};

}  // namespace opossum
