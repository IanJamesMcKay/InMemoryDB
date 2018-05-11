#pragma once

#include <memory>

namespace opossum {

class AbstractOperator;

class AbstractOperatorCallback {
 public:
  virtual ~AbstractOperatorCallback() = default;

  virtual void operator()(const std::shared_ptr<AbstractOperator>& op) = 0;
};

}  // namespace opossum