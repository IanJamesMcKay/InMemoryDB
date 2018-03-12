#pragma once

#include <functional>
#include <memory>

namespace opossum {

class AbstractOperator;

using PostOperatorCallback = std::function<void(const std::shared_ptr<AbstractOperator>& op)>;

}  // namespace opossum
