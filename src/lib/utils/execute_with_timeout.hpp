#pragma once

#include <chrono>
#include <optional>
#include <memory>

namespace opossum {

class AbstractOperator;

bool execute_with_timeout(const std::shared_ptr<AbstractOperator>& pqp, const std::optional<std::chrono::seconds>& timeout);

}  // namespace opossum