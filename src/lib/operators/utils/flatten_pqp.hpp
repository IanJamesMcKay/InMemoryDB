#pragma once

#include <memory>
#include <vector>

namespace opossum {

class AbstractOperator;

std::vector<std::shared_ptr<AbstractOperator>> flatten_pqp(const std::shared_ptr<AbstractOperator>& pqp);

}  // namespace opossum
