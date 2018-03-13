#include "flatten_pqp.hpp"

#include "operators/abstract_operator.hpp"

#include <unordered_set>

namespace {

using namespace opossum;  // NOLINT

std::vector<std::shared_ptr<AbstractOperator>> flatten_pqp_impl(
const std::shared_ptr<AbstractOperator> &pqp,
std::unordered_set<std::shared_ptr<AbstractOperator>> &enlisted_operators) {
  if (enlisted_operators.count(pqp) > 0) return {};
  enlisted_operators.insert(pqp);

  std::vector<std::shared_ptr<AbstractOperator>> operators;

  if (!pqp->input_left()) return {pqp};

  auto left_flattened =
  flatten_pqp_impl(std::const_pointer_cast<AbstractOperator>(pqp->input_left()), enlisted_operators);

  if (pqp->input_right()) {
    auto right_flattened =
    flatten_pqp_impl(std::const_pointer_cast<AbstractOperator>(pqp->input_right()), enlisted_operators);
    left_flattened.insert(left_flattened.end(), right_flattened.begin(), right_flattened.end());
  }

  left_flattened.emplace_back(pqp);

  return left_flattened;
}

}  // namespace

namespace opossum {


std::vector<std::shared_ptr<AbstractOperator>> flatten_pqp(const std::shared_ptr<AbstractOperator> &pqp) {
  std::unordered_set<std::shared_ptr<AbstractOperator>> enlisted_operators;
  return flatten_pqp_impl(pqp, enlisted_operators);
}

}  // namespace opossum
