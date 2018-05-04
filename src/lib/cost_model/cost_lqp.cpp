#include "cost_lqp.hpp"

#include <unordered_set>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "abstract_cost_model.hpp"
#include "cost_feature_lqp_node_proxy.hpp"

namespace {

using namespace opossum;  // NOLINT

Cost cost_lqp_impl(const std::shared_ptr<AbstractLQPNode>& lqp, const AbstractCostModel& cost_model, std::unordered_set<std::shared_ptr<AbstractLQPNode>>& visited) {
  if (visited.count(lqp)) return 0.0f;

  visited.insert(lqp);

  auto cost = cost_model.estimate_cost(CostFeatureLQPNodeProxy(lqp));

  if (lqp->left_input()) cost += cost_lqp_impl(lqp->left_input(), cost_model, visited);
  if (lqp->right_input()) cost += cost_lqp_impl(lqp->right_input(), cost_model, visited);

  return cost;
}

}  // namespace

namespace opossum {

Cost cost_lqp(const std::shared_ptr<AbstractLQPNode>& lqp, const AbstractCostModel& cost_model) {
  std::unordered_set<std::shared_ptr<AbstractLQPNode>> visited;
  return cost_lqp_impl(lqp, cost_model, visited);
}

}  // namespace opossum