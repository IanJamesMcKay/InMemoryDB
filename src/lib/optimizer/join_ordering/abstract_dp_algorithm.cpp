#include "abstract_dp_algorithm.hpp"

#include "abstract_dp_subplan_cache.hpp"
#include "cost_model/abstract_cost_model.hpp"
#include "cost_model/cost_feature_lqp_node_proxy.hpp"
#include "query_blocks/predicate_join_block.hpp"
#include "query_blocks/plan_block.hpp"
#include "utils/assert.hpp"
#include "build_lqp_for_predicate.hpp"

namespace opossum {

AbstractDpAlgorithm::AbstractDpAlgorithm(const std::shared_ptr<AbstractDpSubplanCache>& subplan_cache,
                                         const std::shared_ptr<AbstractCostModel>& cost_model,
                                         const std::shared_ptr<AbstractCardinalityEstimator>& cardinality_estimator)
    : _subplan_cache(subplan_cache), _cost_model(cost_model), _cardinality_estimator(cardinality_estimator) {}

std::shared_ptr<AbstractLQPNode> AbstractDpAlgorithm::operator()(const std::shared_ptr<PredicateJoinBlock>& input_block) {
  _subplan_cache->clear();
  _input_block = input_block;
  _join_graph = JoinGraph::from_query_block(_input_block);

  /**
   * Initialize single-vertex vertex_sets with single node JoinPlans
   */
  for (size_t vertex_idx = 0; vertex_idx < _input_block->sub_blocks.size(); ++vertex_idx) {
    boost::dynamic_bitset<> vertex_bit{_input_block->sub_blocks.size()};
    vertex_bit.set(vertex_idx);

    const auto vertex_predicates = _join_graph->find_predicates(vertex_bit);

    _subplan_cache->cache_plan(
        vertex_bit, _create_join_plan_leaf(_input_block->sub_blocks[vertex_idx], vertex_predicates));
  }

  _on_execute();

  /**
   * Build vertex set with all vertices set and return the plan for it - which is the "best" JoinPlan
   */
  boost::dynamic_bitset<> all_vertices_set{_input_block->sub_blocks.size()};
  all_vertices_set.flip();  // Turns all bits to '1'

  const auto best_plan = _subplan_cache->get_best_plan(all_vertices_set);
  Assert(best_plan, "No plan for all vertices generated. Maybe JoinGraph isn't connected?");

  return best_plan->lqp;
}


JoinPlan AbstractDpAlgorithm::_create_join_plan_leaf(
const std::shared_ptr<AbstractQueryBlock> &vertex_block,
const std::vector<std::shared_ptr<AbstractJoinPlanPredicate>> &predicates) const {

  const auto plan_block = std::dynamic_pointer_cast<PlanBlock>(vertex_block);
  Assert(plan_block, "Dynamic programming needs plans as vertices");

  const auto query_block = std::make_shared<PredicateJoinBlock>(
    std::vector<std::shared_ptr<AbstractQueryBlock>>{vertex_block},
    std::vector<std::shared_ptr<AbstractJoinPlanPredicate>>{});

  auto join_plan = JoinPlan{plan_block->lqp, query_block, 0.0f};

  for (const auto& predicate : predicates) {
    _add_join_plan_predicate(join_plan, predicate);
  }

  return join_plan;
}

void AbstractDpAlgorithm::_add_join_plan_predicate(JoinPlan& join_plan, const std::shared_ptr<AbstractJoinPlanPredicate>& predicate) const {
  const auto predicate_nodes = std::make_shared<std::vector<std::shared_ptr<AbstractLQPNode>>>();

  join_plan.lqp = build_lqp_for_predicate(*predicate, join_plan.lqp, predicate_nodes);

  // Add the cost from all new nodes to the plan cost
  for (const auto& node : *predicate_nodes) {
    join_plan.plan_cost += _cost_model->estimate_cost(CostFeatureLQPNodeProxy{node, _cardinality_estimator});
  }

  join_plan.query_block->predicates.emplace_back(predicate);
}


JoinPlan AbstractDpAlgorithm::_create_join_plan(const JoinPlan& left_input, const JoinPlan& right_input,
                                                const std::vector<std::shared_ptr<AbstractJoinPlanPredicate>>& predicates) const {
  /**
   * Join the two input query blocks
   */
  JoinPlan join_plan{nullptr,
                          PredicateJoinBlock::merge_blocks(*left_input.query_block, *right_input.query_block),
                          left_input.plan_cost + right_input.plan_cost};

  auto primary_join_predicate = std::shared_ptr<JoinPlanAtomicPredicate>{};
  auto secondary_predicates = predicates;

  /**
   * Look for a primary join predicate - needs to be atomic and have one argument in the right and one in the left sub
   * plan
   */
  const auto iter = std::find_if(secondary_predicates.begin(), secondary_predicates.end(), [&](const auto& predicate) {
    if (predicate->type() != JoinPlanPredicateType::Atomic) return false;
    const auto atomic_predicate = std::static_pointer_cast<JoinPlanAtomicPredicate>(predicate);
    if (!is_lqp_column_reference(atomic_predicate->right_operand)) return false;

    const auto right_operand_column_reference = boost::get<LQPColumnReference>(atomic_predicate->right_operand);

    const auto left_operand_in_left_child = left_input.lqp->find_output_column_id(atomic_predicate->left_operand);
    const auto left_operand_in_right_child = right_input.lqp->find_output_column_id(atomic_predicate->left_operand);
    const auto right_operand_in_left_child = left_input.lqp->find_output_column_id(right_operand_column_reference);
    const auto right_operand_in_right_child = right_input.lqp->find_output_column_id(right_operand_column_reference);

    return (left_operand_in_left_child && right_operand_in_right_child) ||
           (left_operand_in_right_child && right_operand_in_left_child);
  });

  /**
   * If a primary join predicate was found, make sure its left operand references the left sub-plan and its right
   * operand the right. If necessary, swap the arguments.
   */
  if (iter != secondary_predicates.end()) {
    primary_join_predicate = std::static_pointer_cast<JoinPlanAtomicPredicate>(*iter);

    auto left_operand = primary_join_predicate->left_operand;
    auto predicate_condition = primary_join_predicate->predicate_condition;
    auto right_operand = boost::get<LQPColumnReference>(primary_join_predicate->right_operand);

    if (!left_input.lqp->find_output_column_id(left_operand)) {
      std::swap(left_operand, right_operand);
      predicate_condition = flip_predicate_condition(predicate_condition);
    }
    DebugAssert(left_input.lqp->find_output_column_id(left_operand) && right_input.lqp->find_output_column_id(right_operand),
                "Predicate not applicable to sub plans");

    primary_join_predicate =
    std::make_shared<JoinPlanAtomicPredicate>(left_operand, predicate_condition, right_operand);

    secondary_predicates.erase(iter);
  }

  auto lqp = std::shared_ptr<AbstractLQPNode>();

  // Add primary join predicate as inner JoinNode, or cross product JoinNode if no such predicate exists
  if (primary_join_predicate) {
    const auto right_operand_column_reference = boost::get<LQPColumnReference>(primary_join_predicate->right_operand);

    join_plan.lqp = JoinNode::make(JoinMode::Inner,
                                   std::make_pair(primary_join_predicate->left_operand,
                                                  right_operand_column_reference),
                                   primary_join_predicate->predicate_condition, left_input.lqp, right_input.lqp);

    join_plan.query_block->predicates.emplace_back(primary_join_predicate);
  } else {
    join_plan.lqp = JoinNode::make(JoinMode::Cross, left_input.lqp, right_input.lqp);
  }

  join_plan.plan_cost += _cost_model->estimate_cost(CostFeatureLQPNodeProxy{join_plan.lqp, _cardinality_estimator});

  // Apply remaining predicates
  for (const auto& predicate : secondary_predicates) {
    _add_join_plan_predicate(join_plan, predicate);
  }

  return join_plan;
}

}  // namespace opossum
