#include "abstract_dp_algorithm.hpp"

#include "abstract_dp_subplan_cache.hpp"
#include "query_blocks/predicate_join_block.hpp"
#include "utils/assert.hpp"

namespace opossum {

AbstractDpAlgorithm::AbstractDpAlgorithm(const std::shared_ptr<AbstractDpSubplanCache>& subplan_cache,
                                         const std::shared_ptr<AbstractCostModel>& cost_model,
                                         const std::shared_ptr<AbstractCardinalityEstimator>& cardinality_estimator)
    : _subplan_cache(subplan_cache), _cost_model(cost_model), _cardinality_estimator(cardinality_estimator) {}

std::shared_ptr<PlanBlock> AbstractDpAlgorithm::operator()(const std::shared_ptr<PredicateJoinBlock>& input_block) {
  _subplan_cache->clear();
  _input_block = input_block;

  /**
   * Initialize single-vertex vertex_sets with single node JoinPlans
   */
  for (size_t vertex_idx = 0; vertex_idx < _input_block->sub_blocks.size(); ++vertex_idx) {
    boost::dynamic_bitset<> vertex_bit{_input_block->sub_blocks.size()};
    vertex_bit.set(vertex_idx);

    const auto vertex_predicates = _input_block->find_predicates(vertex_bit);

    const auto sub_block = _input_block->sub_blocks[vertex_idx];
    Assert(sub_block->type == QueryBlockType::Plan, "Dynamic programming needs plans as vertices");

    _subplan_cache->cache_plan(
        vertex_bit, build_join_plan_vertex_node(*_cost_model, vertex, vertex_predicates, *_cardinality_estimator));
  }

  _on_execute();

  /**
   * Build vertex set with all vertices set and return the plan for it
   */
  boost::dynamic_bitset<> all_vertices_set{_input_block->sub_blocks.size()};
  all_vertices_set.flip();  // Turns all bits to '1'

  const auto best_plan = _subplan_cache->get_best_plan(all_vertices_set);
  Assert(best_plan, "No plan for all vertices generated. Maybe JoinGraph isn't connected?");

  return *best_plan;
}

}  // namespace opossum
