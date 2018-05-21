#include "joe_query_iteration.hpp"

#include "sql/sql.hpp"
#include "utils/timer.hpp"
#include "utils/format_duration.hpp"

#include "out.hpp"
#include "joe_query.hpp"

namespace opossum {

std::ostream &operator<<(std::ostream &stream, const JoeQueryIterationSample &sample) {
  if (sample.best_plan) {
    stream << sample.best_plan->sample.execution_duration.count() << ",";
  } else {
    stream << 0 << ",";
  }

  stream <<
         sample.planning_duration.count() << "," <<
         sample.ce_cache_hit_count << "," <<
         sample.ce_cache_miss_count << "," <<
         sample.ce_cache_size << "," <<
         sample.ce_cache_distinct_hit_count << "," <<
         sample.ce_cache_distinct_miss_count;
  return stream;
}

JoeQueryIteration::JoeQueryIteration(JoeQuery& query, const size_t idx):
  query(query), idx(idx) {
  name = query.sample.name + "-" + std::to_string(idx);
}

void JoeQueryIteration::run() {
  const auto config = query.config;

  out() << "--- Query Iteration " << idx << std::endl;

  /**
   * Initialise Iteration
   */
  if (config->plan_timeout_seconds) current_plan_timeout = std::chrono::seconds{*config->plan_timeout_seconds};

  Timer timer;

  /**
   * Plan Generation
   */
  auto sql_pipeline_statement = SQL{query.sql}.disable_mvcc().pipeline_statement();
  const auto unoptimized_lqp = sql_pipeline_statement.get_unoptimized_logical_plan();
  const auto lqp_root = std::shared_ptr<AbstractLQPNode>(LogicalPlanRootNode::make(unoptimized_lqp));
  const auto join_graph = JoinGraph::from_lqp(unoptimized_lqp);
  const auto plan_generation_count = query.config->max_plan_generation_count ? *query.config->max_plan_generation_count : DpSubplanCacheTopK::NO_ENTRY_LIMIT;
  DpCcpTopK dp_ccp_top_k{plan_generation_count, query.config->cost_model, query.config->main_cardinality_estimator};
  dp_ccp_top_k(join_graph);
  JoinVertexSet all_vertices{join_graph->vertices.size()};
  all_vertices.flip();
  const auto join_plans = dp_ccp_top_k.subplan_cache()->get_best_plans(all_vertices);

  /**
   * Take measurements about Plan Generation
   */
  sample.planning_duration = std::chrono::duration_cast<std::chrono::microseconds>(timer.lap());
  sample.ce_cache_hit_count = query.config->cardinality_estimation_cache->cache_hit_count();
  sample.ce_cache_miss_count = query.config->cardinality_estimation_cache->cache_miss_count();
  sample.ce_cache_size = query.config->cardinality_estimation_cache->size();
  sample.ce_cache_distinct_hit_count = query.config->cardinality_estimation_cache->distinct_hit_count();
  sample.ce_cache_distinct_miss_count = query.config->cardinality_estimation_cache->distinct_miss_count();

  out() << "---- Generated " << join_plans.size() << " plans in " << format_duration(std::chrono::duration_cast<std::chrono::nanoseconds>(sample.planning_duration)) << std::endl;

  /**
   * Initialise the plans
   */
  auto join_plan_iter = join_plans.begin();
  for (auto join_plan_idx = size_t{0}; join_plan_idx < join_plans.size(); ++join_plan_idx, ++join_plan_iter) {
     // Create LQP from join plan
    const auto& join_plan = *join_plan_iter;
    const auto join_ordered_sub_lqp = join_plan.lqp;
    for (const auto &parent_relation : join_graph->output_relations) {
      parent_relation.output->set_input(parent_relation.input_side, join_ordered_sub_lqp);
    }

    lqp_root->print();
    plans.emplace_back(std::make_shared<JoePlan>(*this, lqp_root->left_input()->deep_copy(), join_plan_idx));
  }

  /**
   * Establish the order of plans to evaluate
   */
  std::vector<size_t> plan_indices(join_plans.size());
  std::iota(plan_indices.begin(), plan_indices.end(), 0);

  if (query.config->plan_order_shuffling) {
    if (plan_indices.size() > static_cast<size_t>(*query.config->plan_order_shuffling)) {
      std::random_device rd;
      std::mt19937 g(rd());
      std::shuffle(plan_indices.begin() + *query.config->plan_order_shuffling, plan_indices.end(), g);
    }
  }

  /** 
   * Evaluate the plans
   */
  for (auto plan_idx_idx = size_t{0}; plan_idx_idx < plan_indices.size(); ++plan_idx_idx) {
    const auto plan_idx = plan_indices[plan_idx_idx];
    auto& plan = plans[plan_idx];

    /**
     * Check for break conditions
     */
    if (query.config->max_plan_execution_count && query.plans_execution_count >= *query.config->max_plan_execution_count) {
      out() << "---- Requested number of plans (" << *query.config->max_plan_execution_count << ") executed, stopping" << std::endl;
      break;
    }

    if (config->unique_plans && !query.executed_plans.emplace(plan->lqp).second) {
      if (config->force_plan_zero && plan_idx == 0) {
        out() << "----- Plan was already executed, but is rank#0 and --force-plan-zero is set, so it is executed again" << std::endl;
      } else {
        out() << "----- Plan was already executed, skipping" << std::endl;
        return;
      }
    }

    /**
     * Evaluate the plan
     */
    plan->run();

    /**
     * Update best_plan
     */
    if (!sample.best_plan || plan->sample.execution_duration < sample.best_plan->sample.execution_duration) {
      sample.best_plan = plan;
    }

    /**
     * Timeout query
     */
    if (query.config->query_timeout_seconds) {
      const auto query_duration = std::chrono::steady_clock::now() - query.execution_begin;

      if (std::chrono::duration_cast<std::chrono::seconds>(query_duration).count() >=
          *query.config->query_timeout_seconds) {
        out() << "---- Query timeout" << std::endl;
        break;
      }
    }
  }

  /**
   * Reset
   */
  config->cardinality_estimation_cache->reset_distinct_hit_miss_counts();
}

}  // namespace opossum
