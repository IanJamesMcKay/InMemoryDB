#include "joe_query_iteration.hpp"

#include <sstream>

#include "sql/sql.hpp"
#include "utils/timer.hpp"
#include "utils/format_duration.hpp"

#include "out.hpp"
#include "joe_query.hpp"
#include "write_csv.hpp"
#include "joe.hpp"

namespace opossum {

std::ostream &operator<<(std::ostream &stream, const JoeQueryIterationSample &sample) {
  stream << (sample.rank_zero_plan ? sample.rank_zero_plan->sample.execution_duration.count() : 0) << ",";
  stream << (sample.best_plan ? sample.best_plan->sample.execution_duration.count() : 0) << ",";

  stream << sample.planning_duration.count() << ",";
  stream << sample.cecaching_duration.count() << ",";
  stream << sample.ce_cache_hit_count << ",";
  stream << sample.ce_cache_miss_count << ",";
  stream << sample.ce_cache_size << ",";
  stream << sample.ce_cache_distinct_hit_count << ",";;
  stream << sample.ce_cache_distinct_miss_count << ",";

  stream << (sample.rank_zero_plan ? sample.rank_zero_plan->sample.hash : 0) << ",";
  stream << (sample.best_plan ? sample.best_plan->sample.hash : 0);

  return stream;
}

JoeQueryIteration::JoeQueryIteration(JoeQuery& query, const size_t idx):
  query(query), idx(idx) {
}

void JoeQueryIteration::run() {
  const auto config = query.joe.config;

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
  join_graph = JoinGraph::from_lqp(unoptimized_lqp);
  const auto plan_generation_count = config->max_plan_generation_count ? *config->max_plan_generation_count : DpSubplanCacheTopK::NO_ENTRY_LIMIT;
  DpCcpTopK dp_ccp_top_k{plan_generation_count, config->cost_model, config->main_cardinality_estimator};
  dp_ccp_top_k(join_graph);
  JoinVertexSet all_vertices{join_graph->vertices.size()};
  all_vertices.flip();
  const auto join_plans = dp_ccp_top_k.subplan_cache()->get_best_plans(all_vertices);

  /**
   * Take measurements about Plan Generation
   */
  sample.planning_duration = std::chrono::duration_cast<std::chrono::microseconds>(timer.lap());
  sample.ce_cache_hit_count = config->cardinality_estimation_cache->cache_hit_count();
  sample.ce_cache_miss_count = config->cardinality_estimation_cache->cache_miss_count();
  sample.ce_cache_size = config->cardinality_estimation_cache->size();
  sample.ce_cache_distinct_hit_count = config->cardinality_estimation_cache->distinct_hit_count();
  sample.ce_cache_distinct_miss_count = config->cardinality_estimation_cache->distinct_miss_count();

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

    plans.emplace_back(std::make_shared<JoePlan>(*this, join_plan.plan_cost, lqp_root->left_input()->deep_copy(), join_plan_idx));
  }

  if (!plans.empty()) {
    sample.rank_zero_plan = plans.front();
  }

  /**
   * Establish the order of plans to evaluate
   */
  std::vector<size_t> plan_indices(join_plans.size());
  std::iota(plan_indices.begin(), plan_indices.end(), 0);

  if (config->plan_order_shuffling) {
    if (plan_indices.size() > static_cast<size_t>(*config->plan_order_shuffling)) {
      std::random_device rd;
      std::mt19937 g(rd());
      std::shuffle(plan_indices.begin() + *config->plan_order_shuffling, plan_indices.end(), g);
    }
  }

  // Make sure this happens even if we're not executing plans
  write_plans_csv();

  /** 
   * Evaluate the plans
   */
  for (auto plan_idx_idx = size_t{0}; plan_idx_idx < plan_indices.size(); ++plan_idx_idx) {
    const auto plan_idx = plan_indices[plan_idx_idx];
    auto& plan = plans[plan_idx];

    /**
     * Check for break conditions
     */
    if (config->max_plan_execution_count && plans_execution_count >= *config->max_plan_execution_count) {
      out() << "---- Requested number of plans (" << *config->max_plan_execution_count << ") executed, stopping" << std::endl;
      break;
    }

    if (config->unique_plans && !query.executed_plans.emplace(plan->lqp).second) {
      if (config->force_plan_zero && plan_idx == 0) {
        out() << "----- Plan was already executed, but is rank#0 and --force-plan-zero is set, so it is executed again" << std::endl;
      } else {
        out() << "----- Plan was already executed, skipping" << std::endl;
        continue;
      }
    }

    /**
     * Evaluate the plan
     */
    plan->run();

    /**
     * Update plan related data
     */
    if (!sample.best_plan || plan->sample.execution_duration < sample.best_plan->sample.execution_duration) {
      sample.best_plan = plan;
    }

    sample.cecaching_duration += plan->sample.cecaching_duration;

    // Update with timings
    write_plans_csv();

    /**
     * Timeout query
     */
    if (config->query_timeout_seconds) {
      const auto query_duration = std::chrono::steady_clock::now() - query.execution_begin;

      if (std::chrono::duration_cast<std::chrono::seconds>(query_duration).count() >=
          *config->query_timeout_seconds) {
        out() << "---- Query timeout" << std::endl;
        break;
      }
    }
  }

  dump_cardinality_estimation_cache();

  /**
   * Reset
   */
  config->cardinality_estimation_cache->reset_distinct_hit_miss_counts();
}

void JoeQueryIteration::dump_cardinality_estimation_cache() {
  const auto config = query.joe.config;
  if (!config->cardinality_estimation_cache_dump) return;

  std::ofstream stream{prefix() + "CardinalityEstimationCache.Dump.log"};
  config->cardinality_estimation_cache->print(stream);
}

void JoeQueryIteration::write_plans_csv() {
  const auto config = query.joe.config;
  if (config->save_plan_results) {
    write_csv(plans,
              "ExecutionDuration,EstCost,ReEstCost,AimCost,AbsEstCostError,AbsReEstCostError,CECachingDuration,Hash",
              prefix() + "Plans.csv");
  }
}

std::string JoeQueryIteration::prefix() const {
  std::stringstream stream;
  stream << query.prefix() << "QIteration." << idx << ".";
  return stream.str();
}

}  // namespace opossum
