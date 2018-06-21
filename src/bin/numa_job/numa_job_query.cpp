#include "numa_job_query.hpp"

#include <chrono>

#include "scheduler/current_scheduler.hpp"
#include "utils/timer.hpp"
#include "sql/sql_pipeline_builder.hpp"

#include "out.hpp"

namespace opossum {

std::ostream &operator<<(std::ostream &stream, const NumaJobQuerySample &sample) {
  stream << sample.name << ",";

  if (true) {
  // if (sample.best_plan) {
    // stream << sample.best_plan->sample.execution_duration.count();
    stream << sample.execution_duration.count();
  } else {
    stream << 0;
  }

  return stream;
}

NumaJobQuery::NumaJobQuery(const std::shared_ptr<NumaJobConfig>& config, const std::string& name, const std::string& sql):
  config(config), sql(sql) {
  sample.name = name;
}

void NumaJobQuery::run() {
  out() << "-- Running Query: " << sample.name << std::endl;
  // execution_begin = std::chrono::steady_clock::now();

  auto tasks = std::vector<std::shared_ptr<OperatorTask>>();

  for (auto i = size_t{0} ; i < config->iterations_per_query ; ++i) {
    auto pipeline = SQLPipelineBuilder{sql}.with_mvcc(UseMvcc::No).create_pipeline();

    for (auto task_set : pipeline.get_tasks()) {
      for (auto task : task_set) {
        tasks.emplace_back(std::move(task));
      }
    }
  }

  auto timer = Timer{};
  CurrentScheduler::schedule_and_wait_for_tasks(tasks);
  auto total_duration = timer.lap();

  sample.execution_duration = total_duration / config->iterations_per_query;

  out() << "-- Time per Iteration: " << sample.execution_duration.count() << std::endl;
}

}  // namespace opossum
