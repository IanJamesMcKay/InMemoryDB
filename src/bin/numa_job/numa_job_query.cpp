#include "numa_job_query.hpp"

#include <chrono>

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

  auto pipeline = SQLPipelineBuilder{sql}.create_pipeline();

  execution_begin = std::chrono::steady_clock::now();

  auto timer = Timer{};
  pipeline.get_result_table();
  sample.execution_duration = timer.lap();

  out() << "-- Time: " << sample.execution_duration.count() << std::endl;
}

}  // namespace opossum
