#include <iostream>

#include <json.hpp>

#include <jit_evaluation_helper.hpp>
#include <operators/jit_operator/specialization/jit_repository.hpp>
#include <planviz/lqp_visualizer.hpp>
#include <planviz/sql_query_plan_visualizer.hpp>
#include <sql/sql_pipeline.hpp>
#include <tpch/tpch_db_generator.hpp>

void lqp() {
  std::string query_id = opossum::JitEvaluationHelper::get().experiment().at("query_id");
  bool optimize = opossum::JitEvaluationHelper::get().experiment().at("optimize");
  std::string output_file = opossum::JitEvaluationHelper::get().experiment().at("output_file");

  std::string query = opossum::JitEvaluationHelper::get().queries().at(query_id).at("query");

  opossum::SQLPipeline pipeline(query);
  const auto plans = optimize ? pipeline.get_optimized_logical_plans() : pipeline.get_unoptimized_logical_plans();

  opossum::LQPVisualizer visualizer;
  visualizer.visualize(plans, output_file + ".dot", output_file + ".png");
}

void pqp() {
  std::string query_id = opossum::JitEvaluationHelper::get().experiment().at("query_id");
  std::string output_file = opossum::JitEvaluationHelper::get().experiment().at("output_file");

  std::string query = opossum::JitEvaluationHelper::get().queries().at(query_id).at("query");

  opossum::SQLPipeline pipeline(query);
  opossum::SQLQueryPlan query_plan;
  const auto plans = pipeline.get_query_plans();
  for (const auto& plan : plans) {
    query_plan.append_plan(*plan);
  }

  opossum::SQLQueryPlanVisualizer visualizer;
  visualizer.visualize(query_plan, output_file + ".dot", output_file + ".png");
}

void run() {
  std::string query_id = opossum::JitEvaluationHelper::get().experiment().at("query_id");
  std::string query = opossum::JitEvaluationHelper::get().queries().at(query_id).at("query");
  opossum::SQLPipeline pipeline(query);
  const auto table = pipeline.get_result_table();
  opossum::Print::print(table, 0, std::cerr);
  std::cerr << "rows " << table->row_count() << std::endl;
  std::cerr << "compile " << pipeline.compile_time_microseconds().count() / 1000.0 << std::endl;
  std::cerr << "compile " << pipeline.execution_time_microseconds().count() / 1000.0 << std::endl;
}

int main(int argc, char* argv[]) {
  std::cerr << "Starting the JIT benchmarking suite" << std::endl;

  freopen("input.json", "r", stdin);
  freopen("output.json", "w", stdout);

  nlohmann::json config;
  std::cin >> config;
  opossum::JitEvaluationHelper::get().queries() = config.at("queries");

  double scale_factor = config.at("scale_factor").get<double>();
  std::cerr << "Generating tables with scale factor " << scale_factor << std::endl;
  opossum::TpchDbGenerator generator(scale_factor);
  generator.generate_and_store();

  std::cerr << "Initializing JIT repository" << std::endl;
  opossum::JitRepository::get();

  for (const auto& experiment : config.at("experiments")) {
    opossum::JitEvaluationHelper::get().experiment() = experiment;
    if (experiment.at("task") == "lqp") {
      lqp();
    } else if (experiment.at("task") == "pqp") {
      pqp();
    } else if (experiment.at("task") == "run") {
      run();
    }
  }

  std::cerr << "Bye" << std::endl;
}
