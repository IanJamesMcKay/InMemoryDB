#include <iostream>

#include <json.hpp>

#include <jit/jit_table_generator.hpp>
#include <jit_evaluation_helper.hpp>
#include <operators/jit_operator/specialization/jit_repository.hpp>
#include <planviz/lqp_visualizer.hpp>
#include <planviz/sql_query_plan_visualizer.hpp>
#include <sql/sql_pipeline.hpp>
#include <tpch/tpch_db_generator.hpp>
#include <resolve_type.hpp>

void remove_table_from_cache(opossum::Table& table) {
  const size_t cache_line = 64;

  for (opossum::ChunkID chunk_id{0}; chunk_id < table.chunk_count(); ++chunk_id) {
    auto chunk = table.get_chunk(chunk_id);
    for (opossum::ColumnID column_id{0}; column_id < table.column_count(); ++column_id) {
      auto column = chunk->get_column(column_id);
      const auto data_type = table.column_type(column_id);
      opossum::resolve_data_type(data_type, [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;

        if (auto value_column = std::dynamic_pointer_cast<const opossum::ValueColumn<ColumnDataType>>(column)) {
          for (auto& value : value_column->values()) {
            const auto address = reinterpret_cast<size_t>(&value);
            if (address % cache_line == 0) {
              asm volatile("clflush (%0)\n\t" : : "r"(&value) : "memory");
            }
          }
          if (table.column_is_nullable(column_id)) {
            for (auto& value : value_column->null_values()) {
              const auto address = reinterpret_cast<size_t>(&value);
              if (address % cache_line == 0) {
                asm volatile("clflush (%0)\n\t" : : "r"(&value) : "memory");
              }
            }
          }
          asm volatile("sfence\n\t" : : : "memory");
        } else {
          throw std::logic_error("could not flush cache, unknown column type");
        }
      });
    }
  }
}

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

  const auto table_names = opossum::JitEvaluationHelper::get().queries().at(query_id).at("tables");
  for (const auto& table_name : table_names) {
    auto table = opossum::StorageManager::get().get_table(table_name.get<std::string>());
    remove_table_from_cache(*table);
  }

  opossum::SQLPipeline pipeline(query);
  const auto table = pipeline.get_result_table();
}

int main(int argc, char* argv[]) {
  std::cerr << "Starting the JIT benchmarking suite" << std::endl;

  freopen("input.json", "r", stdin);
  freopen("output.json", "w", stdout);

  nlohmann::json config;
  std::cin >> config;
  opossum::JitEvaluationHelper::get().queries() = config.at("queries");

  if (config.count("tpch_scale_factor")) {
    double scale_factor = config.at("tpch_scale_factor").get<double>();
    std::cerr << "Generating TPCH tables with scale factor " << scale_factor << std::endl;
    opossum::TpchDbGenerator generator(scale_factor);
    generator.generate_and_store();
  }

  if (config.count("jit_scale_factor")) {
    double scale_factor = config.at("jit_scale_factor").get<double>();
    std::cerr << "Generating JIT tables with scale factor " << scale_factor << std::endl;
    opossum::JitTableGenerator generator(scale_factor);
    generator.generate_and_store();
  }

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
