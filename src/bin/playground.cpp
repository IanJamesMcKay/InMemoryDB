#include <chrono>
#include <iostream>
#include <fstream>
#include <experimental/filesystem>

#include <json.hpp>

#include "types.hpp"

#include "import_export/csv_parser.hpp"
#include "cost_model/cost_model_linear.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/print.hpp"
#include "operators/export_binary.hpp"
#include "operators/import_binary.hpp"
#include "operators/import_csv.hpp"
#include "operators/limit.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/chunk.hpp"
#include "storage/base_column.hpp"
#include "storage/table.hpp"
#include "storage/storage_manager.hpp"
#include "sql/sql.hpp"
#include "utils/timer.hpp"
#include "utils/format_duration.hpp"
#include "statistics/statistics_import_export.hpp"
#include "planviz/sql_query_plan_visualizer.hpp"
#include "resolve_type.hpp"
#include "utils/format_duration.hpp"

using namespace opossum;  // NOLINT
using namespace std::string_literals;  // NOLINT

void load_dummy_table(const std::string& name) {
  const auto import_csv = std::make_shared<ImportCsv>("statistics/dummy.csv", name, "statistics/"s + name + ".csv.json");
  import_csv->execute();

  const auto table = import_csv->get_output();

  const auto statistics = import_table_statistics("statistics/"s + name + ".statistics.json");
  table->set_table_statistics(std::make_shared<TableStatistics>(statistics));

  StorageManager::get().add_table(name, table);
}

int main(int argc, char ** argv) {



  return 0;
}
