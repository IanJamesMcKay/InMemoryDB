#include <iostream>
#include <fstream>
#include <experimental/filesystem>

#include "json.hpp"

#include "types.hpp"

#include "import_export/csv_parser.hpp"
#include "cost_model/cost_model_linear.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/print.hpp"
#include "operators/export_binary.hpp"
#include "operators/import_binary.hpp"
#include "operators/limit.hpp"
#include "storage/storage_manager.hpp"
#include "sql/sql.hpp"
#include "utils/timer.hpp"
#include "utils/format_duration.hpp"
#include "statistics/statistics_import_export.hpp"
#include "planviz/sql_query_plan_visualizer.hpp"

using namespace opossum;  // NOLINT

int main() {
  nlohmann::json json;

  std::wstring clara_w = L"Clara Øckenholt";

  json["Actor"] = clara_w;

  std::ofstream ofile("test.json");
  ofile << json;
  ofile.close();

  std::ifstream ifile("test.json");
  nlohmann::json json2;
  ifile >> json2;

  auto clara = json2["Actor"].get<std::wstring>();

  for (auto c_idx = size_t{0}; c_idx < clara.size(); ++c_idx) {
    std::cout << clara[c_idx] << ": " << (int)(clara[c_idx]) << std::endl;
  }

  // std::cout << "Clara: " << clara << std::endl;

  return 0;
}
