#include <iostream>
#include <fstream>
#include <experimental/filesystem>

#include "types.hpp"

#include "import_export/csv_parser.hpp"
#include "optimizer/join_ordering/cost_model_linear.hpp"
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
  const auto table_names = std::vector<std::string>{
    "aka_name",
    "aka_title",
    "cast_info",
    "char_name",
    "comp_cast_type",
    "company_name",
    "company_type",
    "complete_cast",
    "info_type",
    "keyword",
    "kind_type",
    "link_type",
    "movie_companies",
    "movie_info",
    "movie_info_idx",
    "movie_keyword",
    "movie_link",
    "name",
    "person_info",
    "role_type",
    "title"
  };


  const auto csvs_path = "/home/Moritz.Eyssen/imdb/csv/";

  for (const auto& table_name : table_names) {
    const auto table_csv_path = csvs_path + table_name + ".csv";
    const auto table_binary_path = csvs_path + table_name + ".bin";
    const auto table_statistics_path = csvs_path + table_name + ".statistics.json";

    std::cout << "Processing '" << table_name << "'" << std::endl;

    Timer timer;
    auto table = std::shared_ptr<Table>();
    if (std::experimental::filesystem::exists(table_binary_path.c_str())) {
      std::cout << "  Loading Binary" << std::endl;
      auto import_binary = std::make_shared<ImportBinary>(table_binary_path);
      import_binary->execute();
      table = std::const_pointer_cast<Table>(import_binary->get_output());
    } else {
      std::cout << "  Loading CSV" << std::endl;
      table = CsvParser{}.parse(table_csv_path);
    }
    std::cout << "   Done: " << table->row_count() << " rows" << std::endl;
    std::cout << "   Duration: " << format_duration(std::chrono::duration_cast<std::chrono::nanoseconds>(timer.lap()))
              << std::endl;

    try {
      if (std::experimental::filesystem::exists(table_statistics_path.c_str())) {
        std::cout << "  Loading Statistics" << std::endl;
        const auto table_statistics = import_table_statistics(table_statistics_path);
        table->set_table_statistics(std::make_shared<TableStatistics>(table_statistics));
        std::cout << "   Done" << std::endl;
      }
    } catch (const std::exception& e) {
      std::cout << "ERROR while importing statistics: " << e.what() << std::endl;
    }

    std::cout << "  Adding to StorageManager" << std::endl;
    StorageManager::get().add_table(table_name, std::const_pointer_cast<Table>(table));
    std::cout << "   Done" << std::endl;
    std::cout << "   Duration: " << format_duration(std::chrono::duration_cast<std::chrono::nanoseconds>(timer.lap()))
              << std::endl;

    if (!std::experimental::filesystem::exists(table_statistics_path.c_str())) {
      std::cout << "  Exporting Statistics" << std::endl;
      export_table_statistics(*table->table_statistics(), table_statistics_path);
      std::cout << "   Done" << std::endl;
    }

    if (!std::experimental::filesystem::exists(table_binary_path.c_str())) {
      std::cout << "  Saving binary" << std::endl;
      auto table_wrapper = std::make_shared<TableWrapper>(table);
      table_wrapper->execute();
      auto export_binary = std::make_shared<ExportBinary>(table_wrapper, table_binary_path);
      export_binary->execute();
      std::cout << "   Done" << std::endl;
      std::cout << "   Duration: " << format_duration(std::chrono::duration_cast<std::chrono::nanoseconds>(timer.lap()))
                << std::endl;
    }
  }

  /**
   *
   */
  auto query_file_names = std::vector<std::string>({
     "1a.sql",
     "1b.sql",
     "1c.sql",
     "1d.sql",
     "2a.sql",
     "2b.sql",
     "2c.sql",
     "2d.sql",
     // "3a.sql", Uses IN
     // "3b.sql", Uses IN
     // "3c.sql", Uses IN
     "4a.sql",
     "4b.sql",
     "4c.sql",
     // "5a.sql",
     // "5b.sql",
     // "5c.sql",
     "6a.sql",
     // "6b.sql",
     "6c.sql",
     //"6d.sql",
     "6e.sql",
     //"6f.sql",
     "7a.sql",
     "7b.sql",
     //"7c.sql",
     "8a.sql",
     "8b.sql",
     "8c.sql",
     "8d.sql",
     //"9a.sql",
     "9b.sql",
    // "9c.sql",
    // "9d.sql",
     "10a.sql",
     "10b.sql",
     "10c.sql",
     "11a.sql",
     "11b.sql",
     //"11c.sql",
     //"11d.sql",
     //"12a.sql",
     "12b.sql",
     //"12c.sql",
     "13a.sql",
     "13b.sql",
     "13c.sql",
     "13d.sql",
     //"14a.sql",
     //"14b.sql",
     //"14c.sql",
     "15a.sql",
     "15b.sql",
     "15c.sql",
     "15d.sql",
     "16a.sql",
     "16b.sql",
     "16c.sql",
     "16d.sql",
     "17a.sql",
     "17b.sql",
     "17c.sql",
     "17d.sql",
     "17e.sql",
     "17f.sql",
     //"18a.sql",
     //"18b.sql",
     //"18c.sql",
     //"19a.sql",
     "19b.sql",
     //"19c.sql",
     //"19d.sql",
     //"20a.sql",
     //"20b.sql",
     //"20c.sql",
     //"21a.sql",
     //"21b.sql",
     //"21c.sql",
     //"22a.sql",
     //"22b.sql",
     //"22c.sql",
     //"22d.sql",
     "23a.sql",
     //"23b.sql",
//     "23c.sql",
//     "24a.sql",
//     "24b.sql",
//     "25a.sql",
//     "25b.sql",
//     "25c.sql",
//     "26a.sql",
//     "26b.sql",
//     "26c.sql",
//     "27a.sql",
//     "27b.sql",
//     "27c.sql",
//     "28a.sql",
//     "28b.sql",
//     "28c.sql",
//     "29a.sql",
//     "29b.sql",
//     "29c.sql",
//     "30a.sql",
//     "30b.sql",
//     "30c.sql",
//     "31a.sql",
//     "31b.sql",
//     "31c.sql",
     "32a.sql",
     "32b.sql",
     //"33a.sql",
     "33b.sql",
    // "33c.sql",
  });

  auto query_file_directory = std::string{"/home/Moritz.Eyssen/hyrise/third_party/join-order-benchmark/"};

  for (const auto& query_file_name : query_file_names) {
   try {
      const auto query_file_path = query_file_directory + query_file_name;

      std::ifstream query_file(query_file_path);

      query_file.seekg(0, std::ios::end);
      const auto size = query_file.tellg();
      auto query_string = std::string(static_cast<size_t>(size), ' ');
      query_file.seekg(0, std::ios::beg);
      query_file.read(&query_string[0], size);

      std::cout << "Running Query " << query_file_name << std::endl;

      auto pipeline = SQL{query_string}.set_use_mvcc(UseMvcc::No).pipeline();

      Timer execution_timer;
      auto table = pipeline.get_result_table();

      std::cout << "  Rows: " << table->row_count() << std::endl;
      std::cout << "  Duration: " << format_duration(std::chrono::duration_cast<std::chrono::nanoseconds>(execution_timer.lap())) << std::endl;

      {
        auto tw = std::make_shared<TableWrapper>(pipeline.get_result_table());
        tw->execute();
        auto limit = std::make_shared<Limit>(tw, 20'000);
        limit->execute();
        std::ofstream file(query_file_name + ".result");
        Print::print(limit->get_output(), 0, file);
      }

      GraphvizConfig graphviz_config;
      graphviz_config.format = "svg";
      VizGraphInfo viz_graph_info;
      viz_graph_info.bg_color = "black";

      SQLQueryPlanVisualizer visualizer{graphviz_config, viz_graph_info, {}, {}};
      visualizer.set_cost_model(std::make_shared<CostModelLinear>());
      visualizer.visualize(*pipeline.get_query_plans().at(0), "tmp.dot",
                           query_file_name + ".svg");
    } catch (const std::exception& e) {
    std::cout << "ERROR: " << e.what() << std::endl;
    }
  }

  return 0;
}
