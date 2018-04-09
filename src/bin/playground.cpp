#include <iostream>
#include <fstream>
#include <experimental/filesystem>

#include "types.hpp"

#include "import_export/csv_parser.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/export_binary.hpp"
#include "operators/import_binary.hpp"
#include "storage/storage_manager.hpp"
#include "sql/sql.hpp"

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

    std::cout << "Processing '" << table_name << "'" << std::endl;

    auto table = std::shared_ptr<const Table>();
    if (std::experimental::filesystem::exists(table_binary_path.c_str())) {
      std::cout << "  Loading Binary" << std::endl;
      auto import_binary = std::make_shared<ImportBinary>(table_binary_path);
      import_binary->execute();
      table = import_binary->get_output();
    } else {
      std::cout << "  Loading CSV" << std::endl;
      table = CsvParser{}.parse(table_csv_path);
    }
    std::cout << "   Done: " << table->row_count() << " rows" << std::endl;

    std::cout << "  Adding to StorageManager" << std::endl;
    StorageManager::get().add_table(table_name, std::const_pointer_cast<Table>(table));
    std::cout << "   Done" << std::endl;

    if (!std::experimental::filesystem::exists(table_binary_path.c_str())) {
      std::cout << "  Saving binary" << std::endl;
      auto table_wrapper = std::make_shared<TableWrapper>(table);
      table_wrapper->execute();
      auto export_binary = std::make_shared<ExportBinary>(table_wrapper, table_binary_path);
      export_binary->execute();
      std::cout << "   Done" << std::endl;
    }
  }

  /**
   *
   */
  auto query_file_names = std::vector<std::string>({
     "10b.sql",
     "10c.sql",
     "11a.sql",
     "11b.sql",
     "11c.sql",
     "11d.sql",
     "12a.sql",
     "12b.sql",
     "12c.sql",
     "13a.sql",
     "13b.sql",
     "13c.sql",
     "13d.sql",
     "14a.sql",
     "14b.sql",
     "14c.sql",
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
     "18a.sql",
     "18b.sql",
     "18c.sql",
     "19a.sql",
     "19b.sql",
     "19c.sql",
     "19d.sql",
     "1a.sql",
     "1b.sql",
     "1c.sql",
     "1d.sql",
     "20a.sql",
     "20b.sql",
     "20c.sql",
     "21a.sql",
     "21b.sql",
     "21c.sql",
     "22a.sql",
     "22b.sql",
     "22c.sql",
     "22d.sql",
     "23a.sql",
     "23b.sql",
     "23c.sql",
     "24a.sql",
     "24b.sql",
     "25a.sql",
     "25b.sql",
     "25c.sql",
     "26a.sql",
     "26b.sql",
     "26c.sql",
     "27a.sql",
     "27b.sql",
     "27c.sql",
     "28a.sql",
     "28b.sql",
     "28c.sql",
     "29a.sql",
     "29b.sql",
     "29c.sql",
     "2a.sql",
     "2b.sql",
     "2c.sql",
     "2d.sql",
     "30a.sql",
     "30b.sql",
     "30c.sql",
     "31a.sql",
     "31b.sql",
     "31c.sql",
     "32a.sql",
     "32b.sql",
     "33a.sql",
     "33b.sql",
     "33c.sql",
     "3a.sql",
     "3b.sql",
     "3c.sql",
     "4a.sql",
     "4b.sql",
     "4c.sql",
     "5a.sql",
     "5b.sql",
     "5c.sql",
     "6a.sql",
     "6b.sql",
     "6c.sql",
     "6d.sql",
     "6e.sql",
     "6f.sql",
     "7a.sql",
     "7b.sql",
     "7c.sql",
     "8a.sql",
     "8b.sql",
     "8c.sql",
     "8d.sql",
     "9a.sql",
     "9b.sql",
     "9c.sql",
     "9d.sql"
  });

  auto query_file_directory = std::string{"/home/Moritz.Eyssen/hyrise/third_party/join-order-benchmark/"};

  for (const auto& query_file_name : query_file_names) {
    const auto query_file_path = query_file_directory + query_file_name;

    std::ifstream query_file(query_file_path);

    std::cout << "File: " << query_file_path << " - " << query_file.good() << std::endl;

    query_file.seekg(0, std::ios::end);
    const auto size = query_file.tellg();
    auto query_string = std::string(static_cast<size_t>(size), ' ');
    query_file.seekg(0, std::ios::beg);
    query_file.read(&query_string[0], size);

    std::cout << "Running Query\n" << query_string << std::endl;

    auto pipeline = SQL{query_string}.pipeline();

    pipeline.get_optimized_logical_plans().at(0)->print();

    auto table = pipeline.get_result_table();

    std::cout << "  Rows: " << table->row_count();
  }

  return 0;
}
