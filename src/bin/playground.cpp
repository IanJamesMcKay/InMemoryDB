#include <iostream>

#include "types.hpp"

#include "import_export/csv_parser.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/export_binary.hpp"
#include "storage/table.hpp"

using namespace opossum;  // NOLINT

int main() {
  const auto table_names = std::vector<std::string>{
    "aka_name",
    "aka_title",
    "cast_info",
    "char_name",
    "comp_cast_type",
    "company_name",
    //"company_type",
    "complete_cast",
    //"info_type",
    "keyword",
    //"kind_type",
    //"link_type",
    "movie_companies",
    "movie_info",
    "movie_info_idx",
    "movie_keyword",
    "movie_link",
    "name",
    "person_info",
    //"role_type",
    "title"
  };

  const auto csvs_path = "/home/moritz/Coding/imdb/csv/";

  for (const auto& table_name : table_names) {
    const auto table_csv_path = csvs_path + table_name + ".csv";
    const auto table_binary_path = csvs_path + table_name + ".bin";

    std::cout << "Processing " << table_name << std::endl;

    std::cout << "  Loading CSV" << std::endl;
    const auto table = CsvParser{}.parse(table_csv_path);
    std::cout << "   Done: " << table->row_count() << " rows" << std::endl;

    std::cout << "  Saving binary" << std::endl;
    auto table_wrapper = std::make_shared<TableWrapper>(table);
    table_wrapper->execute();
    auto export_binary = std::make_shared<ExportBinary>(table_wrapper, table_binary_path);
    export_binary->execute();
    std::cout << "   Done" << std::endl;
  }

  return 0;
}
