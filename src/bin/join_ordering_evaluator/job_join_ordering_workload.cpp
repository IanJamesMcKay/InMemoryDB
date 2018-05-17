#include "job_join_ordering_workload.hpp"

#include "out.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

JobWorkload::JobWorkload(const std::optional<std::vector<std::string>>& query_names)
{
  if (query_names) {
    _query_names = *query_names;
  } else {
    _query_names = {
    "1a", "1b", "1c", "1d", "2a", "2b", "2c", "2d",
    // "3a", "3b", "3c"
    "4a", "4b", "4c",
    // "5a", "5b", "5c",
    "6a",
    // "6b",
    "6c",
    //"6d",
    "6e",
    //"6f",
    "7a", "7b",
    //"7c",
    "8a", "8b", "8c", "8d",
    //"9a",
    "9b",
    // "9c", "9d",
    "10a", "10b", "10c", "11a", "11b",
    //"11c", "11d", "12a", "12b", "12c",
    "13a", "13b", "13c", "13d",
    //"14a", "14b", "14c",
    "15a", "15b", "15c", "15d", "16a", "16b", "16c", "16d", "17a", "17b",
    "17c", "17d", "17e", "17f",
    //"18a","18b", "18c", "19a",
    "19b",
    //"19c","19d", "20a", "20b", "20c", "21a", "21b", "21c", "22a", "22b",
    //"22c","22d", "23a", "23b", "23c", "24a",
//     "24b",
//     "25a",
//     "25b",
//     "25c",
//     "26a",
//     "26b",
//     "26c",
//     "27a",
//     "27b",
//     "27c",
//     "28a",
//     "28b",
//     "28c",
//     "29a",
//     "29b",
//     "29c",
//     "30a",
//     "30b",
//     "30c",
//     "31a",
//     "31b",
//     "31c",
    "32a",
    "32b",
    //"33a",
    "33b",
    // "33c"
    };

    for (auto q : _query_names) std::cout << q << std::endl;
  }
}

void JobWorkload::setup() {
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

    out() << "Processing '" << table_name << "'" << std::endl;

    Timer timer;
    auto table = std::shared_ptr<Table>();
    if (std::experimental::filesystem::exists(table_binary_path.c_str())) {
      out() << "  Loading Binary" << std::endl;
      auto import_binary = std::make_shared<ImportBinary>(table_binary_path);
      import_binary->execute();
      table = std::const_pointer_cast<Table>(import_binary->get_output());
    } else {
      out() << "  Loading CSV" << std::endl;
      table = CsvParser{}.parse(table_csv_path);
    }
    out() << "   Done: " << table->row_count() << " rows" << std::endl;
    out() << "   Duration: " << format_duration(std::chrono::duration_cast<std::chrono::nanoseconds>(timer.lap()))
          << std::endl;

    try {
      if (std::experimental::filesystem::exists(table_statistics_path.c_str())) {
        out() << "  Loading Statistics" << std::endl;
        const auto table_statistics = import_table_statistics(table_statistics_path);
        table->set_table_statistics(std::make_shared<TableStatistics>(table_statistics));
        out() << "   Done" << std::endl;
      }
    } catch (const std::exception& e) {
      out() << "ERROR while importing statistics: " << e.what() << std::endl;
    }

    out() << "  Adding to StorageManager" << std::endl;
    StorageManager::get().add_table(table_name, std::const_pointer_cast<Table>(table));
    out() << "   Done" << std::endl;
    out() << "   Duration: " << format_duration(std::chrono::duration_cast<std::chrono::nanoseconds>(timer.lap()))
          << std::endl;

    if (!std::experimental::filesystem::exists(table_statistics_path.c_str())) {
      out() << "  Exporting Statistics" << std::endl;
      export_table_statistics(*table->table_statistics(), table_statistics_path);
      out() << "   Done" << std::endl;
    }

    if (!std::experimental::filesystem::exists(table_binary_path.c_str())) {
      out() << "  Saving binary" << std::endl;
      auto table_wrapper = std::make_shared<TableWrapper>(table);
      table_wrapper->execute();
      auto export_binary = std::make_shared<ExportBinary>(table_wrapper, table_binary_path);
      export_binary->execute();
      out() << "   Done" << std::endl;
      out() << "   Duration: " << format_duration(std::chrono::duration_cast<std::chrono::nanoseconds>(timer.lap()))
            << std::endl;
    }
  }
}

size_t JobWorkload::query_count() const {
  return _query_names.size();
}

std::string JobWorkload::get_query(const size_t query_idx) const {
  auto query_file_directory = std::string{"/home/Moritz.Eyssen/hyrise/third_party/join-order-benchmark/"};

  const auto query_file_path = query_file_directory + _query_names[query_idx] + ".sql";

  std::ifstream query_file(query_file_path);
  Assert(query_file.good(), std::string("Failed to open '") + query_file_path + "'");

  query_file.seekg(0, std::ios::end);
  const auto size = query_file.tellg();
  auto query_string = std::string(static_cast<size_t>(size), ' ');
  query_file.seekg(0, std::ios::beg);
  query_file.read(&query_string[0], size);

  return query_string;
}

std::string JobWorkload::get_query_name(const size_t query_idx) const {
  return "JOB"s + "-" + _query_names[query_idx];
}

}  // namespace opossum
