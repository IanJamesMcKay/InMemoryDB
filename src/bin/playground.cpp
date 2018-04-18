#include <chrono>
#include <iostream>
#include <fstream>
#include <experimental/filesystem>

#include "types.hpp"

#include "import_export/csv_parser.hpp"
#include "cost_model/cost_model_linear.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/print.hpp"
#include "operators/export_binary.hpp"
#include "operators/import_binary.hpp"
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

int main(int argc, char ** argv) {
  const auto csvs_path = std::string{"/home/Moritz.Eyssen/imdb/csv/"};
  const auto table_binary_path = csvs_path + "movie_companies.bin";
  auto import_binary = std::make_shared<ImportBinary>(table_binary_path);
  import_binary->execute();
  const auto table = std::const_pointer_cast<Table>(import_binary->get_output());

  auto note_column_id = table->column_id_by_name("note");

  std::vector<std::string> note_values;

  auto num = 0;
  Timer timer;

  for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    auto base_column = table->get_chunk(chunk_id)->get_column(note_column_id);
    resolve_data_and_column_type(table->column_data_type(note_column_id), *base_column, [&](auto type, auto& column) {
      auto iterable = create_iterable_from_column<typename decltype(type)::type>(column);
      iterable.for_each([&](const auto value) {
        if constexpr (std::is_same_v<std::string, typename decltype(type)::type>) {
          if (!value.is_null()) {
            if (value.value().find(argv[1]) != std::string::npos) {
              num++;
            }
          }
        }
      });
    });
  }

  std::cout << "Read " << note_values.size() << " notes from column  " << note_column_id << std::endl;
  std::cout << "Duration: " << timer.lap().count() << std::endl;
  std::cout << num << " co productions found" << std::endl;


  return 0;
}
