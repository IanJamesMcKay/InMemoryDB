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

  const auto csvs_path = "statistics/";

  for (const auto& table_name : table_names) {
    const auto table_csv_path = csvs_path + table_name + ".csv";
    const auto table_statistics_path = csvs_path + table_name + ".statistics.json";

    auto table = std::shared_ptr<Table>();
    table = CsvParser{}.parse(table_csv_path);
    const auto table_statistics = import_table_statistics(table_statistics_path);
    table->set_table_statistics(std::make_shared<TableStatistics>(table_statistics));

    StorageManager::get().add_table(table_name, table);
  }

  const auto query = "SELECT an.name AS cool_actor_pseudonym,\n"
                     "       t.title AS series_named_after_char\n"
                     "FROM aka_name AS an,\n"
                     "     cast_info AS ci,\n"
                     "     company_name AS cn,\n"
                     "     keyword AS k,\n"
                     "     movie_companies AS mc,\n"
                     "     movie_keyword AS mk,\n"
                     "     name AS n,\n"
                     "     title AS t\n"
                     "WHERE cn.country_code ='[us]'\n"
                     "  AND k.keyword ='character-name-in-title'\n"
                     "  AND t.episode_nr >= 50\n"
                     "  AND t.episode_nr < 100\n"
                     "  AND an.person_id = n.id\n"
                     "  AND n.id = ci.person_id\n"
                     "  AND ci.movie_id = t.id\n"
                     "  AND t.id = mk.movie_id\n"
                     "  AND mk.keyword_id = k.id\n"
                     "  AND t.id = mc.movie_id\n"
                     "  AND mc.company_id = cn.id\n"
                     "  AND an.person_id = ci.person_id\n"
                     "  AND ci.movie_id = mc.movie_id\n"
                     "  AND ci.movie_id = mk.movie_id\n"
                     "  AND mc.movie_id = mk.movie_id;";

  auto pipeline_statement = SQL{query}.set_use_mvcc(UseMvcc::No).pipeline_statement();
  auto lqp = pipeline_statement.get_optimized_logical_plan();
  auto pqp = pipeline_statement.get_query_plan();

  lqp->print();

  GraphvizConfig graphviz_config;
  graphviz_config.format = "svg";
  VizGraphInfo viz_graph_info;
  viz_graph_info.bg_color = "black";

  SQLQueryPlanVisualizer visualizer{graphviz_config, viz_graph_info, {}, {}};
  visualizer.visualize(*pqp, "tmp.dot",
                       "tmp.svg");

  return 0;
}
