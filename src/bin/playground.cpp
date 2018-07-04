#include <chrono>
#include <iostream>
#include <unordered_map>
#include <fstream>
#include <experimental/filesystem>

#include <json.hpp>

#include "types.hpp"

#include "import_export/csv_parser.hpp"
#include "cost_model/cost_model_linear.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
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
  const auto csv_meta = process_csv_meta_file("statistics/"s + name + ".csv.json");

  CsvParser csv_parser;

  const auto table = csv_parser.parse("statistics/"s + name + ".csv");

  const auto statistics = import_table_statistics("statistics/"s + name + ".statistics.json");
  table->set_table_statistics(std::make_shared<TableStatistics>(statistics));

  StorageManager::get().add_table(name, table);
}

int main(int argc, char ** argv) {
  const auto table_names = {"cast_info", "name", "movie_keyword", "keyword", "movie_companies"};

  std::unordered_map<std::string, std::shared_ptr<StoredTableNode>> stored_table_nodes;

  for (const auto& table_name : table_names) {
    load_dummy_table(table_name);
    stored_table_nodes.emplace(table_name, StoredTableNode::make(table_name));
  }


  const auto cast_info_person_id = stored_table_nodes.at("cast_info")->get_column("person_id"s);
  const auto cast_info_movie_id = stored_table_nodes.at("cast_info")->get_column("movie_id"s);
  const auto name_name = stored_table_nodes.at("name")->get_column("name"s);
  const auto name_id = stored_table_nodes.at("name")->get_column("id"s);
  const auto movie_keyword_keyword_id = stored_table_nodes.at("movie_keyword")->get_column("keyword_id"s);
  const auto movie_keyword_movie_id = stored_table_nodes.at("movie_keyword")->get_column("movie_id"s);
  const auto keyword_id = stored_table_nodes.at("keyword")->get_column("id"s);
  const auto keyword_keyword = stored_table_nodes.at("keyword")->get_column("keyword"s);
  const auto movie_companies_movie_id = stored_table_nodes.at("movie_companies")->get_column("id"s);

  const auto lqp =
  PredicateNode::make(cast_info_movie_id, PredicateCondition::Equals, movie_companies_movie_id,
    JoinNode::make(JoinMode::Inner, std::make_pair(movie_keyword_movie_id, movie_companies_movie_id), PredicateCondition::Equals,
      JoinNode::make(JoinMode::Inner, std::make_pair(movie_keyword_keyword_id, keyword_id), PredicateCondition::Equals,
        JoinNode::make(JoinMode::Inner, std::make_pair(cast_info_movie_id, movie_keyword_movie_id), PredicateCondition::Equals,
          JoinNode::make(JoinMode::Inner, std::make_pair(cast_info_person_id, name_id), PredicateCondition::Equals,
            stored_table_nodes["cast_info"],
            PredicateNode::make(name_name, PredicateCondition::Like, "B%",
              stored_table_nodes["name"])),
          stored_table_nodes["movie_keyword"]),
        PredicateNode::make(keyword_keyword, PredicateCondition::Equals, "character-name-in-title",
          stored_table_nodes["keyword"])),
      stored_table_nodes["movie_companies"]));

  lqp->print();

  std::cout << "Row count: " << lqp->get_statistics()->row_count() << std::endl;
  std::cout << "Row count: " << lqp->left_input()->get_statistics()->row_count() << std::endl;

  return 0;
}

