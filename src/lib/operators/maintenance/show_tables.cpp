#include "show_tables.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "tbb/concurrent_vector.h"

#include "storage/chunk.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"

namespace opossum {

const std::string ShowTables::name() const { return "ShowTables"; }

std::shared_ptr<AbstractOperator> ShowTables::recreate(const std::vector<AllParameterVariant>& args) const {
  return std::make_shared<ShowTables>();
}

std::shared_ptr<const Table> ShowTables::_on_execute() {
  auto table = std::make_shared<Table>(TableColumnDefinitions{{"table_name", DataType::String}}, TableType::Data);

  const auto table_names = StorageManager::get().table_names();
  const auto column = std::make_shared<ValueColumn<std::string>>(
      tbb::concurrent_vector<std::string>(table_names.begin(), table_names.end()));

  ChunkColumnList columns;
  columns.emplace_back(column);
  table->add_chunk_new(columns);

  return table;
}

}  // namespace opossum
