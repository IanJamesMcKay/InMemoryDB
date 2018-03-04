#pragma once

#include <memory>
#include <optional>

#include "storage/encoding_type.hpp"
#include "types.hpp"

namespace opossum {

class Table;

struct TableGenerator2ColumnDefinition {
  TableGenerator2ColumnDefinition(DataType data_type, AllTypeVariant min_value, AllTypeVariant max_value):
    data_type(data_type), min_value(min_value), max_value(max_value)
  {
  }

  DataType data_type;
  AllTypeVariant min_value;
  AllTypeVariant max_value;
};

using TableGenerator2ColumnDefinitions = std::vector<TableGenerator2ColumnDefinition>;

class TableGenerator2 {
 public:
  TableGenerator2(const TableGenerator2ColumnDefinitions& column_definitions, const size_t num_rows_per_chunk, const size_t chunk_count);

  std::shared_ptr<Table> generate_table() const;

 protected:
  TableGenerator2ColumnDefinitions _column_definitions;
  size_t _num_rows_per_chunk;
  size_t _chunk_count;
};

}  // namespace opossum
