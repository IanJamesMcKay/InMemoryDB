#include "table_generator.hpp"

#include <algorithm>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "tbb/concurrent_vector.h"

#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"

#include "types.hpp"

namespace opossum {


TableGenerator::TableGenerator(const size_t num_columns, const size_t num_rows, const size_t max_value):
  _num_columns(num_columns), _num_rows(num_rows), _max_value(max_value) {

}

std::shared_ptr<Table> TableGenerator::generate_table(const ChunkOffset chunk_size,
                                                      std::optional<EncodingType> encoding_type) const {
  std::vector<tbb::concurrent_vector<int>> value_vectors;
  auto vector_size = std::min(static_cast<size_t>(chunk_size), _num_rows);
  /*
   * Generate table layout with column names from 'a' to 'z'.
   * Create a vector for each column.
   */
  TableColumnDefinitions column_definitions;
  for (size_t i = 0; i < _num_columns; i++) {
    auto column_name = std::string(1, static_cast<char>(static_cast<int>('a') + i));
    column_definitions.emplace_back(column_name, DataType::Int);
    value_vectors.emplace_back(tbb::concurrent_vector<int>(vector_size));
  }
  const auto table = std::make_shared<Table>(column_definitions, TableType::Data, chunk_size);

  std::default_random_engine engine;
  std::uniform_int_distribution<int> dist(0, _max_value);
  for (size_t i = 0; i < _num_rows; i++) {
    /*
     * Add vectors to chunk when full, and add chunk to table.
     * Reset vectors and chunk.
     */
    if (i % vector_size == 0 && i > 0) {
      ChunkColumns columns;
      for (size_t j = 0; j < _num_columns; j++) {
        columns.push_back(std::make_shared<ValueColumn<int>>(std::move(value_vectors[j])));
        value_vectors[j] = tbb::concurrent_vector<int>(vector_size);
      }
      table->append_chunk(columns);
    }
    /*
     * Set random value for every column.
     */
    for (size_t j = 0; j < _num_columns; j++) {
      value_vectors[j][i % vector_size] = dist(engine);
    }
  }
  /*
   * Add remaining values to table, if any.
   */
  if (value_vectors[0].size() > 0) {
    ChunkColumns columns;
    for (size_t j = 0; j < _num_columns; j++) {
      columns.push_back(std::make_shared<ValueColumn<int>>(std::move(value_vectors[j])));
    }
    table->append_chunk(columns);
  }

  if (encoding_type.has_value()) {
    ChunkEncoder::encode_all_chunks(table, {encoding_type.value()});
  }

  return table;
}
}  // namespace opossum
