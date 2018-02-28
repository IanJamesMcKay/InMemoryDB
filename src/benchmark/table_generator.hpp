#pragma once

#include <memory>
#include <optional>

#include "storage/encoding_type.hpp"
#include "types.hpp"

namespace opossum {

class Table;

class TableGenerator {
 public:
  TableGenerator(const size_t num_columns = 10, const size_t num_rows = 4'000'000, const size_t max_value = 10'000);

  std::shared_ptr<Table> generate_table(const ChunkOffset chunk_size,
                                        std::optional<EncodingType> encoding_type = std::nullopt);

 protected:
  const size_t _num_columns = 10;
  const size_t _num_rows = 4'000'000;
  const int32_t _max_value = 10'000;
};

}  // namespace opossum
