
#include "base_column.hpp"

namespace opossum {

BaseColumn::BaseColumn(const DataType data_type) : _data_type(data_type) {}

DataType BaseColumn::data_type() const { return _data_type; }

const AllTypeVariant BaseColumn::get_scrambled_value(const ChunkOffset chunk_offset) const {
  return this->operator[](chunk_offset);
}

}  // namespace opossum
