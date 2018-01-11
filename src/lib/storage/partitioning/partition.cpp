#include "storage/partitioning/partition.hpp"
#include "resolve_type.hpp"
#include "storage/reference_column.hpp"

namespace opossum {

Partition::Partition(PartitionID partition_id) : _partition_id(partition_id) {
  _chunks.push_back(Chunk{ChunkUseMvcc::Yes});
}

const PartitionID Partition::get_partition_id() { return _partition_id; }

void Partition::add_column(DataType data_type, bool nullable) {
  for (auto& chunk : _chunks) {
    chunk.add_column(make_shared_by_data_type<BaseColumn, ValueColumn>(data_type, nullable));
  }
}

void Partition::append(std::vector<AllTypeVariant> values, const uint32_t max_chunk_size,
                       const std::vector<DataType>& column_types, const std::vector<bool>& column_nullables) {
  if (_chunks.back().size() == max_chunk_size) {
    create_new_chunk(column_types, column_nullables);
  }

  _chunks.back().append(values);
}

ChunkID Partition::chunk_count() const { return static_cast<ChunkID>(_chunks.size()); }

void Partition::create_new_chunk(const std::vector<DataType>& column_types, const std::vector<bool>& column_nullables) {
  // Create chunk with mvcc columns
  Chunk new_chunk{ChunkUseMvcc::Yes};

  for (auto column_id = 0u; column_id < column_types.size(); ++column_id) {
    const auto type = column_types[column_id];
    auto nullable = column_nullables[column_id];

    new_chunk.add_column(make_shared_by_data_type<BaseColumn, ValueColumn>(type, nullable));
  }
  _chunks.push_back(std::move(new_chunk));
}

TableType Partition::get_type(uint16_t column_count) const {
  Assert(!_chunks.empty() && column_count > 0, "Table has no content, can't specify type");

  // We assume if one column is a reference column, all are.
  const auto column = _chunks[0].get_column(ColumnID{0});
  const auto ref_column = std::dynamic_pointer_cast<const ReferenceColumn>(column);

  if (ref_column != nullptr) {
// In debug mode we're pedantic and check whether all columns in all chunks are Reference Columns
#if IS_DEBUG
    for (auto chunk_idx = ChunkID{0}; chunk_idx < chunk_count(); ++chunk_idx) {
      for (auto column_idx = ColumnID{0}; column_idx < column_count; ++column_idx) {
        const auto column2 = _chunks[chunk_idx].get_column(ColumnID{column_idx});
        const auto ref_column2 = std::dynamic_pointer_cast<const ReferenceColumn>(column);
        DebugAssert(ref_column2 != nullptr, "Invalid table: Contains Reference and Non-Reference Columns");
      }
    }
#endif
    return TableType::References;
  } else {
// In debug mode we're pedantic and check whether all columns in all chunks are Value/Dict Columns
#if IS_DEBUG
    for (auto chunk_idx = ChunkID{0}; chunk_idx < chunk_count(); ++chunk_idx) {
      for (auto column_idx = ColumnID{0}; column_idx < column_count; ++column_idx) {
        const auto column2 = _chunks[chunk_idx].get_column(ColumnID{column_idx});
        const auto ref_column2 = std::dynamic_pointer_cast<const ReferenceColumn>(column);
        DebugAssert(ref_column2 == nullptr, "Invalid table: Contains Reference and Non-Reference Columns");
      }
    }
#endif
    return TableType::Data;
  }
}

AllTypeVariant Partition::get_value(const ColumnID column_id, const size_t row_number) const {
  size_t row_counter = 0u;
  for (auto& chunk : _chunks) {
    size_t current_size = chunk.size();
    row_counter += current_size;
    if (row_counter > row_number) {
      return (*chunk.get_column(column_id))[row_number + current_size - row_counter];
    }
  }
  Fail("Row does not exist.");
  return {};
}

void Partition::emplace_chunk(Chunk& chunk, uint16_t column_count) {
  if (_chunks.size() == 1 && (_chunks.back().column_count() == 0 || _chunks.back().size() == 0)) {
    // the initial chunk was not used yet
    _chunks.clear();
  }
  DebugAssert(chunk.column_count() > 0, "Trying to add chunk without columns.");
  DebugAssert(chunk.column_count() == column_count,
              std::string("adding chunk with ") + std::to_string(chunk.column_count()) + " columns to table with " +
                  std::to_string(column_count) + " columns");
  _chunks.emplace_back(std::move(chunk));
}

Chunk& Partition::get_modifiable_chunk(ChunkID chunk_id) {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  return _chunks[chunk_id];
}

const Chunk& Partition::get_chunk(ChunkID chunk_id) const {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  return _chunks[chunk_id];
}

ProxyChunk Partition::get_modifiable_chunk_with_access_counting(ChunkID chunk_id) {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  return ProxyChunk(_chunks[chunk_id]);
}

const ProxyChunk Partition::get_chunk_with_access_counting(ChunkID chunk_id) const {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  return ProxyChunk(_chunks[chunk_id]);
}

uint64_t Partition::row_count() const {
  uint64_t ret = 0;
  for (const auto& chunk : _chunks) {
    ret += chunk.size();
  }
  return ret;
}

}  // namespace opossum
