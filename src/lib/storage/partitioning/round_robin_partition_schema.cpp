#include <storage/partitioning/round_robin_partition_schema.hpp>

namespace opossum {

RoundRobinPartitionSchema::RoundRobinPartitionSchema(size_t number_of_partitions)
    : _number_of_partitions(number_of_partitions) {
  _partitions.reserve(number_of_partitions);

  for (size_t index = 0; index < number_of_partitions; ++index) {
    _partitions.emplace_back(std::make_shared<Partition>(static_cast<PartitionID>(index)));
  }
}

void RoundRobinPartitionSchema::append(std::vector<AllTypeVariant> values, const uint32_t max_chunk_size,
                                       const std::vector<DataType>& column_types,
                                       const std::vector<bool>& column_nullables) {
  std::shared_ptr<Partition> partition_to_append = _partitions.at(_next_partition);
  partition_to_append->append(values, max_chunk_size, column_types, column_nullables);
  if (_next_partition >= static_cast<PartitionID>(_partitions.size())) {
    _next_partition = 0;
  } else {
    _next_partition += 1;
  }
}

const PartitionID RoundRobinPartitionSchema::get_matching_partition_for(AllTypeVariant value) const {
  // TODO(partitioning group/Felix): Implement
  throw "Not implemented";
};


}  // namespace opossum
