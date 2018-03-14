#include "jit_table_generator.hpp"

namespace opossum {

JitTableGenerator::JitTableGenerator(const float scale_factor, const opossum::ChunkOffset chunk_size)
    : benchmark_utilities::AbstractBenchmarkTableGenerator(chunk_size), _scale_factor{scale_factor} {}

void JitTableGenerator::generate_and_store() {
  benchmark_utilities::RandomGenerator generator;
  auto cardinalities =
      std::make_shared<std::vector<size_t>>(std::initializer_list<size_t>{static_cast<size_t>(_scale_factor * 1000000)});

  auto table_scan = std::make_shared<opossum::Table>();
  add_column<int32_t>(table_scan, "ID", cardinalities, [&](std::vector<size_t> indices) { return indices[0]; });
  add_column<float>(table_scan, "A", cardinalities,
                      [&](std::vector<size_t> indices) { return generator.random_number(0, 100000); });
  add_column<float>(table_scan, "B", cardinalities,
                      [&](std::vector<size_t> indices) { return generator.random_number(0, 100000); });
  add_column<int32_t>(table_scan, "C", cardinalities,
                      [&](std::vector<size_t> indices) { return generator.random_number(0, 100000); });
  add_column<int32_t>(table_scan, "D", cardinalities,
                      [&](std::vector<size_t> indices) { return generator.random_number(0, 100000); });
  add_nullable_column<int32_t>(table_scan, "E", cardinalities,
                               [&](std::vector<size_t> indices) { return generator.random_number(0, 100000); }, [&](std::vector<size_t> indices) { return false; });
  add_nullable_column<int32_t>(table_scan, "F", cardinalities,
                      [&](std::vector<size_t> indices) { return generator.random_number(0, 100000); }, [&](std::vector<size_t> indices) { return false; });
  add_nullable_column<int32_t>(table_scan, "G", cardinalities,
                               [&](std::vector<size_t> indices) { return generator.random_number(0, 100000); }, [&](std::vector<size_t> indices) { return false; });
 add_nullable_column<int32_t>(table_scan, "H", cardinalities,
                               [&](std::vector<size_t> indices) { return generator.random_number(0, 100000); }, [&](std::vector<size_t> indices) { return false; });
  auto table_aggregate = std::make_shared<opossum::Table>();
  add_column<int32_t>(table_aggregate, "ID", cardinalities, [&](std::vector<size_t> indices) { return indices[0]; });
  add_column<int32_t>(table_aggregate, "A", cardinalities,
                      [&](std::vector<size_t> indices) { return generator.random_number(0, 10); });
  add_column<int32_t>(table_aggregate, "B", cardinalities,
                      [&](std::vector<size_t> indices) { return generator.random_number(0, 10); });
  add_column<int32_t>(table_aggregate, "C", cardinalities,
                      [&](std::vector<size_t> indices) { return generator.random_number(0, 10); });
  add_column<int32_t>(table_aggregate, "D", cardinalities,
                      [&](std::vector<size_t> indices) { return generator.random_number(0, 10); });

  StorageManager::get().add_table("TABLE_SCAN", table_scan);
  StorageManager::get().add_table("TABLE_AGGREGATE", table_aggregate);
}

}  // namespace opossum
