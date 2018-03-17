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
  add_column<int32_t>(table_scan, "A", cardinalities,
                      [&](std::vector<size_t> indices) { return generator.random_number(0, 100000); });
  add_column<int32_t>(table_scan, "B", cardinalities,
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

  add_column<int32_t>(table_scan, "X0", cardinalities,
                      [&](std::vector<size_t> indices) { return 0; });
  add_column<int32_t>(table_scan, "X5000", cardinalities,
                      [&](std::vector<size_t> indices) { return 5000; });
  add_column<int32_t>(table_scan, "X10000", cardinalities,
                      [&](std::vector<size_t> indices) { return 10000; });
  add_column<int32_t>(table_scan, "X15000", cardinalities,
                      [&](std::vector<size_t> indices) { return 15000; });
  add_column<int32_t>(table_scan, "X20000", cardinalities,
                      [&](std::vector<size_t> indices) { return 20000; });
  add_column<int32_t>(table_scan, "X25000", cardinalities,
                      [&](std::vector<size_t> indices) { return 25000; });
  add_column<int32_t>(table_scan, "X30000", cardinalities,
                      [&](std::vector<size_t> indices) { return 30000; });
  add_column<int32_t>(table_scan, "X35000", cardinalities,
                      [&](std::vector<size_t> indices) { return 35000; });
  add_column<int32_t>(table_scan, "X40000", cardinalities,
                      [&](std::vector<size_t> indices) { return 40000; });
  add_column<int32_t>(table_scan, "X45000", cardinalities,
                      [&](std::vector<size_t> indices) { return 45000; });
  add_column<int32_t>(table_scan, "X50000", cardinalities,
                      [&](std::vector<size_t> indices) { return 50000; });
  add_column<int32_t>(table_scan, "X55000", cardinalities,
                      [&](std::vector<size_t> indices) { return 55000; });
  add_column<int32_t>(table_scan, "X60000", cardinalities,
                      [&](std::vector<size_t> indices) { return 60000; });
  add_column<int32_t>(table_scan, "X65000", cardinalities,
                      [&](std::vector<size_t> indices) { return 65000; });
  add_column<int32_t>(table_scan, "X70000", cardinalities,
                      [&](std::vector<size_t> indices) { return 70000; });
  add_column<int32_t>(table_scan, "X75000", cardinalities,
                      [&](std::vector<size_t> indices) { return 75000; });
  add_column<int32_t>(table_scan, "X80000", cardinalities,
                      [&](std::vector<size_t> indices) { return 80000; });
  add_column<int32_t>(table_scan, "X85000", cardinalities,
                      [&](std::vector<size_t> indices) { return 85000; });
  add_column<int32_t>(table_scan, "X90000", cardinalities,
                      [&](std::vector<size_t> indices) { return 90000; });
  add_column<int32_t>(table_scan, "X95000", cardinalities,
                      [&](std::vector<size_t> indices) { return 95000; });
  add_column<int32_t>(table_scan, "X100000", cardinalities,
                      [&](std::vector<size_t> indices) { return 100000; });

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
  add_column<int32_t>(table_aggregate, "E", cardinalities,
                      [&](std::vector<size_t> indices) { return generator.random_number(0, 10); });
  add_column<int32_t>(table_aggregate, "F", cardinalities,
                      [&](std::vector<size_t> indices) { return generator.random_number(0, 10); });
  add_column<int32_t>(table_aggregate, "X1", cardinalities,
                      [&](std::vector<size_t> indices) { return generator.random_number(0, 0); });
  add_column<int32_t>(table_aggregate, "X10", cardinalities,
                      [&](std::vector<size_t> indices) { return generator.random_number(0, 9); });
  add_column<int32_t>(table_aggregate, "X100", cardinalities,
                      [&](std::vector<size_t> indices) { return generator.random_number(0, 99); });
  add_column<int32_t>(table_aggregate, "X1000", cardinalities,
                      [&](std::vector<size_t> indices) { return generator.random_number(0, 999); });
  add_column<int32_t>(table_aggregate, "X10000", cardinalities,
                      [&](std::vector<size_t> indices) { return generator.random_number(0, 9999); });
  add_column<int32_t>(table_aggregate, "X100000", cardinalities,
                      [&](std::vector<size_t> indices) { return generator.random_number(0, 99999); });

  auto table_types = std::make_shared<opossum::Table>();
  add_column<int32_t>(table_types, "I1", cardinalities,
                      [&](std::vector<size_t> indices) { return generator.random_number(0, 10); });
  add_column<int32_t>(table_types, "I2", cardinalities,
                      [&](std::vector<size_t> indices) { return generator.random_number(0, 10); });
  add_column<int64_t>(table_types, "L1", cardinalities,
                      [&](std::vector<size_t> indices) { return generator.random_number(0, 10); });
  add_column<int64_t>(table_types, "L2", cardinalities,
                      [&](std::vector<size_t> indices) { return generator.random_number(0, 10); });
  add_column<float>(table_types, "F1", cardinalities,
                      [&](std::vector<size_t> indices) { return generator.random_number(0, 10); });
  add_column<float>(table_types, "F2", cardinalities,
                      [&](std::vector<size_t> indices) { return generator.random_number(0, 10); });
  add_column<double>(table_types, "D1", cardinalities,
                      [&](std::vector<size_t> indices) { return generator.random_number(0, 10); });
  add_column<double>(table_types, "D2", cardinalities,
                      [&](std::vector<size_t> indices) { return generator.random_number(0, 10); });
  add_column<std::string>(table_types, "S1", cardinalities,
                      [&](std::vector<size_t> indices) { return std::string(1, static_cast<char>(generator.random_number(65, 75))); });
  add_column<std::string>(table_types, "S2", cardinalities,
                      [&](std::vector<size_t> indices) { return std::string(1, static_cast<char>(generator.random_number(65, 75))); });

  StorageManager::get().add_table("TABLE_SCAN", table_scan);
  StorageManager::get().add_table("TABLE_AGGREGATE", table_aggregate);
  StorageManager::get().add_table("TABLE_TYPES", table_types);
}

}  // namespace opossum
