#pragma once

#include <benchmark_utilities/abstract_benchmark_table_generator.hpp>
#include <benchmark_utilities/random_generator.hpp>
#include <storage/storage_manager.hpp>

namespace opossum {

class JitTableGenerator : public benchmark_utilities::AbstractBenchmarkTableGenerator {
 public:
  JitTableGenerator(float scale_factor, uint32_t chunk_size = Chunk::MAX_SIZE);

  void generate_and_store();

 private:
  const float _scale_factor;
};

} // namespace opossum
