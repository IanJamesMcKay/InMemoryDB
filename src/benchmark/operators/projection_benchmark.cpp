#include <memory>
#include <vector>

#include "benchmark/benchmark.h"

#include "../benchmark_basic_fixture.hpp"
#include "../table_generator.hpp"
#include "operators/pqp_expression.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"

namespace opossum {

enum class InputTableType {
  Data, Encoded, References
};

class OperatorsProjectionBenchmark : public benchmark::Fixture {
 public:
  void SetUp(::benchmark::State& state) override {
    const auto chunk_size = static_cast<ChunkOffset>(state.range(0));
    const auto input_table_type = static_cast<InputTableType>(state.range(1));
    const auto num_rows = static_cast<size_t>(state.range(2));
    
    TableGenerator table_generator{5, num_rows, 3'000};
    
    std::shared_ptr<const Table> input_table;
    switch (input_table_type) {
      case InputTableType::Data:
        input_table = table_generator.generate_table(chunk_size);
        break;
        
      case InputTableType::References: {
        const auto referenced_table = table_generator.generate_table(chunk_size);
        const auto table_wrapper = std::make_shared<TableWrapper>(referenced_table);
        table_wrapper->execute();
        
        const auto table_scan = std::make_shared<TableScan>(table_wrapper, ColumnID{0} /* "a" */,
                                                 PredicateCondition::GreaterThanEquals, 0);  // all
        table_scan->execute();      
        input_table = table_scan->get_output();
      } break;
        
      case InputTableType::Encoded: 
        input_table = table_generator.generate_table(chunk_size, EncodingType::Dictionary);
        break;     
    }

    _table_wrapper = std::make_shared<TableWrapper>(input_table);
    _table_wrapper->execute();
  }

 protected:
  std::shared_ptr<TableWrapper> _table_wrapper;
};

BENCHMARK_DEFINE_F(OperatorsProjectionBenchmark, BM_ProjectionSimple)(benchmark::State& state) {
  BenchmarkBasicFixture::clear_cache();

  Projection::ColumnExpressions expressions = {PQPExpression::create_column(ColumnID{0} /* "a" */)};

  auto warm_up = std::make_shared<Projection>(_table_wrapper, expressions);
  warm_up->execute();

  while (state.KeepRunning()) {
    auto projection = std::make_shared<Projection>(_table_wrapper, expressions);
    projection->execute();
  }
}

BENCHMARK_DEFINE_F(OperatorsProjectionBenchmark, BM_ProjectionVariableTerm)(benchmark::State& state) {
  BenchmarkBasicFixture::clear_cache();

  // "a" + "b"
  Projection::ColumnExpressions expressions = {PQPExpression::create_binary_operator(
      ExpressionType::Addition, PQPExpression::create_column(ColumnID{0}), PQPExpression::create_column(ColumnID{1}))};
  auto warm_up = std::make_shared<Projection>(_table_wrapper, expressions);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto projection = std::make_shared<Projection>(_table_wrapper, expressions);
    projection->execute();
  }
}

BENCHMARK_DEFINE_F(OperatorsProjectionBenchmark, BM_ProjectionConstantTerm)(benchmark::State& state) {
  BenchmarkBasicFixture::clear_cache();

  // "a" + 5
  Projection::ColumnExpressions expressions = {PQPExpression::create_binary_operator(
      ExpressionType::Addition, PQPExpression::create_column(ColumnID{0}), PQPExpression::create_literal(5))};
  auto warm_up = std::make_shared<Projection>(_table_wrapper, expressions);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto projection = std::make_shared<Projection>(_table_wrapper, expressions);
    projection->execute();
  }
}

static void CustomArguments(benchmark::internal::Benchmark* b) {
  b->Args({1, static_cast<int>(InputTableType::Data), 10});
  b->Args({1, static_cast<int>(InputTableType::References), 10});
  b->Args({1, static_cast<int>(InputTableType::Encoded), 10});

//  for (ChunkOffset chunk_size : {ChunkOffset(10000), ChunkOffset(100000), Chunk::MAX_SIZE}) {
//    for (int num_rows : {10, 1'000, 100'000}) {
//      for (int column_type = 0; column_type <= 2; column_type++) {
//        b->Args({static_cast<int>(chunk_size), column_type, num_rows});
//      }
//    }
//  }
}

BENCHMARK_REGISTER_F(OperatorsProjectionBenchmark, BM_ProjectionSimple)->Apply(CustomArguments);

BENCHMARK_REGISTER_F(OperatorsProjectionBenchmark, BM_ProjectionVariableTerm)->Apply(CustomArguments);

BENCHMARK_REGISTER_F(OperatorsProjectionBenchmark, BM_ProjectionConstantTerm)->Apply(CustomArguments);

}  // namespace opossum
