#pragma once

#include "jit_abstract_sink.hpp"

namespace opossum {



class JitAggregate : public JitAbstractSink {
 public:
  std::string description() const final;

  void before_query(Table& out_table, JitRuntimeContext& ctx) final;
//  void after_query(Table& out_table, JitRuntimeContext& ctx) final;

  void add_aggregate_column(const std::string& column_name, const JitTupleValue& tuple_value, const AggregateFunction function);
  void add_groupby_column(const std::string& column_name, const JitTupleValue& tuple_value);

 private:
  void next(JitRuntimeContext& ctx) const final;

  uint64_t _compute_hash(JitRuntimeContext& ctx) const;
  bool _equals(JitRuntimeContext& ctx, const uint64_t index) const;

  std::vector<std::tuple<std::string, JitTupleValue, AggregateFunction>> _aggregate_columns;
  std::vector<std::pair<std::string, JitTupleValue>> _groupby_columns;
};

}  // namespace opossum
