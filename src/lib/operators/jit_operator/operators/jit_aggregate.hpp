#pragma once

#include "jit_abstract_sink.hpp"

namespace opossum {

struct JitAggregateColumn {
  std::string column_name;
  JitTupleValue tuple_value;
  JitHashmapValue hashmap_value;
  AggregateFunction function;
};

struct JitGroupByColumn {
  std::string column_name;
  JitTupleValue tuple_value;
  JitHashmapValue hashmap_value;
};

class JitAggregate : public JitAbstractSink {
 public:
  std::string description() const final;

  void before_query(Table& out_table, JitRuntimeContext& ctx) final;
  //  void after_query(Table& out_table, JitRuntimeContext& ctx) final;

  void add_aggregate_column(const std::string& column_name, const JitTupleValue& tuple_value,
                            const AggregateFunction function);
  void add_groupby_column(const std::string& column_name, const JitTupleValue& tuple_value);

 private:
  void _consume(JitRuntimeContext& ctx) const final;

  //uint64_t _compute_hash(JitRuntimeContext& ctx) const;
  //bool _equals(JitRuntimeContext& ctx, const uint64_t index) const;
  //void _assign(const JitMaterializedValue& in, JitMaterializedValue& out) const;
  //bool _equals_one(const JitMaterializedValue& lhs, const JitMaterializedValue& rhs) const;

  uint32_t _num_hashmap_values{0};
  std::vector<JitAggregateColumn> _aggregate_columns;
  std::vector<JitGroupByColumn> _groupby_columns;
};

}  // namespace opossum
