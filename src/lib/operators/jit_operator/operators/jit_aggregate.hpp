#pragma once

#include "jit_abstract_sink.hpp"

namespace opossum {

struct JitAggregateColumn {
  std::string column_name;
  JitTupleValue tuple_value;
  JitHashmapValue hashmap_value;
  AggregateFunction function;
  bool part_of_output;
};

struct JitGroupByColumn {
  std::string column_name;
  JitTupleValue tuple_value;
  JitHashmapValue hashmap_value;
};

class JitAggregate : public JitAbstractSink {
 public:
  std::string description() const final;

  void before_query(Table& out_table, JitRuntimeContext& context) const final;
  void after_query(Table& out_table, JitRuntimeContext& context) const final;

  void add_aggregate_column(const std::string& column_name, const JitTupleValue& tuple_value,
                            const AggregateFunction function);
  void add_groupby_column(const std::string& column_name, const JitTupleValue& tuple_value);

 private:
  void _consume(JitRuntimeContext& ctx) const final;

  uint32_t _num_hashmap_values{0};
  std::vector<JitAggregateColumn> _aggregate_columns;
  std::vector<JitGroupByColumn> _groupby_columns;
};

}  // namespace opossum
