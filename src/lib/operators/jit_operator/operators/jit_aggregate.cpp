#include "jit_aggregate.hpp"

#include "constant_mappings.hpp"
#include "operators/jit_operator/jit_operations.hpp"
#include "resolve_type.hpp"
#include "storage/value_column.hpp"

namespace opossum {

std::string JitAggregate::description() const {
  std::stringstream desc;
  desc << "[Aggregate] GroupBy: ";
  for (const auto &groupby_column : _groupby_columns) {
    desc << groupby_column.column_name << " = x" << groupby_column.tuple_value.tuple_index() << ", ";
  }
  desc << " Aggregates: ";
  for (const auto &aggregate_column : _aggregate_columns) {
    desc << aggregate_column.column_name << " = " << aggregate_function_to_string.left.at(aggregate_column.function)
         << "(x" << aggregate_column.tuple_value.tuple_index() << "), ";
  }
  return desc.str();
}

void JitAggregate::before_query(Table &out_table, JitRuntimeContext &context) const {
  context.hashmap.values.resize(_num_hashmap_values);
}

void JitAggregate::after_query(Table &out_table, JitRuntimeContext &context) const {
  const auto out_chunk = std::make_shared<Chunk>();

  for (const auto &groupby_column : _groupby_columns) {
    const auto data_type = groupby_column.hashmap_value.data_type();
    const auto is_nullable = groupby_column.hashmap_value.is_nullable();
    out_table.add_column_definition(groupby_column.column_name, data_type, is_nullable);

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      auto& values = context.hashmap.values[groupby_column.hashmap_value.column_index()].get_vector<ColumnDataType>();
      auto column = std::make_shared<ValueColumn<ColumnDataType>>(values);
      out_chunk->add_column(column);
    });
  }

  for (const auto &aggregate_column : _aggregate_columns) {
    const auto data_type = aggregate_column.hashmap_value.data_type();
    const auto is_nullable = aggregate_column.hashmap_value.is_nullable();
    out_table.add_column_definition(aggregate_column.column_name, data_type, is_nullable);

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      auto& values = context.hashmap.values[aggregate_column.hashmap_value.column_index()].template get_vector<ColumnDataType>();
      auto column = std::make_shared<ValueColumn<ColumnDataType>>(values);
      out_chunk->add_column(column);
    });
  }

  out_table.emplace_chunk(out_chunk);
}

void JitAggregate::add_aggregate_column(const std::string &column_name, const JitTupleValue &value,
                                        const AggregateFunction function) {
  switch (function) {
    case AggregateFunction::Count:
      _aggregate_columns.push_back(JitAggregateColumn{column_name, value, JitHashmapValue(DataType::Long, false, _num_hashmap_values++), function, true});
      break;
    case AggregateFunction::Sum:
    case AggregateFunction::Max:
    case AggregateFunction::Min:
      _aggregate_columns.push_back(JitAggregateColumn{column_name, value, JitHashmapValue(value.data_type(), false, _num_hashmap_values++), function, true});
      break;
    case AggregateFunction::Avg:
      _aggregate_columns.push_back(JitAggregateColumn{column_name, value, JitHashmapValue(DataType::Double, false, _num_hashmap_values++), function, true});
      _aggregate_columns.push_back(JitAggregateColumn{column_name + "_sum", value, JitHashmapValue(value.data_type(), false, _num_hashmap_values++), function, false});
      _aggregate_columns.push_back(JitAggregateColumn{column_name + "_count", value, JitHashmapValue(DataType::Long, false, _num_hashmap_values++), function, false});
      break;
    default:
      Fail("aggregate function not supported");
  }

}

void JitAggregate::add_groupby_column(const std::string &column_name, const JitTupleValue &value) {
  _groupby_columns.push_back(JitGroupByColumn{column_name, value, JitHashmapValue(value.data_type(), value.is_nullable(), _num_hashmap_values++)});
}

void JitAggregate::_consume(JitRuntimeContext& context) const {
  // compute hash value
  uint64_t hash_value = 0;

  const auto size = _groupby_columns.size();

  for (uint32_t i = 0; i < size; ++i) {
    hash_value = (hash_value << 5) ^ jit_hash(_groupby_columns[i].tuple_value, context);
  }

  auto& hash_bucket = context.hashmap.indices[hash_value];

  bool found_match = false;
  uint64_t match_index;
  for (const auto& index : hash_bucket) {
    bool all_values_equal = true;
    for (auto& groupby_column : _groupby_columns) {
      if (!jit_aggregate_equals(groupby_column.tuple_value, groupby_column.hashmap_value, index, context)) {
        all_values_equal = false;
        break;
      }
    }
    if (all_values_equal) {
      found_match = true;
      match_index = index;
      break;
    }
  }

  if (!found_match) {
    //for (auto& groupby_column : _groupby_columns) {
    for (uint32_t i = 0; i < size; ++i) {
      match_index = jit_grow_by_one(_groupby_columns[i].hashmap_value, context);
      jit_assign(_groupby_columns[i].tuple_value, _groupby_columns[i].hashmap_value, match_index, context);
    }
    for (auto& aggregate_column : _aggregate_columns) {
      match_index = jit_grow_by_one(aggregate_column.hashmap_value, context);
    }
    hash_bucket.push_back(match_index);
  }

  for (auto& aggregate_column : _aggregate_columns) {
    switch (aggregate_column.function) {
      case AggregateFunction::Sum:
        jit_aggregate_compute(jit_addition, aggregate_column.tuple_value, aggregate_column.hashmap_value, match_index,
                              context);
        break;
      case AggregateFunction::Count:
        jit_aggregate_compute(jit_increment, aggregate_column.tuple_value, aggregate_column.hashmap_value, match_index,
                              context);
        break;
      case AggregateFunction::Max:
        jit_aggregate_compute(jit_maximum, aggregate_column.tuple_value, aggregate_column.hashmap_value, match_index,
                              context);
        break;
      case AggregateFunction::Min:
        jit_aggregate_compute(jit_minimum, aggregate_column.tuple_value, aggregate_column.hashmap_value, match_index,
                              context);
        break;
      default:
        break;
    }
  }
}

}  // namespace opossum
