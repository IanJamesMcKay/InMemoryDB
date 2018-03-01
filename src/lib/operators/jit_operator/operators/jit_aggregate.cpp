#include "jit_aggregate.hpp"

#include "constant_mappings.hpp"
#include "operators/jit_operator/jit_operations.hpp"
#include "resolve_type.hpp"
#include "storage/value_column.hpp"

namespace opossum {

std::string JitAggregate::description() const {
  std::stringstream desc;
  desc << "[Aggregate] GroupBy: ";
  for (const auto& groupby_column : _groupby_columns) {
    desc << groupby_column.column_name << " = x" << groupby_column.tuple_value.tuple_index() << ", ";
  }
  desc << " Aggregates: ";
  for (const auto& aggregate_column : _aggregate_columns) {
    desc << aggregate_column.column_name << " = " << aggregate_function_to_string.left.at(aggregate_column.function)
         << "(x" << aggregate_column.tuple_value.tuple_index() << "), ";
  }
  return desc.str();
}

void JitAggregate::before_query(Table& out_table, JitRuntimeContext& context) const {
  context.hashmap.values.resize(_num_hashmap_values);
}

void JitAggregate::after_query(Table& out_table, JitRuntimeContext& context) const {
  const auto out_chunk = std::make_shared<Chunk>();

  for (const auto& groupby_column : _groupby_columns) {
    const auto data_type = groupby_column.tuple_value.data_type();
    const auto is_nullable = groupby_column.tuple_value.is_nullable();
    out_table.add_column_definition(groupby_column.column_name, data_type, is_nullable);

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      auto column = std::make_shared<ValueColumn<ColumnDataType>>(is_nullable);
      out_chunk->add_column(column);
    });
  }

  for (const auto& aggregate_column : _aggregate_columns) {
    const auto data_type = aggregate_column.tuple_value.data_type();
    const auto is_nullable = aggregate_column.tuple_value.is_nullable();
    out_table.add_column_definition(aggregate_column.column_name, data_type, is_nullable);

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      auto column = std::make_shared<ValueColumn<ColumnDataType>>(is_nullable);
      out_chunk->add_column(column);
    });
  }

  out_table.emplace_chunk(out_chunk);
}

void JitAggregate::add_aggregate_column(const std::string& column_name, const JitTupleValue& value,
                                        const AggregateFunction function) {
  _aggregate_columns.push_back(JitAggregateColumn{column_name, value, JitHashmapValue(value, _num_hashmap_values++), function});
}

void JitAggregate::add_groupby_column(const std::string& column_name, const JitTupleValue& value) {
  _groupby_columns.push_back(JitGroupByColumn{column_name, value, JitHashmapValue(value, _num_hashmap_values++)});
}

void JitAggregate::_consume(JitRuntimeContext& ctx) const {}
/*  const auto hash = _compute_hash(ctx);
  auto &bucket = ctx.hashmap.map[hash];

  auto value_index{-1};
  for (const auto &index : bucket) {
    if (_equals(ctx, index)) {
      value_index = index;
      break;
    }
  }

  if (value_index == -1) {
    for (auto &groupby_column : _groupby_columns) {
      value_index = groupby_column.hashmap_value.grow(ctx);
      //auto value = groupby_column.hashmap_value.materialize(ctx, value_index);
      // _assign(groupby_column.tuple_value.materialize(ctx), value);
    }
    for (auto &aggregate_column : _aggregate_columns) {
      aggregate_column.hashmap_value.grow(ctx);
    }
    bucket.push_back(value_index);
  }

//  for (auto &aggregate_column : _aggregate_columns) {
    //auto result = aggregate_column.hashmap_value.materialize(ctx, value_index);
    // jit_compute(jit_addition, aggregate_column.tuple_value.materialize(ctx), result, result);
  //}

}

bool JitAggregate::_equals(JitRuntimeContext& ctx, const uint64_t index) const {
  //for (auto& groupby_column : _groupby_columns) {
    //if (!_equals_one(groupby_column.tuple_value.materialize(ctx), groupby_column.hashmap_value.materialize(ctx, index))) return false;
  //}
  return false;
}

bool JitAggregate::_equals_one(const JitMaterializedValue& lhs, const JitMaterializedValue& rhs) const {
  switch (lhs.data_type()) {
    case DataType::Bool:
      return lhs.get<bool>() == rhs.get<bool>();
    case DataType::Int:
      return lhs.get<int32_t>() == rhs.get<int32_t>();
    case DataType::Long:
      return lhs.get<int64_t>() == rhs.get<int64_t>();
    case DataType::Float:
      return lhs.get<float>() == rhs.get<float>();
    case DataType::Double:
      return lhs.get<double>() == rhs.get<double>();
    case DataType::String:
      return lhs.get<std::string>() == rhs.get<std::string>();
    case DataType::Null:
      return false;
  }
}

uint64_t JitAggregate::_compute_hash(JitRuntimeContext& ctx) const {
  uint64_t hash{0};
  for (const auto& groupby_column : _groupby_columns) {
    // auto const value = groupby_column.tuple_value.materialize(ctx);
    if (!value.is_null()) {
      switch (value.data_type()) {
        case DataType::Bool:
          hash ^= std::hash<bool>()(value.get<bool>()) << 1;
          break;
        case DataType::Int:
          hash ^= std::hash<int32_t>()(value.get<int32_t>()) << 1;
          break;
        case DataType::Long:
          hash ^= std::hash<int64_t>()(value.get<int64_t>()) << 1;
          break;
        case DataType::Float:
          hash ^= std::hash<float>()(value.get<float>()) << 1;
          break;
        case DataType::Double:
          hash ^= std::hash<double>()(value.get<double>()) << 1;
          break;
        case DataType::String:
          hash ^= std::hash<std::string>()(value.get<std::string>()) << 1;
          break;
        case DataType::Null:
          break;
      }
    }
  }
  return hash;
}

void JitAggregate::_assign(const JitMaterializedValue& in, JitMaterializedValue& out) const {
  switch (in.data_type()) {
    case DataType::Bool:
      out.set<bool>(in.get<bool>());
      break;
    case DataType::Int:
      out.set<int32_t>(in.get<int32_t>());
      break;
    case DataType::Long:
      out.set<int64_t>(in.get<int64_t>());
      break;
    case DataType::Float:
      out.set<float>(in.get<float>());
      break;
    case DataType::Double:
      out.set<double>(in.get<double>());
      break;
    case DataType::String:
      out.set<std::string>(in.get<std::string>());
      break;
    case DataType::Null:
      break;
  }
}
*/
}  // namespace opossum
