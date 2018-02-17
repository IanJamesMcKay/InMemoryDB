#include "jit_aggregate.hpp"

#include "constant_mappings.hpp"
#include "resolve_type.hpp"
#include "storage/value_column.hpp"
#include "operators/jit_operator/jit_operations.hpp"

namespace opossum {

std::string JitAggregate::description() const {
  std::stringstream desc;
  desc << "[Aggregate] GroupBy: ";
  for (const auto& groupby_column : _groupby_columns) {
    desc << groupby_column.first << " = x" << groupby_column.second.tuple_index() << ", ";
  }
  desc << " Aggregates: ";
  for (const auto& aggregate_column : _aggregate_columns) {
    desc << std::get<0>(aggregate_column) << " = " << aggregate_function_to_string.left.at(std::get<2>(aggregate_column)) << "(x" << std::get<1>(aggregate_column).tuple_index() << "), ";
  }
  return desc.str();
}

void JitAggregate::before_query(Table& out_table, JitRuntimeContext& ctx) {
  ctx.out_chunk = std::make_shared<Chunk>();

  for (const auto& groupby_column : _groupby_columns) {
    const auto data_type = groupby_column.second.data_type();
    const auto is_nullable = groupby_column.second.is_nullable();
    out_table.add_column_definition(groupby_column.first, data_type, is_nullable);

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      auto column = std::make_shared<ValueColumn<ColumnDataType>>(is_nullable);
      ctx.out_chunk->add_column(column);
    });
  }

  for (const auto& aggregate_column : _aggregate_columns) {
    const auto data_type = std::get<1>(aggregate_column).data_type();
    const auto is_nullable = std::get<1>(aggregate_column).is_nullable();
    out_table.add_column_definition(std::get<0>(aggregate_column), data_type, is_nullable);

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      auto column = std::make_shared<ValueColumn<ColumnDataType>>(is_nullable);
      ctx.out_chunk->add_column(column);
    });
  }

  out_table.emplace_chunk(ctx.out_chunk);

/*    const auto data_type = jit_data_type_to_data_type.at(output_column.second.data_type());

    out_table.add_column_definition(output_column.first, data_type, is_nullable);

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      if (is_nullable) {
        _column_writers.push_back(std::make_shared<JitColumnWriter<ValueColumn<ColumnDataType>, ColumnDataType, true>>(
                _column_writers.size(), output_column.second));
      } else {
        _column_writers.push_back(std::make_shared<JitColumnWriter<ValueColumn<ColumnDataType>, ColumnDataType, false>>(
                _column_writers.size(), output_column.second));
      }
    });
  }*/

  // _create_output_chunk(ctx);
}

void JitAggregate::add_aggregate_column(const std::string& column_name, const JitTupleValue& value, const AggregateFunction function) {
  _aggregate_columns.push_back(std::make_tuple(column_name, value, function));
}

void JitAggregate::add_groupby_column(const std::string& column_name, const JitTupleValue& value) {
  _groupby_columns.push_back(std::make_pair(column_name, value));
}

void JitAggregate::next(JitRuntimeContext& ctx) const {
  const auto hash = _compute_hash(ctx);
  auto bucket = ctx.hashmap.map[hash];

  auto value_index{-1};
  for (const auto& index : bucket) {
    if (_equals(ctx, index)) {
      value_index = index;
      break;
    }
  }

  /*if (value_index = -1) {
    value_index =
  }
*/
}

bool JitAggregate::_equals(JitRuntimeContext& ctx, const uint64_t index) const {
  return false;
}

uint64_t JitAggregate::_compute_hash(JitRuntimeContext& ctx) const {
  uint64_t hash{0};
  for (const auto& groupby_column : _groupby_columns) {
    auto const value = groupby_column.second.materialize(ctx);
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

/*void JitAggreagate::_create_output_chunk(JitRuntimeContext& ctx) const {
  ctx.out_chunk = std::make_shared<Chunk>();
  ctx.outputs.clear();

  for (const auto& output_column : _output_columns) {
    const auto data_type = jit_data_type_to_data_type.at(output_column.second.data_type());
    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      auto column = std::make_shared<ValueColumn<ColumnDataType>>(output_column.second.is_nullable());
      ctx.outputs.push_back(column);
      ctx.out_chunk->add_column(column);
    });
  }
}*/

}  // namespace opossum
