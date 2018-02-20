#pragma once

#include "jit_abstract_operator.hpp"
#include "storage/chunk.hpp"
#include "storage/table.hpp"

namespace opossum {

/* Base class for all column readers.
 * We need this class, so we can store a number of JitColumnReaders with different template
 * specializations in a common data structure.
 */
class BaseJitColumnReader {
 public:
  using Ptr = std::shared_ptr<const BaseJitColumnReader>;

  virtual void read_value(JitRuntimeContext& ctx) const = 0;
  virtual void increment(JitRuntimeContext& ctx) const = 0;
};

/* JitColumnReaders wrap the column iterable interface used by most operators and makes it accessible
 * to the JitOperator.
 *
 * Why we need this wrapper:
 * Most operators access data by creating a fixed number (usually one or two) of column iterables and
 * then immediately uses those iterators in a lambda. The JitOperator, on the other hand, processes
 * data in a tuple-at-a-time fashion and thus needs access to an arbitrary number of column iterators
 * at the same time.
 *
 * We solve this problem by introducing a template-free super class to all column iterators. This allows us to
 * create an iterator for each input column (before processing each chunk) and store these iterators in a
 * common vector in the runtime context.
 * We then use JitColumnReader instances to access these iterators. JitColumnReaders are templated with the
 * type of iterator they are supposed to handle. They are initialized with an input_index and a tuple value.
 * When requested to read a value, they will access the iterator from the runtime context corresponding to their
 * input_index and copy the value to their JitTupleValue.
 *
 * All column readers have a common template-free base class. That allows us to store the column readers in a
 * vector as well and access all types of columns with a single interface.
 */
template <typename Iterator, typename DataType, bool Nullable>
class JitColumnReader : public BaseJitColumnReader {
 public:
  JitColumnReader(const size_t input_index, const JitTupleValue& tuple_value)
      : _input_index{input_index}, _tuple_value{tuple_value} {}

  void read_value(JitRuntimeContext& ctx) const {
    const auto& value = *_iterator(ctx);
    // clang-format off
    if constexpr (Nullable) {
      _tuple_value.materialize(ctx).set_is_null(value.is_null());
      if (!value.is_null()) {
        _tuple_value.materialize(ctx).template set<DataType>(value.value());
      }
    } else {
      _tuple_value.materialize(ctx).template set<DataType>(value.value());
    }
    // clang-format on
  }

  void increment(JitRuntimeContext& ctx) const final { ++_iterator(ctx); }

 private:
  Iterator& _iterator(JitRuntimeContext& ctx) const {
    return *std::static_pointer_cast<Iterator>(ctx.inputs[_input_index]);
  }

  const size_t _input_index;
  const JitTupleValue _tuple_value;
};

struct JitInputColumn {
  ColumnID column_id;
  JitTupleValue tuple_value;
};

struct JitInputLiteral {
  AllTypeVariant value;
  JitTupleValue tuple_value;
};

/* JitReadTuple must be the first operator in any chain of jit operators.
 * It is responsible for
 * 1) storing literal values to the runtime tuple before the query is executed
 * 2) reading data from the the input table to the runtime tuple
 * 3) advancing the column iterators
 * 4) keeping track of the number of values in the runtime tuple. Whenever
 *    another operator needs to store a temporary value in the runtime tuple,
 *    it can request a slot in the tuple from JitReadTuple.
 */
class JitReadTuple : public JitAbstractOperator {
 public:
  using Ptr = std::shared_ptr<JitReadTuple>;

  std::string description() const final;

  void before_query(const Table& in_table, JitRuntimeContext& ctx);
  void before_chunk(const Table& in_table, const Chunk& in_chunk, JitRuntimeContext& ctx) const;

  JitTupleValue add_input_column(const Table& table, const ColumnID column_id);
  JitTupleValue add_literal_value(const AllTypeVariant& value);
  size_t add_temorary_value();

  void execute(JitRuntimeContext& ctx) const;

 protected:
  uint32_t _num_tuple_values{0};
  std::vector<JitInputColumn> _input_columns;
  std::vector<JitInputLiteral> _input_literals;
  std::vector<BaseJitColumnReader::Ptr> _column_readers;

 private:
  void next(JitRuntimeContext& ctx) const final {}
};

}  // namespace opossum
