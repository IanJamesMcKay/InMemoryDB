#include "jit_utils.hpp"

#include "jit_operations.hpp"

namespace opossum {



namespace no_inline {

Mvcc unpack_mvcc(const MvccColumns& columns, ChunkOffset chunk_offset) {
  return {columns.tids[chunk_offset].load(), columns.begin_cids[chunk_offset], columns.end_cids[chunk_offset]};
}

template <>
void jit_compute_str<StringFunction>(const StringFunction& op_func, const JitTupleValue& lhs, const JitTupleValue& rhs, const JitTupleValue& result,
                     JitRuntimeContext& context) {

  // This lambda calls the op_func (a lambda that performs the actual computation) with typed arguments and stores
  // the result.
  const auto store_result_wrapper = [&](const auto& typed_lhs, const auto& typed_rhs,
                                        auto& result) -> decltype(op_func(typed_lhs, typed_rhs), void()) {
    using ResultType = decltype(op_func(typed_lhs, typed_rhs));
    result.template set<ResultType>(op_func(typed_lhs, typed_rhs), context);
  };

  const auto catching_func = InvalidTypeCatcher<decltype(store_result_wrapper), void>(store_result_wrapper);

  DebugAssert(lhs.data_type() == DataType::String, "Left hand side is not a string.")
  DebugAssert(rhs.data_type() == DataType::String, "Right hand side is not a string.")
  catching_func(lhs.get<std::string>(context), rhs.get<std::string>(context), result);
}

}  // namespace no_inline

}  // namespace opossum
