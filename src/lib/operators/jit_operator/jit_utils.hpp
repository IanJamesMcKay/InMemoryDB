#pragma once

#include "types.hpp"
#include "operators/jit_operator/jit_types.hpp"
#include "all_type_variant.hpp"
#include "storage/mvcc_columns.hpp"

namespace opossum {

struct Mvcc {
  TransactionID row_tid;
  CommitID begin_cid;
  CommitID end_cid;
};

using StringFunction = std::function<void(const std::string& a, const std::string& b)>;

namespace no_inline {

__attribute__((noinline))
Mvcc unpack_mvcc(const MvccColumns& columns, ChunkOffset chunk_offset);

__attribute__((noinline))
void compute_binary(const JitTupleValue& lhs, const ExpressionType expression_type,
                    const JitTupleValue& rhs,  const JitTupleValue& result,
                    JitRuntimeContext& context);

}  // namespace no_inline

}  // namespace opossum
