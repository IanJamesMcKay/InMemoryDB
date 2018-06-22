#pragma once

#include "types.hpp"
#include "storage/mvcc_columns.hpp"

namespace opossum {

struct Mvcc {
  TransactionID row_tid;
  CommitID begin_cid;
  CommitID end_cid;
};

namespace no_inline {

__attribute__((noinline))
Mvcc unpack_mvcc(const MvccColumns &columns, ChunkOffset chunk_offset);

}  // namespace no_inline

}  // namespace opossum
