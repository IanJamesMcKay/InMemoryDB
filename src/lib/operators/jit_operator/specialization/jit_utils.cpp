#include "jit_utils.hpp"


namespace opossum {

namespace no_inline {

Mvcc unpack_mvcc(const MvccColumns &columns, ChunkOffset chunk_offset) {
  return {
    columns.tids[chunk_offset].load(),
    columns.begin_cids[chunk_offset],
    columns.end_cids[chunk_offset]
  };
}

}  // namespace no_inline

}  // namespace opossum
