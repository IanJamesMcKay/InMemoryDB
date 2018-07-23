#include "jit_validate.hpp"

#include "operators/jit_operator/jit_types.hpp"
#include "operators/jit_operator/jit_utils.hpp"
#include "operators/validate.hpp"

namespace opossum {

namespace {

bool jit_is_row_visible(CommitID our_tid, CommitID snapshot_commit_id, ChunkOffset chunk_offset,
                        const MvccColumns& columns) {
  Mvcc mvcc = no_inline::unpack_mvcc(columns, chunk_offset);
  return Validate::is_row_visible(our_tid, snapshot_commit_id, mvcc.row_tid, mvcc.begin_cid, mvcc.end_cid);
}

}  // namespace

template <bool use_ref_pos_list>
JitValidate<use_ref_pos_list>::JitValidate() {
  if constexpr (use_ref_pos_list) PerformanceWarning("Jit Validate is used with reference table as input.");
}

template <bool use_ref_pos_list>
std::string JitValidate<use_ref_pos_list>::description() const {
  return "[Validate]";
}

template <bool use_ref_pos_list>
void JitValidate<use_ref_pos_list>::_consume(JitRuntimeContext& context) const {
  bool row_is_visible;
  if constexpr (use_ref_pos_list) {
    const auto row_id = (*context.pos_list)[context.chunk_offset];
    const auto& referenced_chunk = context.referenced_table->get_chunk(row_id.chunk_id);
    const auto& mvcc_columns = referenced_chunk->mvcc_columns();
    row_is_visible =
        jit_is_row_visible(context.transaction_id, context.snapshot_commit_id, row_id.chunk_offset, *mvcc_columns);
  } else {
    row_is_visible =
        jit_is_row_visible(context.transaction_id, context.snapshot_commit_id, context.chunk_offset, *context.columns);
  }
  if (row_is_visible) _emit(context);
}

template class JitValidate<true>;
template class JitValidate<false>;

}  // namespace opossum
