#include "jit_validate.hpp"

#include "operators/jit_operator/jit_types.hpp"
#include "operators/jit_operator/jit_utils.hpp"

namespace opossum {

namespace {

bool is_row_visible(CommitID our_tid, CommitID snapshot_commit_id, ChunkOffset chunk_offset,
                    const MvccColumns& columns) {
  Mvcc mvcc = no_inline::unpack_mvcc(columns, chunk_offset);

  // Taken from: https://github.com/hyrise/hyrise/blob/master/docs/documentation/queryexecution/tx.rst
  // auto own_insert = (our_tid == row_tid) && !(snapshot_commit_id >= begin_cid) && !(snapshot_commit_id >= end_cid);
  // auto past_insert = (our_tid != row_tid) && (snapshot_commit_id >= begin_cid) && !(snapshot_commit_id >= end_cid);
  // return own_insert || past_insert;

  // since gcc and clang are surprisingly bad at optimizing the above boolean expression, lets do that ourselves
  return snapshot_commit_id < mvcc.end_cid && ((snapshot_commit_id >= mvcc.begin_cid) != (mvcc.row_tid == our_tid));
}

}  // namespace

JitValidate::JitValidate(const TransactionID transaction_id, const CommitID snapshot_commit_id,
                         const bool use_ref_pos_list)
    : _transaction_id(transaction_id), _snapshot_commit_id(snapshot_commit_id), _use_ref_pos_list(use_ref_pos_list) {}

JitValidate::JitValidate(const std::shared_ptr<TransactionContext>& transaction_context, const bool use_ref_pos_list)
    : _transaction_id(transaction_context->transaction_id()),
      _snapshot_commit_id(transaction_context->snapshot_commit_id()),
      _use_ref_pos_list(use_ref_pos_list) {}

std::string JitValidate::description() const {
  return "[Validate] with transaction id: " + std::to_string(_transaction_id) +
         ", commit id: " + std::to_string(_snapshot_commit_id);
}

TransactionID JitValidate::transaction_id() const { return _transaction_id; }

CommitID JitValidate::snapshot_commit_id() const { return _snapshot_commit_id; }

void JitValidate::_consume(JitRuntimeContext& context) const {
  bool row_is_visible;
  if (_use_ref_pos_list) {
    const auto row_id = *context.pos_list_itr;
    const auto referenced_chunk = context.referenced_table->get_chunk(row_id.chunk_id);
    const auto mvcc_columns = referenced_chunk->mvcc_columns();
    row_is_visible = is_row_visible(_transaction_id, _snapshot_commit_id, row_id.chunk_offset, *mvcc_columns);
  } else {
    row_is_visible = is_row_visible(_transaction_id, _snapshot_commit_id, context.chunk_offset, *context.columns);
  }
  if (row_is_visible) _emit(context);
}

}  // namespace opossum
