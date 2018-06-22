#pragma once

#include "abstract_jittable.hpp"
#include "types.hpp"
#include "concurrency/transaction_context.hpp"

namespace opossum {

/* The JitFilter operator filters on a single boolean value and only passes on
 * tuple, for which that value is non-null and true.
 */
class JitValidate : public AbstractJittable {
 public:
  explicit JitValidate(const TransactionID transaction_id, const CommitID snapshot_commit_id);
  explicit JitValidate(const std::shared_ptr<TransactionContext>& transaction_context);

  std::string description() const final;

  TransactionID transaction_id() const;
  CommitID snapshot_commit_id() const;

 private:
  void _consume(JitRuntimeContext& context) const final;

  const TransactionID _transaction_id;
  const CommitID _snapshot_commit_id;
};

}  // namespace opossum