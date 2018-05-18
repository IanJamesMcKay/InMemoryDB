#include "execute_with_timeout.hpp"

#include "concurrency/transaction_context.hpp"
#include "concurrency/transaction_manager.hpp"
#include "scheduler/current_scheduler.hpp"
#include "sql/sql_query_plan.hpp"

namespace opossum {

bool execute_with_timeout(const std::shared_ptr<AbstractOperator>& pqp, const std::optional<std::chrono::seconds>& timeout) {
  auto transaction_context = TransactionManager::get().new_transaction_context();
  pqp->set_transaction_context_recursively(transaction_context);

  // Schedule timeout
  if (timeout) {
    std::thread timeout_thread([transaction_context, timeout]() {
      std::this_thread::sleep_for(*timeout);
      transaction_context->rollback(TransactionPhaseSwitch::Lenient);
    });
    timeout_thread.detach();
  }

  // Execute Plan
  SQLQueryPlan plan;
  plan.add_tree_by_root(pqp);

  CurrentScheduler::schedule_and_wait_for_tasks(plan.create_tasks());

  return !transaction_context->commit(TransactionPhaseSwitch::Lenient);
}

}  // namespace opossum