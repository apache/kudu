// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_TABLET_TASKS_H_
#define KUDU_TABLET_TASKS_H_

#include "gutil/macros.h"
#include "util/task_executor.h"

namespace kudu {
namespace tablet {

class TransactionContext;

class TabletPeer;

//=============================================================================
//  Tablet Specific Tasks
//=============================================================================

// A prepare task for a transaction.
//
// Acquires all the relevant row locks for the transaction, the tablet
// component_lock in shared mode and starts an Mvcc transaction. When the task
// is finished, the next one in the pipeline must take ownership of these.
class PrepareTask : public Task {
 public:
  explicit PrepareTask(tablet::TransactionContext *tx_ctx);

  Status Run();
  bool Abort();

 private:
  tablet::TransactionContext *tx_ctx_;

  DISALLOW_COPY_AND_ASSIGN(PrepareTask);
};

// Actually applies inserts/mutates into the tablet. After these start being
// applied, the transaction must run to completion as there is currently no
// means of undoing an update.
//
// After completing the inserts/mutates, the row locks and the mvcc transaction
// can be released, allowing other transactions to update the same rows.
// However the component lock must not be released until the commit msg, which
// indicates where each of the inserts/mutates were applied, is persisted to
// stable storage. Because of this ApplyTask must enqueue a CommitTask before
// releasing both the row locks and deleting the MvccTransaction as we need to
// make sure that Commits that touch the same set of rows are persisted in
// order, for recovery.
// This, of course, assumes that commits are executed in the same order they
// are placed in the queue (but not necessarily in the same order of the
// original requests) which is already a requirement of the consensus
// algorithm.
class ApplyTask : public Task {
 public:
  explicit ApplyTask(tablet::TransactionContext *tx_ctx);

  Status Run();
  bool Abort();

 private:
  tablet::TransactionContext *tx_ctx_;

  DISALLOW_COPY_AND_ASSIGN(ApplyTask);
};

}  // namespace tablet
}  // namespace kudu

#endif /* KUDU_TABLET_TASKS_H_ */
