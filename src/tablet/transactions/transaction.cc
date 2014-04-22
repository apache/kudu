// Copyright (c) 2013, Cloudera, inc.

#include "tablet/transactions/transaction.h"

#include <boost/thread/locks.hpp>

#include "consensus/consensus.h"
#include "rpc/rpc_context.h"
#include "tablet/transactions/transaction_tracker.h"
#include "tablet/tablet_peer.h"
#include "util/task_executor.h"
#include "util/trace.h"

namespace kudu {
namespace tablet {

using base::subtle::Barrier_AtomicIncrement;
using consensus::Consensus;
using consensus::ConsensusContext;
using consensus::ReplicateMsg;
using consensus::CommitMsg;
using rpc::RpcContext;
using std::tr1::shared_ptr;

Transaction::Transaction(TaskExecutor* prepare_executor,
                         TaskExecutor* apply_executor)
: prepare_finished_callback_(
    new BoundFunctionCallback(boost::bind(&Transaction::PrepareSucceeded, this),
                              boost::bind(&Transaction::PrepareFailed, this, _1))),
  commit_finished_callback_(
    new BoundFunctionCallback(boost::bind(&Transaction::ApplySucceeded, this),
                              boost::bind(&Transaction::ApplyFailed, this, _1))),
  prepare_executor_(prepare_executor),
  apply_executor_(apply_executor) {
}

Status Transaction::CommitWait() {
  DCHECK(tx_ctx()->external_consistency_mode() == COMMIT_WAIT);
  RETURN_NOT_OK(tx_ctx()->tablet_peer()->clock()->WaitUntilAfter(tx_ctx()->timestamp()));
  return Status::OK();
}

LeaderTransaction::LeaderTransaction(TransactionTracker *txn_tracker,
                                     Consensus* consensus,
                                     TaskExecutor* prepare_executor,
                                     TaskExecutor* apply_executor,
                                     simple_spinlock* prepare_replicate_lock)
    : Transaction(prepare_executor,
                  apply_executor),
      txn_tracker_(txn_tracker),
      consensus_(consensus),
      prepare_finished_calls_(0),
      prepare_replicate_lock_(DCHECK_NOTNULL(prepare_replicate_lock)) {
  txn_tracker_->Add(this);
}

Status LeaderTransaction::Execute() {
  // The remainder of this method needs to be guarded because, for any given
  // transactions A and B, if A prepares before B on the leader, then A must
  // also replicate before B to other nodes, so that those other nodes
  // serialize the transactions in the same order that the leader does.
  // This relies on the fact that (a) the prepare_executor_ only has a single
  // worker thread, and (b) that Consensus::Append calls do not get reordered
  // internally in the consensus implementation.
  boost::lock_guard<simple_spinlock> l(*prepare_replicate_lock_);


  gscoped_ptr<ReplicateMsg> replicate_msg;
  NewReplicateMsg(&replicate_msg);
  gscoped_ptr<ConsensusContext> context(consensus_->NewContext(replicate_msg.Pass(),
                                                               prepare_finished_callback_,
                                                               commit_finished_callback_));
  // persist the message through consensus, asynchronously
  Status s = consensus_->Replicate(context.get());
  if (!s.ok()) {
    prepare_finished_callback_->OnFailure(s);
    HandlePrepareFailure();
    return s;
  }

  tx_ctx()->set_consensus_ctx(context.Pass());

  // submit the prepare task
  shared_ptr<Future> prepare_task_future;
  s = prepare_executor_->Submit(boost::bind(&LeaderTransaction::Prepare, this),
                                boost::bind(&LeaderTransaction::AbortPrepare, this),
                                &prepare_task_future);
  if (!s.ok()) {
    prepare_finished_callback_->OnFailure(s);
    return s;
  }
  prepare_task_future->AddListener(prepare_finished_callback_);
  return Status::OK();
}

void LeaderTransaction::PrepareSucceeded() {
  // Atomically increase the number of calls. It doesn't matter whether Log
  // or Prepare finished first, we can only proceed when both are done.
  int num_tasks_finished = Barrier_AtomicIncrement(&prepare_finished_calls_, 1);
  if (num_tasks_finished < 2) {
    // Still waiting on the other task.
    return;
  }
  CHECK_EQ(2, num_tasks_finished);

  if (!prepare_status_.ok()) {
    HandlePrepareFailure();
    return;
  }

  shared_ptr<Future> apply_future;
  Status s = apply_executor_->Submit(boost::bind(&LeaderTransaction::Apply, this),
                                     boost::bind(&LeaderTransaction::AbortApply, this),
                                     &apply_future);
  if (!s.ok()) {
    prepare_status_ = s;
    HandlePrepareFailure();
  }
}

void LeaderTransaction::PrepareFailed(const Status& failure_reason) {
  prepare_status_ = failure_reason;
  int num_tasks_finished = Barrier_AtomicIncrement(&prepare_finished_calls_, 1);
  if (num_tasks_finished < 2) {
    // Still waiting on the other task.
    return;
  }
  HandlePrepareFailure();
}

void LeaderTransaction::HandlePrepareFailure() {
  DCHECK(!prepare_status_.ok());
  // once HandlePrepareFailure() has been called there is no need for additional
  // error handling on the dctor.
  prepare_finished_calls_ = 2;

  // set the error on the completion callback
  tx_ctx()->completion_callback()->set_error(prepare_status_);

  // If there is no consensus context nothing got done so just reply to the client.
  if (tx_ctx()->consensus_ctx() == NULL) {
    tx_ctx()->completion_callback()->TransactionCompleted();
    txn_tracker_->Release(this);
    return;
  }

  gscoped_ptr<CommitMsg> commit(new CommitMsg());
  PrepareFailedPreCommitHooks(&commit);

  // ConsensusContext will own this pointer and dispose of it when it is no longer
  // required.
  Status s = tx_ctx()->consensus_ctx()->Commit(commit.Pass());
  if (!s.ok()) {
    LOG(ERROR) << "Could not commit transaction abort message. Status: " << s.ToString();
    // we couldn't commit the prepare failure either, which means the commit callback
    // will never be called, so we need to notify the caller here.
    tx_ctx()->completion_callback()->TransactionCompleted();
  }
}

void LeaderTransaction::ApplySucceeded() {
  UpdateMetrics();
  tx_ctx()->completion_callback()->TransactionCompleted();
  txn_tracker_->Release(this);
}

void LeaderTransaction::ApplyFailed(const Status& abort_reason) {
  //TODO use an application level error status here with better error details.
  tx_ctx()->completion_callback()->set_error(abort_reason);
  tx_ctx()->completion_callback()->TransactionCompleted();
  txn_tracker_->Release(this);
}

LeaderTransaction::~LeaderTransaction() {
  if (prepare_finished_calls_ < 2) {
    Status s = prepare_status_;
    if (s.ok()) {
      s = Status::IllegalState(
          "Consensus::Append()/Transaction::PrepareSucceded was only called once.");
    }
    HandlePrepareFailure();
  }
}


TransactionMetrics::TransactionMetrics()
  : successful_inserts(0),
    successful_updates(0) {
}

void TransactionMetrics::Reset() {
  successful_inserts = 0;
  successful_updates = 0;
}


}  // namespace tablet
}  // namespace kudu
