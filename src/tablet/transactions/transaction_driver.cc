// Copyright (c) 2014, Cloudera, inc.

#include "tablet/transactions/transaction_driver.h"

#include "consensus/consensus.h"
#include "tablet/tablet_peer.h"
#include "tablet/transactions/transaction_tracker.h"
#include "util/task_executor.h"
#include "util/trace.h"

namespace kudu {
namespace tablet {

using consensus::Consensus;
using consensus::ConsensusRound;
using consensus::ReplicateMsg;
using consensus::CommitMsg;
using std::tr1::shared_ptr;

TransactionDriver::TransactionDriver(TransactionTracker *txn_tracker,
                                     Consensus* consensus,
                                     TaskExecutor* prepare_executor,
                                     TaskExecutor* apply_executor)
    : txn_tracker_(txn_tracker),
      consensus_(consensus),
      prepare_finished_callback_(
          new BoundFunctionCallback(
              boost::bind(&TransactionDriver::PrepareOrReplicateSucceeded, this),
              boost::bind(&TransactionDriver::PrepareOrReplicateFailed, this, _1))),
      commit_finished_callback_(
          new BoundFunctionCallback(
              boost::bind(&TransactionDriver::ApplyAndCommitSucceeded, this),
              boost::bind(&TransactionDriver::ApplyOrCommitFailed, this, _1))),
      prepare_executor_(prepare_executor),
      apply_executor_(apply_executor),
      prepare_finished_calls_(0) {
  txn_tracker_->Add(this);
}

consensus::OpId TransactionDriver::GetOpId() {
  boost::lock_guard<simple_spinlock> lock(opid_lock_);
  return op_id_copy_;
}

TransactionState* TransactionDriver::state() {
  return transaction_ != NULL ? transaction_->state() : NULL;
}

const std::tr1::shared_ptr<FutureCallback>& TransactionDriver::commit_finished_callback() {
  boost::lock_guard<simple_spinlock> lock(lock_);
  return commit_finished_callback_;
}

LeaderTransactionDriver::LeaderTransactionDriver(TransactionTracker* txn_tracker,
                                                 Consensus* consensus,
                                                 TaskExecutor* prepare_executor,
                                                 TaskExecutor* apply_executor,
                                                 simple_spinlock* prepare_replicate_lock)
    : TransactionDriver(txn_tracker,
                        consensus,
                        prepare_executor,
                        apply_executor),
      prepare_replicate_lock_(DCHECK_NOTNULL(prepare_replicate_lock)) {
}

Status LeaderTransactionDriver::Execute(Transaction* transaction) {

  Status s;
  shared_ptr<Future> prepare_task_future;
  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    transaction_.reset(transaction);

    // This portion of this method needs to be guarded across transactions
    // because, for any given transactions A and B, if A prepares before B
    // on the leader, then A must also replicate before B to other nodes, so
    // that those other nodes serialize the transactions in the same order that
    // the leader does. This relies on the fact that (a) the prepare_executor_
    // only has a single worker thread, and (b) that Consensus::Append calls do
    // not get reordered internally in the consensus implementation.
    {
      boost::lock_guard<simple_spinlock> l(*prepare_replicate_lock_);

      gscoped_ptr<ReplicateMsg> replicate_msg;
      transaction_->NewReplicateMsg(&replicate_msg);
      gscoped_ptr<ConsensusRound> round(consensus_->NewRound(replicate_msg.Pass(),
                                                             prepare_finished_callback_,
                                                             commit_finished_callback_));

      s = consensus_->Replicate(round.get());

      if (PREDICT_TRUE(s.ok())) {
        {
          // See: TransactionDriver::GetOpId() and opid_lock_ declaration.
          boost::lock_guard<simple_spinlock> lock(opid_lock_);
          op_id_copy_ = round->id();
        }

        state()->set_consensus_round(round.Pass());
        s = prepare_executor_->Submit(boost::bind(&Transaction::Prepare, transaction_.get()),
                                      boost::bind(&Transaction::AbortPrepare, transaction_.get()),
                                      &prepare_task_future);
      }
    }
  }

  if (PREDICT_TRUE(s.ok())) {
    prepare_task_future->AddListener(prepare_finished_callback_);
  } else {
    prepare_finished_callback_->OnFailure(s);
    return s;
  }
  return Status::OK();
}

void LeaderTransactionDriver::PrepareOrReplicateSucceeded() {
  boost::lock_guard<simple_spinlock> lock(lock_);
  prepare_finished_calls_++;
  if (prepare_finished_calls_ < 2) {
    // Still waiting on the other task.
    return;
  }
  CHECK_EQ(2, prepare_finished_calls_);

  if (!transaction_status_.ok()) {
    HandlePrepareOrReplicateFailure();
    return;
  }

  shared_ptr<Future> apply_future;
  // TODO Allow to abort apply/commit
  Status s = apply_executor_->Submit(boost::bind(&LeaderTransactionDriver::ApplyAndCommit, this),
                                     &apply_future);
  if (!s.ok()) {
    transaction_status_ = s;
    HandlePrepareOrReplicateFailure();
  }
}

void LeaderTransactionDriver::PrepareOrReplicateFailed(const Status& failure_reason) {
  boost::lock_guard<simple_spinlock> lock(lock_);
  transaction_status_ = failure_reason;
  prepare_finished_calls_++;
  if (prepare_finished_calls_ < 2) {
    // Still waiting on the other task.
    return;
  }
  HandlePrepareOrReplicateFailure();
}

void LeaderTransactionDriver::HandlePrepareOrReplicateFailure() {
  DCHECK(!transaction_status_.ok());
  // once HandlePrepareFailure() has been called there is no need for additional
  // error handling on the dctor.
  prepare_finished_calls_ = 2;

  // set the error on the completion callback
  DCHECK_NOTNULL(state())->completion_callback()->set_error(transaction_status_);

  // If there is no consensus round nothing got done so just reply to the client.
  if (state()->consensus_round() == NULL) {
    transaction_->Finish();
    state()->completion_callback()->TransactionCompleted();
    txn_tracker_->Release(this);
    return;
  }

  gscoped_ptr<CommitMsg> commit;
  transaction_->NewCommitAbortMessage(&commit);

  // ConsensusRound will own this pointer and dispose of it when it is no longer
  // required.
  Status s = state()->consensus_round()->Commit(commit.Pass());
  if (!s.ok()) {
    LOG(ERROR) << "Could not commit transaction abort message. Status: " << s.ToString();
    // we couldn't commit the prepare failure either, which means the commit callback
    // will never be called, so we need to notify the caller here.
    transaction_->Finish();
    state()->completion_callback()->TransactionCompleted();
    txn_tracker_->Release(this);
  }
}


// Note: Transaction::Apply() and Consensus::Commit() are called in sequence, so there
// is no reason for the Status returned by this function to be handled like the Status
// returned from Transaction::Prepare() and Consensus::Replicate(), which are executed
// concurrently. However TaskExecutor forces submitted methods to return a Status
// so we handle whatever error happen synchronously and always return Status::OK();
// TODO: Consider exposing underlying ThreadPool::Submit()/SubmitFunc() methods in
// TaskExecutor.
Status LeaderTransactionDriver::ApplyAndCommit() {
  Status s;
  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    gscoped_ptr<CommitMsg> commit_msg;
    s = transaction_->Apply(&commit_msg);
    // If the client requested COMMIT_WAIT as the external consistency mode
    // calculate the latest that the prepare timestamp could be and wait
    // until now.earliest > prepare_latest. Only after this are the locks
    // released.
    if (s.ok() && state()->external_consistency_mode() == COMMIT_WAIT) {
      TRACE("APPLY: Commit Wait.");
      // If we can't commit wait and have already applied we might have consistency
      // issues if we still reply to the client that the operation was a success.
      // On the other hand we don't have rollbacks as of yet thus we can't undo the
      // the apply either, so we just CHECK_OK for now.
      CHECK_OK(CommitWait());
    }

    if (PREDICT_TRUE(s.ok())) {
      transaction_->PreCommit();
      s = state()->consensus_round()->Commit(commit_msg.Pass());
      if (PREDICT_TRUE(s.ok())) {
        transaction_->PostCommit();
      }
    }
  }

  // If the apply or the commit failed, abort the transaction.
  if (PREDICT_FALSE(!s.ok())) {
    ApplyOrCommitFailed(s);
  }

  return Status::OK();
}

void LeaderTransactionDriver::ApplyAndCommitSucceeded() {
  boost::lock_guard<simple_spinlock> lock(lock_);
  transaction_->Finish();
  state()->completion_callback()->TransactionCompleted();
  txn_tracker_->Release(this);
}

void LeaderTransactionDriver::ApplyOrCommitFailed(const Status& abort_reason) {
  boost::lock_guard<simple_spinlock> lock(lock_);
  prepare_finished_calls_ = 2;

  //TODO use an application level error status here with better error details.
  transaction_status_ = abort_reason;
  if (state() != NULL) {
    // Submit the commit abort
    gscoped_ptr<CommitMsg> commit;
    transaction_->NewCommitAbortMessage(&commit);
    // Make sure to remove the commit callback since the transaction will
    // be disappearing when this method ends.
    state()->consensus_round()->release_commit_callback();
    WARN_NOT_OK(state()->consensus_round()->Commit(commit.Pass()),
                "Could not submit commit abort message.")

    transaction_->Finish();
    state()->completion_callback()->set_error(abort_reason);
    state()->completion_callback()->TransactionCompleted();
  }
  txn_tracker_->Release(this);
}

Status LeaderTransactionDriver::CommitWait() {
  MonoTime before = MonoTime::Now(MonoTime::FINE);
  DCHECK(state()->external_consistency_mode() == COMMIT_WAIT);
  RETURN_NOT_OK(state()->tablet_peer()->clock()->WaitUntilAfter(state()->timestamp()));
  state()->mutable_metrics()->commit_wait_duration_usec =
      MonoTime::Now(MonoTime::FINE).GetDeltaSince(before).ToMicroseconds();
  return Status::OK();
}

LeaderTransactionDriver::~LeaderTransactionDriver() {
  if (prepare_finished_calls_ < 2) {
    HandlePrepareOrReplicateFailure();
  }
}

}  // namespace tablet
}  // namespace kudu
