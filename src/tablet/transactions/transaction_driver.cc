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
using consensus::DriverType;
using consensus::OperationPB;
using std::tr1::shared_ptr;

////////////////////////////////////////////////////////////
// TransactionDriver
////////////////////////////////////////////////////////////

TransactionDriver::TransactionDriver(TransactionTracker *txn_tracker,
                                     Consensus* consensus,
                                     TaskExecutor* prepare_executor,
                                     TaskExecutor* apply_executor)
    : txn_tracker_(txn_tracker),
      consensus_(consensus),
      commit_finished_callback_(
          new BoundFunctionCallback(
              boost::bind(&TransactionDriver::ApplyAndCommitSucceeded, this),
              boost::bind(&TransactionDriver::ApplyOrCommitFailed, this, _1))),
      prepare_executor_(prepare_executor),
      apply_executor_(apply_executor),
      prepare_finished_calls_(0),
      trace_(new Trace()),
      start_time_(MonoTime::Now(MonoTime::FINE)) {
}

void TransactionDriver::Init(Transaction* transaction) {
  boost::lock_guard<simple_spinlock> lock(lock_);
  transaction_.reset(transaction);
  txn_tracker_->Add(this);
}

consensus::OpId TransactionDriver::GetOpId() {
  boost::lock_guard<simple_spinlock> lock(opid_lock_);
  return op_id_copy_;
}

const TransactionState* TransactionDriver::state() const {
  return transaction_ != NULL ? transaction_->state() : NULL;
}

TransactionState* TransactionDriver::mutable_state() {
  return transaction_ != NULL ? transaction_->state() : NULL;
}

Transaction::TransactionType TransactionDriver::tx_type() const {
  return transaction_->tx_type();
}

DriverType TransactionDriver::type() const {
  return transaction_->type();
}

const std::tr1::shared_ptr<FutureCallback>& TransactionDriver::commit_finished_callback() {
  boost::lock_guard<simple_spinlock> lock(lock_);
  return commit_finished_callback_;
}

string TransactionDriver::ToString() const {
  return transaction_ != NULL ? transaction_->ToString() : "";
}

////////////////////////////////////////////////////////////
// LeaderTransactionDriver
////////////////////////////////////////////////////////////

void LeaderTransactionDriver::Create(Transaction* transaction,
                                     TransactionTracker* txn_tracker,
                                     Consensus* consensus,
                                     TaskExecutor* prepare_executor,
                                     TaskExecutor* apply_executor,
                                     simple_spinlock* prepare_replicate_lock,
                                     scoped_refptr<LeaderTransactionDriver>* driver) {
  driver->reset(new LeaderTransactionDriver(txn_tracker, consensus, prepare_executor,
                                            apply_executor, prepare_replicate_lock));
  (*driver)->Init(transaction);
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

Status LeaderTransactionDriver::Execute() {
  ADOPT_TRACE(trace());

  const shared_ptr<FutureCallback> prepare_replicate_callback(
    new BoundFunctionCallback(
      boost::bind(&LeaderTransactionDriver::PrepareOrReplicateSucceeded, this),
      boost::bind(&LeaderTransactionDriver::PrepareOrReplicateFailed, this, _1)));

  Status prepare_status;
  Status replicate_status;

  shared_ptr<Future> prepare_task_future;
  {
    boost::lock_guard<simple_spinlock> lock(lock_);

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
                                                             prepare_replicate_callback,
                                                             commit_finished_callback_));

      replicate_status = consensus_->Replicate(round.get());
      if (replicate_status.ok()) {
        // See: TransactionDriver::GetOpId() and opid_lock_ declaration.
        boost::lock_guard<simple_spinlock> lock(opid_lock_);
        op_id_copy_ = round->id();
        mutable_state()->set_consensus_round(round.Pass());

        prepare_status = prepare_executor_->Submit(
          boost::bind(&Transaction::Prepare, transaction_.get()),
          boost::bind(&Transaction::AbortPrepare, transaction_.get()),
          &prepare_task_future);

      } else {
        // If replicate failed, don't bother preparing
        prepare_status = Status::Aborted("Replicate failed, not preparing");
      }
    }
  }

  if (!prepare_status.ok()) {
    // Mark as failed.
    prepare_replicate_callback->OnFailure(prepare_status);
  } else {
    prepare_task_future->AddListener(prepare_replicate_callback);
  }

  if (!replicate_status.ok()) {
    prepare_replicate_callback->OnFailure(replicate_status);
  }

  return Status::OK();
}

void LeaderTransactionDriver::Abort() {
  boost::lock_guard<simple_spinlock> lock(lock_);
  // Just set the status, upon the next task completing this will cause
  // the transaction to abort.
  transaction_status_ = Status::Aborted("Transaction Aborted on request.");
}

void LeaderTransactionDriver::PrepareOrReplicateSucceeded() {
  ADOPT_TRACE(trace());
  // TODO: this is an ugly hack so that the Release() call doesn't delete the
  // object while we still hold the lock.
  scoped_refptr<LeaderTransactionDriver> ref(this);
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
  // TODO Allow to abort apply/commit. See KUDU-341
  Status s = apply_executor_->Submit(boost::bind(&LeaderTransactionDriver::ApplyAndCommit, this),
                                     &apply_future);
  if (!s.ok()) {
    transaction_status_ = s;
    HandlePrepareOrReplicateFailure();
  }
}

void LeaderTransactionDriver::PrepareOrReplicateFailed(const Status& failure_reason) {
  ADOPT_TRACE(trace());
  // TODO: this is an ugly hack so that the Release() call doesn't delete the
  // object while we still hold the lock.
  scoped_refptr<LeaderTransactionDriver> ref(this);
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
  CHECK_EQ(2, prepare_finished_calls_);

  // set the error on the completion callback
  DCHECK_NOTNULL(mutable_state())->completion_callback()->set_error(transaction_status_);

  // If there is no consensus round nothing got done so just reply to the client.
  if (mutable_state()->consensus_round() == NULL) {
    transaction_->Finish();
    mutable_state()->completion_callback()->TransactionCompleted();
    txn_tracker_->Release(this);
    return;
  }

  gscoped_ptr<CommitMsg> commit;
  transaction_->NewCommitAbortMessage(&commit);

  // ConsensusRound will own this pointer and dispose of it when it is no longer
  // required.
  Status s = mutable_state()->consensus_round()->Commit(commit.Pass());
  if (!s.ok()) {
    LOG(ERROR) << "Could not commit transaction abort message. Status: " << s.ToString();
    // we couldn't commit the prepare failure either, which means the commit callback
    // will never be called, so we need to notify the caller here.
    transaction_->Finish();
    mutable_state()->completion_callback()->TransactionCompleted();
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
  ADOPT_TRACE(trace());
  Status s;
  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    gscoped_ptr<CommitMsg> commit_msg;
    s = transaction_->Apply(&commit_msg);
    // If the client requested COMMIT_WAIT as the external consistency mode
    // calculate the latest that the prepare timestamp could be and wait
    // until now.earliest > prepare_latest. Only after this are the locks
    // released.
    if (s.ok() && mutable_state()->external_consistency_mode() == COMMIT_WAIT) {
      TRACE("APPLY: Commit Wait.");
      // If we can't commit wait and have already applied we might have consistency
      // issues if we still reply to the client that the operation was a success.
      // On the other hand we don't have rollbacks as of yet thus we can't undo the
      // the apply either, so we just CHECK_OK for now.
      CHECK_OK(CommitWait());
    }

    if (PREDICT_TRUE(s.ok())) {
      transaction_->PreCommit();
      s = mutable_state()->consensus_round()->Commit(commit_msg.Pass());
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
  ADOPT_TRACE(trace());
  // TODO: this is an ugly hack so that the Release() call doesn't delete the
  // object while we still hold the lock.
  scoped_refptr<LeaderTransactionDriver> ref(this);
  boost::lock_guard<simple_spinlock> lock(lock_);
  transaction_->Finish();
  mutable_state()->completion_callback()->TransactionCompleted();
  txn_tracker_->Release(this);
}

void LeaderTransactionDriver::ApplyOrCommitFailed(const Status& abort_reason) {
  ADOPT_TRACE(trace());
  // TODO: this is an ugly hack so that the Release() call doesn't delete the
  // object while we still hold the lock.
  scoped_refptr<LeaderTransactionDriver> ref(this);
  boost::lock_guard<simple_spinlock> lock(lock_);
  CHECK_EQ(prepare_finished_calls_, 2);

  LOG(WARNING) << "Commit failed in transaction: " << ToString()
      << " with Status: " << abort_reason.ToString();

  //TODO use an application level error status here with better error details.
  transaction_status_ = abort_reason;
  if (mutable_state() != NULL) {
    // Submit the commit abort
    gscoped_ptr<CommitMsg> commit;
    transaction_->NewCommitAbortMessage(&commit);
    // Make sure to remove the commit callback since the transaction will
    // be disappearing when this method ends.
    mutable_state()->consensus_round()->release_commit_callback();
    WARN_NOT_OK(mutable_state()->consensus_round()->Commit(commit.Pass()),
                "Could not submit commit abort message.")

    transaction_->Finish();
    mutable_state()->completion_callback()->set_error(abort_reason);
    mutable_state()->completion_callback()->TransactionCompleted();
  }
  txn_tracker_->Release(this);
}

Status LeaderTransactionDriver::CommitWait() {
  MonoTime before = MonoTime::Now(MonoTime::FINE);
  DCHECK(mutable_state()->external_consistency_mode() == COMMIT_WAIT);
  RETURN_NOT_OK(
      mutable_state()->tablet_peer()->clock()->WaitUntilAfter(mutable_state()->timestamp()));
  mutable_state()->mutable_metrics()->commit_wait_duration_usec =
      MonoTime::Now(MonoTime::FINE).GetDeltaSince(before).ToMicroseconds();
  return Status::OK();
}

LeaderTransactionDriver::~LeaderTransactionDriver() {
  if (prepare_finished_calls_ < 2) {
    HandlePrepareOrReplicateFailure();
  }
}

////////////////////////////////////////////////////////////
// ReplicaTransactionDriver
////////////////////////////////////////////////////////////

void ReplicaTransactionDriver::Create(Transaction* transaction,
                                      TransactionTracker* txn_tracker,
                                      Consensus* consensus,
                                      TaskExecutor* prepare_executor,
                                      TaskExecutor* apply_executor,
                                      scoped_refptr<ReplicaTransactionDriver>* driver) {
  driver->reset(new ReplicaTransactionDriver(txn_tracker, consensus,
                                             prepare_executor, apply_executor));
  (*driver)->Init(transaction);
}

void ReplicaTransactionDriver::Init(Transaction* transaction) {
  op_id_copy_ = transaction->state()->op_id();
  TransactionDriver::Init(transaction);
}

Status ReplicaTransactionDriver::Execute() {
  ADOPT_TRACE(trace());

  shared_ptr<FutureCallback> prepare_callback(
    new BoundFunctionCallback(
      boost::bind(&ReplicaTransactionDriver::PrepareFinished, this, Status::OK()),
      boost::bind(&ReplicaTransactionDriver::PrepareFinished, this, _1)));
  Status s;
  shared_ptr<Future> prepare_task_future;
  {
    boost::lock_guard<simple_spinlock> state_lock(lock_);

    // submit the prepare task
    s = prepare_executor_->Submit(boost::bind(&Transaction::Prepare, transaction_.get()),
                                  boost::bind(&Transaction::AbortPrepare, transaction_.get()),
                                  &prepare_task_future);
  }

  if (PREDICT_TRUE(s.ok())) {
    prepare_task_future->AddListener(prepare_callback);
  } else {
    prepare_callback->OnFailure(s);
    return s;
  }
  return Status::OK();
}

void ReplicaTransactionDriver::Abort() {
  // TODO: this is an ugly hack so that the Release() call doesn't delete the
  // object while we still hold the lock.
  scoped_refptr<ReplicaTransactionDriver> ref(this);
  boost::lock_guard<simple_spinlock> state_lock(lock_);
  prepare_finished_calls_ = 2;
  LOG(WARNING) << "Transaction aborted on request: " << ToString();
  transaction_->Finish();
  txn_tracker_->Release(this);
}

Status ReplicaTransactionDriver::LeaderCommitted(gscoped_ptr<OperationPB> leader_commit_op) {
  ADOPT_TRACE(trace());
  OperationPB* leader_op;
  {
    boost::lock_guard<simple_spinlock> state_lock(lock_);
    mutable_state()->consensus_round()->SetLeaderCommitOp(leader_commit_op.Pass());
    leader_op = mutable_state()->consensus_round()->leader_commit_op();
  }
  TRACE("Leader committed: $0",
        OperationType_Name(leader_op->commit().op_type()));
  // check if the leader aborted the transaction
  if (leader_op->commit().op_type() == consensus::OP_ABORT) {
    PrepareOrLeaderCommitFailed(Status::Aborted("Leader aborted Operation"));
    // Note that we still return Status::OK() since aborting the same way as the leader
    // is not an error.
    return Status::OK();
  }
  PrepareOrLeaderCommitSucceeded();
  return Status::OK();
}

void ReplicaTransactionDriver::PrepareFinished(const Status& s) {
  ADOPT_TRACE(trace());
  TRACE("PrepareFinished: $0", s.ToString());
  if (s.ok()) {
    PrepareOrLeaderCommitSucceeded();
  } else {
    PrepareOrLeaderCommitFailed(s);
  }
}

void ReplicaTransactionDriver::PrepareOrLeaderCommitSucceeded() {
  boost::lock_guard<simple_spinlock> state_lock(lock_);
  // Atomically increase the number of calls.
  prepare_finished_calls_++;
  if (prepare_finished_calls_ < 2) {
    return;
  }
  CHECK_EQ(2, prepare_finished_calls_);

  if (transaction_status_.ok()) {
    Status s = apply_executor_->Submit(boost::bind(&ReplicaTransactionDriver::ApplyAndCommit, this),
                                       &apply_future_);
    if (PREDICT_TRUE(s.ok())) {
      return;
    }
    transaction_status_ = s;
  }

  // submit the handle leader commit failure on the apply executor, if possible, as the
  // current thread already holds the lock for the consensus update.
  CHECK_OK(apply_executor_->Submit(boost::bind(&ReplicaTransactionDriver::AbortAndCommit, this),
                                   &apply_future_));
}

void ReplicaTransactionDriver::PrepareOrLeaderCommitFailed(const Status& failure_reason) {
  boost::lock_guard<simple_spinlock> state_lock(lock_);
  transaction_status_ = failure_reason;
  prepare_finished_calls_++;
  if (prepare_finished_calls_ < 2) {
    return;
  }
  CHECK_OK(apply_executor_->Submit(boost::bind(&ReplicaTransactionDriver::AbortAndCommit, this),
                                   &apply_future_));
}

Status ReplicaTransactionDriver::AbortAndCommit() {
  boost::lock_guard<simple_spinlock> state_lock(lock_);
  HandlePrepareOrLeaderCommitFailure();
  return Status::OK();
}

void ReplicaTransactionDriver::HandlePrepareOrLeaderCommitFailure() {
  DCHECK(!transaction_status_.ok());
  CHECK_EQ(prepare_finished_calls_, 2);

  // If we're here one of two things happened:
  // - The leader sent an OP_ABORT, in which case we abort with the same message
  // - The leader committed but we had another failure, in which case we FATAL or
  //   risk diverging from the leader.

  OperationPB* leader_op = mutable_state()->consensus_round()->leader_commit_op();
  if (leader_op->commit().op_type() == consensus::OP_ABORT) {
    gscoped_ptr<CommitMsg> commit(new CommitMsg());
    transaction_->NewCommitAbortMessage(&commit);
    // If quiescing is properly impl. this should always succeed. Might not be
    // mega serious if it doens't but let's CHECK_OK() for now and handle it
    // if it ever fails.
    CHECK_OK(mutable_state()->consensus_round()->Commit(commit.Pass()));
    return;
  }
  LOG(FATAL) << "An error occurred while preparing a transaction in a replica that"
      << " was successful at the leader. Replica Error: " << transaction_status_.ToString()
      << "\n LeaderOp: " << leader_op->ShortDebugString();
}

// See: LeaderTransactionDriver::ApplyAndCommit();
Status ReplicaTransactionDriver::ApplyAndCommit() {
  ADOPT_TRACE(trace());
  TRACE("ApplyAndCommit()");
  Status s;
  {
    boost::lock_guard<simple_spinlock> state_lock(lock_);
    gscoped_ptr<CommitMsg> commit_msg;
    s = transaction_->Apply(&commit_msg);

    if (PREDICT_TRUE(s.ok())) {
      transaction_->PreCommit();
      s = mutable_state()->consensus_round()->Commit(commit_msg.Pass());
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

void ReplicaTransactionDriver::ApplyOrCommitFailed(const Status& abort_reason) {
  TRACE("ApplyOrCommitFailed($0)", abort_reason.ToString());
  transaction_status_ = abort_reason;
  // If we ere told to Apply & Commit it was because the leader
  // succeeded so if we failed for some reason we might diverge.
  // For now we simply FATAL out, later on we might try to do
  // cleverer error handling for specific cases but those need
  // to be carefully reasoned about.
  LOG(FATAL) << "An error occurred while applying/committing a transaction in a replica that"
      << " was successful at the leader. Replica Error: " << transaction_status_.ToString()
      << "\n LeaderOp: "
      << mutable_state()->consensus_round()->leader_commit_op()->ShortDebugString();
}


void ReplicaTransactionDriver::ApplyAndCommitSucceeded() {
  ADOPT_TRACE(trace());
  TRACE("ApplyAndCommitSucceeded()");
  // TODO: this is an ugly hack so that the Release() call doesn't delete the
  // object while we still hold the lock.
  scoped_refptr<ReplicaTransactionDriver> ref(this);
  boost::lock_guard<simple_spinlock> state_lock(lock_);
  transaction_->Finish();
  txn_tracker_->Release(this);
}

ReplicaTransactionDriver::~ReplicaTransactionDriver() {
  CHECK_EQ(prepare_finished_calls_, 2);
}

ReplicaTransactionDriver::ReplicaTransactionDriver(TransactionTracker* txn_tracker,
                                                   consensus::Consensus* consensus,
                                                   TaskExecutor* prepare_executor,
                                                   TaskExecutor* apply_executor)
    : TransactionDriver(txn_tracker, consensus, prepare_executor, apply_executor) {
}

}  // namespace tablet
}  // namespace kudu
