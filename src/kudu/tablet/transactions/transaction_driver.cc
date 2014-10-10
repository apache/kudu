// Copyright (c) 2014, Cloudera, inc.

#include "kudu/tablet/transactions/transaction_driver.h"

#include "kudu/consensus/consensus.h"
#include "kudu/tablet/tablet_peer.h"
#include "kudu/tablet/transactions/transaction_tracker.h"
#include "kudu/util/task_executor.h"
#include "kudu/util/trace.h"

namespace kudu {
namespace tablet {

using consensus::CommitMsg;
using consensus::Consensus;
using consensus::ConsensusRound;
using consensus::ReplicateMsg;
using consensus::CommitMsg;
using consensus::DriverType;
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
              boost::bind(&TransactionDriver::Finalize, this),
              boost::bind(&TransactionDriver::ApplyOrCommitFailed, this, _1))),
      prepare_executor_(prepare_executor),
      apply_executor_(apply_executor),
      trace_(new Trace()),
      start_time_(MonoTime::Now(MonoTime::FINE)) {
}

void TransactionDriver::Init(Transaction* transaction) {
  boost::lock_guard<simple_spinlock> lock(lock_);
  transaction_.reset(transaction);
  txn_tracker_->Add(this);
}

// TaskExecutor forces submitted methods to return a Status so we handle whatever error
// happen synchronously and always return Status::OK();
// TODO: Consider exposing underlying ThreadPool::Submit()/SubmitFunc() methods in
// TaskExecutor so we don't need to do this.
Status TransactionDriver::ApplyTask() {
  Status s = Apply();
  if (!s.ok()) {
    ApplyOrCommitFailed(s);
  }
  return Status::OK();
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
  boost::lock_guard<simple_spinlock> lock(lock_);
  return ToStringUnlocked();
}

string TransactionDriver::ToStringUnlocked() const {
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
                        apply_executor) {
}

Status LeaderTransactionDriver::Execute() {
  ADOPT_TRACE(trace());

  // TODO: we don't really need to use a future here -- the task itself
  // handles triggering Replicate once it is complete. We should add
  // a way to submit a simple Callback to the underlying threadpool.
  shared_ptr<Future> prepare_task_future;
  RETURN_NOT_OK(prepare_executor_->Submit(
      boost::bind(&LeaderTransactionDriver::PrepareAndStartTask, this),
      &prepare_task_future));

  return Status::OK();
}

Status LeaderTransactionDriver::PrepareAndStartTask() {
  Status prepare_status = PrepareAndStart();
  if (PREDICT_FALSE(!prepare_status.ok())) {
    transaction_status_ = prepare_status;
    HandlePrepareOrReplicateFailure();
  }
  return Status::OK();
}

Status LeaderTransactionDriver::PrepareAndStart() {
  // Actually prepare and start the transaction.
  RETURN_NOT_OK(transaction_->Prepare());
  RETURN_NOT_OK(transaction_->Start());

  // Trigger the consensus replication.
  boost::lock_guard<simple_spinlock> lock(lock_);
  gscoped_ptr<ReplicateMsg> replicate_msg;
  transaction_->NewReplicateMsg(&replicate_msg);

  std::tr1::shared_ptr<FutureCallback> replicate_callback(
    new BoundFunctionCallback(
      boost::bind(&LeaderTransactionDriver::ReplicateSucceeded, this),
      boost::bind(&LeaderTransactionDriver::ReplicateFailed, this, _1)));

  gscoped_ptr<ConsensusRound> round(consensus_->NewRound(
                                      replicate_msg.Pass(),
                                      replicate_callback,
                                      commit_finished_callback_));

  RETURN_NOT_OK(consensus_->Replicate(round.get()));

  boost::lock_guard<simple_spinlock> op_id_lock(opid_lock_);
  op_id_copy_ = round->id();
  mutable_state()->set_consensus_round(round.Pass());
  return Status::OK();
}

void LeaderTransactionDriver::Abort() {
  boost::lock_guard<simple_spinlock> lock(lock_);
  // Just set the status, upon the next task completing this will cause
  // the transaction to abort.
  transaction_status_ = Status::Aborted("Transaction Aborted on request.");
}

void LeaderTransactionDriver::ReplicateSucceeded() {
  ADOPT_TRACE(trace());
  TRACE("REPLICATE succeeded");

  // TODO: this is an ugly hack so that the Release() call doesn't delete the
  // object while we still hold the lock.
  scoped_refptr<LeaderTransactionDriver> ref(this);
  boost::lock_guard<simple_spinlock> lock(lock_);

  // Even though Replicate succeeded, it's possible that the transaction
  // got Abort()ed.
  if (!transaction_status_.ok()) {
    HandlePrepareOrReplicateFailure();
    return;
  }

  shared_ptr<Future> apply_future;
  // TODO Allow to abort apply/commit. See KUDU-341
  Status s = apply_executor_->Submit(boost::bind(&LeaderTransactionDriver::ApplyTask, this),
                                     &apply_future);
  if (!s.ok()) {
    transaction_status_ = s;
    HandlePrepareOrReplicateFailure();
  }
}

void LeaderTransactionDriver::ReplicateFailed(const Status& failure_reason) {
  ADOPT_TRACE(trace());
  TRACE("REPLICATE failed: $0", failure_reason.ToString());
  // TODO: this is an ugly hack so that the Release() call doesn't delete the
  // object while we still hold the lock.
  scoped_refptr<LeaderTransactionDriver> ref(this);
  boost::lock_guard<simple_spinlock> lock(lock_);
  transaction_status_ = failure_reason;
  HandlePrepareOrReplicateFailure();
}

void LeaderTransactionDriver::HandlePrepareOrReplicateFailure() {
  DCHECK(!transaction_status_.ok());

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

Status LeaderTransactionDriver::Apply() {
  ADOPT_TRACE(trace());
  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    gscoped_ptr<CommitMsg> commit_msg;
    CHECK_OK(transaction_->Apply(&commit_msg));
    // If the client requested COMMIT_WAIT as the external consistency mode
    // calculate the latest that the prepare timestamp could be and wait
    // until now.earliest > prepare_latest. Only after this are the locks
    // released.
    if (mutable_state()->external_consistency_mode() == COMMIT_WAIT) {
      TRACE("APPLY: Commit Wait.");
      // If we can't commit wait and have already applied we might have consistency
      // issues if we still reply to the client that the operation was a success.
      // On the other hand we don't have rollbacks as of yet thus we can't undo the
      // the apply either, so we just CHECK_OK for now.
      CHECK_OK(CommitWait());
    }

    transaction_->PreCommit();
    CHECK_OK(mutable_state()->consensus_round()->Commit(commit_msg.Pass()));
    transaction_->PostCommit();
  }

  return Status::OK();
}

void LeaderTransactionDriver::Finalize() {
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

  LOG(WARNING) << "Commit failed in transaction: " << ToStringUnlocked()
      << " with Status: " << abort_reason.ToString();

  //TODO use an application level error status here with better error details.
  transaction_status_ = abort_reason;
  if (mutable_state() != NULL) {
    // Submit the commit abort
    gscoped_ptr<CommitMsg> commit;
    transaction_->NewCommitAbortMessage(&commit);
    // Make sure to remove the commit callback since the transaction will
    // be disappearing when this method ends.
    shared_ptr<FutureCallback> junk;
    mutable_state()->consensus_round()->release_commit_callback(&junk);
    junk.reset();
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
  {
    boost::lock_guard<simple_spinlock> lock(opid_lock_);
    op_id_copy_ = transaction->state()->op_id();
  }
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
  LOG(WARNING) << "Transaction aborted on request: " << ToStringUnlocked();
  transaction_->Finish();
  txn_tracker_->Release(this);
}

Status ReplicaTransactionDriver::ConsensusCommitted() {
  ADOPT_TRACE(trace());
  TRACE("ConsensusCommitted");
  {
    boost::lock_guard<simple_spinlock> state_lock(lock_);
    CHECK(!consensus_committed_) << "ConsensusCommitted() called twice: "
                                 << ToStringUnlocked();
    consensus_committed_ = true;
    PrepareFinishedOrConsensusCommittedUnlocked();
  }
  return Status::OK();
}

void ReplicaTransactionDriver::PrepareFinished(const Status& s) {
  ADOPT_TRACE(trace());
  TRACE("PrepareFinished: $0", s.ToString());

  {
    boost::lock_guard<simple_spinlock> state_lock(lock_);
    CHECK(!prepare_finished_) << "PrepareFinished() called twice: " << ToStringUnlocked();
    prepare_finished_ = true;

    if (!s.ok()) {
      transaction_status_ = s.CloneAndPrepend("Prepare failed");
    }

    PrepareFinishedOrConsensusCommittedUnlocked();
  }
}

void ReplicaTransactionDriver::PrepareFinishedOrConsensusCommittedUnlocked() {
  if (!prepare_finished_ || !consensus_committed_) {
    // We're still waiting on either the PREPARE to finish or the leader to commit.
    // This function will get called again when the other parallel portion finishes.
    return;
  }

  if (!transaction_status_.ok()) {
    // Abort the transaction if necessary.
    CHECK_OK(apply_executor_->Submit(boost::bind(&ReplicaTransactionDriver::AbortAndCommit, this),
                                     &apply_future_));
    return;
  }

  // Otherwise continue with the Apply phase
  StartAndTriggerApplyUnlocked();
}

void ReplicaTransactionDriver::StartAndTriggerApplyUnlocked() {
  // Replicas must start the actual transaction in the same order as they
  // receive it from the leader.
  CHECK_OK(transaction_->Start());

  if (transaction_status_.ok()) {
    Status s = apply_executor_->Submit(boost::bind(&ReplicaTransactionDriver::ApplyTask, this),
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

Status ReplicaTransactionDriver::AbortAndCommit() {
  boost::lock_guard<simple_spinlock> state_lock(lock_);
  HandlePrepareOrLeaderCommitFailure();
  return Status::OK();
}

void ReplicaTransactionDriver::HandlePrepareOrLeaderCommitFailure() {
  DCHECK(!transaction_status_.ok());

  // TODO handle op aborts. In particular, handle aborting operations when the leader
  // doesn't have them in his state machine.
  LOG(FATAL) << "An error occurred while preparing a transaction in a replica that"
      << " was successful at the leader. Replica Error: " << transaction_status_.ToString();
}

Status ReplicaTransactionDriver::Apply() {
  ADOPT_TRACE(trace());
  TRACE("Apply()");
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
      << " was successful at the leader. Replica Error: " << transaction_status_.ToString();
}


void ReplicaTransactionDriver::Finalize() {
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
}

ReplicaTransactionDriver::ReplicaTransactionDriver(TransactionTracker* txn_tracker,
                                                   consensus::Consensus* consensus,
                                                   TaskExecutor* prepare_executor,
                                                   TaskExecutor* apply_executor)
  : TransactionDriver(txn_tracker, consensus, prepare_executor, apply_executor),
    prepare_finished_(false),
    consensus_committed_(false) {
}

}  // namespace tablet
}  // namespace kudu
