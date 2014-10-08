// Copyright (c) 2014, Cloudera, inc.

#include "kudu/tablet/transactions/transaction_driver.h"

#include "kudu/consensus/consensus.h"
#include "kudu/tablet/tablet_peer.h"
#include "kudu/tablet/transactions/transaction_tracker.h"
#include "kudu/util/debug-util.h"
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
              boost::bind(&TransactionDriver::HandleFailure, this, _1))),
      prepare_executor_(prepare_executor),
      apply_executor_(apply_executor),
      trace_(new Trace()),
      start_time_(MonoTime::Now(MonoTime::FINE)),
      replication_state_(NOT_REPLICATING),
      prepare_state_(NOT_PREPARED) {
}

void TransactionDriver::Init(Transaction* transaction,
                             DriverType type) {
  boost::lock_guard<simple_spinlock> lock(lock_);
  transaction_.reset(transaction);

  if (type == consensus::REPLICA) {
    boost::lock_guard<simple_spinlock> lock(opid_lock_);
    op_id_copy_ = transaction->state()->op_id();
    DCHECK(op_id_copy_.IsInitialized());
    replication_state_ = REPLICATING;
  }

  txn_tracker_->Add(this);
}

// TaskExecutor forces submitted methods to return a Status so we handle whatever error
// happen synchronously and always return Status::OK();
// TODO: Consider exposing underlying ThreadPool::Submit()/SubmitFunc() methods in
// TaskExecutor so we don't need to do this.
Status TransactionDriver::ApplyTask() {
  Status s = ApplyAndTriggerCommit();
  if (!s.ok()) {
    HandleFailure(s);
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


Status TransactionDriver::ExecuteAsync() {
  VLOG(2) << this << ": " << "ExecuteAsync()";
  ADOPT_TRACE(trace());

  // TODO: we don't really need to use a future here -- the task itself
  // handles triggering Replicate once it is complete. We should add
  // a way to submit a simple Callback to the underlying threadpool.
  shared_ptr<Future> prepare_task_future;
  RETURN_NOT_OK(prepare_executor_->Submit(
      boost::bind(&TransactionDriver::PrepareAndStartTask, this),
      &prepare_task_future));

  return Status::OK();
}

Status TransactionDriver::PrepareAndStartTask() {
  VLOG(2) << this << ": " << "PrepareAndStart()";
  Status prepare_status = PrepareAndStart();
  VLOG(2) << this << ": prepare_status: " << prepare_status.ToString();
  if (PREDICT_FALSE(!prepare_status.ok())) {
    HandleFailure(prepare_status);
  }
  return Status::OK();
}

Status TransactionDriver::PrepareAndStart() {
  // Actually prepare and start the transaction.
  RETURN_NOT_OK(transaction_->Prepare());

  // TODO: use the already-provided timestamp if there is one
  RETURN_NOT_OK(transaction_->Start());

  // Only take the lock long enough to take a local copy of the
  // replication state and set our prepare state. This ensures that
  // exactly one of Replicate/Prepare callbacks will trigger the apply
  // phase.
  ReplicationState repl_state_copy;
  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    CHECK_EQ(prepare_state_, NOT_PREPARED);
    prepare_state_ = PREPARED;
    repl_state_copy = replication_state_;
  }

  switch (repl_state_copy) {
    case NOT_REPLICATING:
    {
      VLOG(2) << this << ": " << "Triggering consensus repl";
      // Trigger the consensus replication.
      gscoped_ptr<ReplicateMsg> replicate_msg;
      transaction_->NewReplicateMsg(&replicate_msg);

      std::tr1::shared_ptr<FutureCallback> replicate_callback(
        new BoundFunctionCallback(
          boost::bind(&TransactionDriver::ConsensusCommitted, this),
          boost::bind(&TransactionDriver::HandleFailure, this, _1)));

      gscoped_ptr<ConsensusRound> round(consensus_->NewRound(
                                          replicate_msg.Pass(),
                                          replicate_callback,
                                          commit_finished_callback_));

      {
        boost::lock_guard<simple_spinlock> lock(lock_);
        replication_state_ = REPLICATING;
      }
      ConsensusRound* round_ptr = round.get();
      mutable_state()->set_consensus_round(round.Pass());
      Status s = consensus_->Replicate(round_ptr);
      if (PREDICT_FALSE(!s.ok())) {
        boost::lock_guard<simple_spinlock> lock(lock_);
        CHECK_EQ(replication_state_, REPLICATING);
        replication_state_ = REPLICATION_FAILED;
        return s;
      }
      break;
    }
    case REPLICATING:
    {
      // Already replicating - nothing to trigger
      break;
    }
    case REPLICATED:
    {
      // We can move on to apply.
      return ApplyAsync();
    }
    case REPLICATION_FAILED:
      LOG(FATAL) << "We should not Prepare() a transaction which has failed replication: "
                 << ToString();
  }

  return Status::OK();
}

void TransactionDriver::HandleFailure(const Status& s) {
  CHECK(!s.ok());
  TRACE("HandleFailure($0)", s.ToString());
  transaction_status_ = s;

  switch (replication_state_) {
    case NOT_REPLICATING:
    case REPLICATION_FAILED:
    {
      VLOG(1) << "Transaction " << ToString() << " failed prior to replication success: "
              << transaction_status_.ToString();
      // great, we didn't send it anywhere, so we can just reply.
      transaction_->Finish();
      mutable_state()->completion_callback()->set_error(transaction_status_);
      mutable_state()->completion_callback()->TransactionCompleted();
      txn_tracker_->Release(this);
      return;
    }

    case REPLICATING:
    case REPLICATED:
    {
      // If we fail after we've replicated an operation, we can't continue, since our
      // state would diverge from the rest of the quorum (and potentially our state would
      // not recover on restart if a Commit failed to persist).
      //
      // Some day, we may want to only mark this tablet as failed, rather than
      // crash the whole server, but that's substantially more complicated.

      LOG(FATAL) << "Cannot deal with failed transactions that have already replicated"
                 << ": " << transaction_status_.ToString()
                 << " transaction:" << ToString();
    }
  }
}


// TODO: why does this return a status?
Status TransactionDriver::ConsensusCommitted() {
  {
    boost::lock_guard<simple_spinlock> op_id_lock(opid_lock_);
    // TODO: it's a bit silly that we have three copies of the opid:
    // one here, one in ConsensusRound, and one in TransactionState.

    op_id_copy_ = DCHECK_NOTNULL(mutable_state()->consensus_round())->id();
    DCHECK(op_id_copy_.IsInitialized());
    mutable_state()->mutable_op_id()->CopyFrom(op_id_copy_);
  }

  PrepareState prepare_state_copy;
  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    CHECK_EQ(replication_state_, REPLICATING);
    replication_state_ = REPLICATED;
    prepare_state_copy = prepare_state_;
  }

  // If we have prepared and replicated, we're ready
  // to move ahead and apply this operation.
  if (prepare_state_copy == PREPARED) {
    return ApplyAsync();
  }
  return Status::OK();
}

void TransactionDriver::Abort() {
  // TODO: this is an ugly hack so that the Release() call doesn't delete the
  // object while we still hold the lock.
  scoped_refptr<TransactionDriver> ref(this);
  boost::lock_guard<simple_spinlock> state_lock(lock_);
  transaction_status_ = Status::Aborted("");
  LOG(WARNING) << "Transaction aborted on request: " << ToStringUnlocked();
  transaction_->Finish();
  txn_tracker_->Release(this);
}

Status TransactionDriver::ApplyAsync() {
  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    DCHECK_EQ(replication_state_, REPLICATED);
    DCHECK_EQ(prepare_state_, PREPARED);
    CHECK_OK(transaction_status_);
  }

  shared_ptr<Future> apply_future;
  return apply_executor_->Submit(boost::bind(&TransactionDriver::ApplyTask, this),
                                     &apply_future);
}

Status TransactionDriver::ApplyAndTriggerCommit() {
  ADOPT_TRACE(trace());
  {
    gscoped_ptr<CommitMsg> commit_msg;
    CHECK_OK(transaction_->Apply(&commit_msg));
    // If the client requested COMMIT_WAIT as the external consistency mode
    // calculate the latest that the prepare timestamp could be and wait
    // until now.earliest > prepare_latest. Only after this are the locks
    // released.
    if (mutable_state()->external_consistency_mode() == COMMIT_WAIT) {
      // TODO: only do this on the leader side
      TRACE("APPLY: Commit Wait.");
      // If we can't commit wait and have already applied we might have consistency
      // issues if we still reply to the client that the operation was a success.
      // On the other hand we don't have rollbacks as of yet thus we can't undo the
      // the apply either, so we just CHECK_OK for now.
      CHECK_OK(CommitWait());
    }

    // We need to ref-count ourself, since Commit() may run very quickly
    // and end up calling Finalize() while we're still in this code.
    scoped_refptr<TransactionDriver> ref(this);

    transaction_->PreCommit();
    CHECK_OK(mutable_state()->consensus_round()->Commit(commit_msg.Pass()));
    transaction_->PostCommit();
  }

  return Status::OK();
}

Status TransactionDriver::CommitWait() {
  MonoTime before = MonoTime::Now(MonoTime::FINE);
  DCHECK(mutable_state()->external_consistency_mode() == COMMIT_WAIT);
  RETURN_NOT_OK(
      mutable_state()->tablet_peer()->clock()->WaitUntilAfter(mutable_state()->timestamp()));
  mutable_state()->mutable_metrics()->commit_wait_duration_usec =
      MonoTime::Now(MonoTime::FINE).GetDeltaSince(before).ToMicroseconds();
  return Status::OK();
}

void TransactionDriver::Finalize() {
  ADOPT_TRACE(trace());
  // TODO: this is an ugly hack so that the Release() call doesn't delete the
  // object while we still hold the lock.
  scoped_refptr<TransactionDriver> ref(this);
  boost::lock_guard<simple_spinlock> lock(lock_);
  transaction_->Finish();
  if (mutable_state()->completion_callback()) {
    mutable_state()->completion_callback()->TransactionCompleted();
  }
  txn_tracker_->Release(this);
}

}  // namespace tablet
}  // namespace kudu
