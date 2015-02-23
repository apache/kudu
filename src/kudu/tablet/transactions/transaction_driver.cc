// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/tablet/transactions/transaction_driver.h"

#include "kudu/consensus/consensus.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/tablet/tablet_peer.h"
#include "kudu/tablet/transactions/transaction_tracker.h"
#include "kudu/util/debug-util.h"
#include "kudu/util/logging.h"
#include "kudu/util/task_executor.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/trace.h"

namespace kudu {
namespace tablet {

using consensus::CommitMsg;
using consensus::Consensus;
using consensus::ConsensusRound;
using consensus::ReplicateMsg;
using consensus::CommitMsg;
using consensus::DriverType;
using log::Log;
using std::tr1::shared_ptr;


////////////////////////////////////////////////////////////
// TransactionDriver
////////////////////////////////////////////////////////////

TransactionDriver::TransactionDriver(TransactionTracker *txn_tracker,
                                     Consensus* consensus,
                                     Log* log,
                                     ThreadPool* prepare_pool,
                                     ThreadPool* apply_pool,
                                     TransactionOrderVerifier* order_verifier)
    : txn_tracker_(txn_tracker),
      consensus_(consensus),
      log_(log),
      prepare_pool_(prepare_pool),
      apply_pool_(apply_pool),
      order_verifier_(order_verifier),
      trace_(new Trace()),
      start_time_(MonoTime::Now(MonoTime::FINE)),
      replication_state_(NOT_REPLICATING),
      prepare_state_(NOT_PREPARED) {
  if (Trace::CurrentTrace()) {
    Trace::CurrentTrace()->AddChildTrace(trace_.get());
  }
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
  } else {
    DCHECK_EQ(type, consensus::LEADER);
    gscoped_ptr<ReplicateMsg> replicate_msg;
    transaction_->NewReplicateMsg(&replicate_msg);
    if (consensus_) { // sometimes NULL in tests
      mutable_state()->set_consensus_round(
        consensus_->NewRound(replicate_msg.Pass(), this));
    }
  }

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

string TransactionDriver::ToString() const {
  boost::lock_guard<simple_spinlock> lock(lock_);
  return ToStringUnlocked();
}

string TransactionDriver::ToStringUnlocked() const {
  string ret = StateString(replication_state_, prepare_state_);
  if (transaction_ != NULL) {
    ret += " " + transaction_->ToString();
  } else {
    ret += "[unknown txn]";
  }
  return ret;
}


Status TransactionDriver::ExecuteAsync() {
  VLOG_WITH_PREFIX_LK(4) << "ExecuteAsync()";
  ADOPT_TRACE(trace());

  Status s;
  if (replication_state_ == NOT_REPLICATING) {
    // We're a leader transaction. Before submitting, check that we are the leader and
    // determine the current term.
    s = consensus_->CheckLeadershipAndBindTerm(mutable_state()->consensus_round());
  }

  if (s.ok()) {
    s = prepare_pool_->SubmitClosure(
      Bind(&TransactionDriver::PrepareAndStartTask, Unretained(this)));
  }

  if (!s.ok()) {
    HandleFailure(s);
  }

  // TODO: make this return void
  return Status::OK();
}

void TransactionDriver::PrepareAndStartTask() {
  Status prepare_status = PrepareAndStart();
  if (PREDICT_FALSE(!prepare_status.ok())) {
    HandleFailure(prepare_status);
  }
}

Status TransactionDriver::PrepareAndStart() {
  VLOG_WITH_PREFIX_LK(4) << "PrepareAndStart()";
  // Actually prepare and start the transaction.
  prepare_physical_timestamp_ = GetMonoTimeMicros();
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
      VLOG_WITH_PREFIX_LK(4) << "Triggering consensus repl";
      // Trigger the consensus replication.

      {
        boost::lock_guard<simple_spinlock> lock(lock_);
        replication_state_ = REPLICATING;
      }
      Status s = consensus_->Replicate(mutable_state()->consensus_round());

      if (PREDICT_FALSE(!s.ok())) {
        boost::lock_guard<simple_spinlock> lock(lock_);
        CHECK_EQ(replication_state_, REPLICATING);
        transaction_status_ = s;
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
    case REPLICATION_FAILED: {
      DCHECK(!transaction_status_.ok());
      HandleFailure(transaction_status_);
    }
  }

  return Status::OK();
}

void TransactionDriver::HandleFailure(const Status& s) {
  VLOG_WITH_PREFIX_LK(2) << "Failed transaction: " << s.ToString();
  CHECK(!s.ok());
  TRACE("HandleFailure($0)", s.ToString());

  ReplicationState repl_state_copy;

  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    transaction_status_ = s;
    repl_state_copy = replication_state_;
  }


  switch (repl_state_copy) {
    case NOT_REPLICATING:
    case REPLICATION_FAILED:
    {
      VLOG_WITH_PREFIX_LK(1) << "Transaction " << ToString() << " failed prior to "
          "replication success: " << s.ToString();
      transaction_->Finish(Transaction::ABORTED);
      mutable_state()->completion_callback()->set_error(transaction_status_);
      mutable_state()->completion_callback()->TransactionCompleted();
      txn_tracker_->Release(this);
      return;
    }

    case REPLICATING:
    case REPLICATED:
    {
      LOG_WITH_PREFIX_LK(FATAL) << "Cannot cancel transactions that have already replicated"
          << ": " << transaction_status_.ToString()
          << " transaction:" << ToString();
    }
  }
}

void TransactionDriver::ReplicationFinished(const Status& status) {
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
    if (status.ok()) {
      replication_state_ = REPLICATED;
    } else {
      replication_state_ = REPLICATION_FAILED;
      transaction_status_ = status;
    }
    prepare_state_copy = prepare_state_;
  }

  // If we have prepared and replicated, we're ready
  // to move ahead and apply this operation.
  // Note that if we set the state to REPLICATION_FAILED above,
  // ApplyAsync() will actually abort the transaction, i.e.
  // ApplyTask() will never be called and the transaction will never
  // be applied to the tablet.
  if (prepare_state_copy == PREPARED) {
    // We likely need to do cleanup if this fails so for now just
    // CHECK_OK
    CHECK_OK(ApplyAsync());
  }
}

void TransactionDriver::Abort(const Status& status) {
  CHECK(!status.ok());

  ReplicationState repl_state_copy;
  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    repl_state_copy = replication_state_;
    transaction_status_ = status;
  }

  // If the state is not NOT_REPLICATING we abort immediately and the transaction
  // will never be replicated.
  // In any other state we just set the transaction status, if the transaction's
  // Apply hasn't started yet this prevents it from starting, but if it has then
  // the transaction runs to completion.
  if (repl_state_copy == NOT_REPLICATING) {
    HandleFailure(status);
  }
}

Status TransactionDriver::ApplyAsync() {
  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    DCHECK_EQ(prepare_state_, PREPARED);
    if (transaction_status_.ok()) {
      DCHECK_EQ(replication_state_, REPLICATED);
      order_verifier_->CheckApply(op_id_copy_.index(),
                                  prepare_physical_timestamp_);
    } else {
      DCHECK_EQ(replication_state_, REPLICATION_FAILED);
      DCHECK(!transaction_status_.ok());
    }
  }

  return apply_pool_->SubmitClosure(Bind(&TransactionDriver::ApplyTask, Unretained(this)));
}

void TransactionDriver::ApplyTask() {
  Status s = ApplyAndTriggerCommit();
  if (!s.ok()) {
    HandleFailure(s);
  }
}

Status TransactionDriver::ApplyAndTriggerCommit() {
  ADOPT_TRACE(trace());

  Status txn_status_copy;

  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    txn_status_copy = transaction_status_;
  }

  if (!txn_status_copy.ok()) {
    HandleFailure(txn_status_copy);
    // We return Status::OK() anyways as this was not a failure to submit
    // the Apply(), likely someone called Abort();
    return Status::OK();
  }

  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    DCHECK_EQ(replication_state_, REPLICATED);
    DCHECK_EQ(prepare_state_, PREPARED);
  }

  // We need to ref-count ourself, since Commit() may run very quickly
  // and end up calling Finalize() while we're still in this code.
  scoped_refptr<TransactionDriver> ref(this);

  {
    gscoped_ptr<CommitMsg> commit_msg;
    CHECK_OK(transaction_->Apply(&commit_msg));
    commit_msg->mutable_commited_op_id()->CopyFrom(op_id_copy_);

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

    transaction_->PreCommit();
    CHECK_OK(log_->AsyncAppendCommit(commit_msg.Pass(),
                                     Bind(&TransactionDriver::CommitCallback,
                                          this)));
    transaction_->PostCommit();
  }

  return Status::OK();
}

void TransactionDriver::CommitCallback(const Status& s) {
  if (s.ok()) {
    Finalize();
  } else {
    HandleFailure(s);
  }
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
  transaction_->Finish(Transaction::COMMITTED);
  mutable_state()->completion_callback()->TransactionCompleted();
  txn_tracker_->Release(this);
}


std::string TransactionDriver::StateString(ReplicationState repl_state,
                                           PrepareState prep_state) {
  string state_str;
  switch (repl_state) {
    case NOT_REPLICATING:
      StrAppend(&state_str, "NR-"); // For Not Replicating
      break;
    case REPLICATING:
      StrAppend(&state_str, "R-"); // For Replicating
      break;
    case REPLICATION_FAILED:
      StrAppend(&state_str, "RF-"); // For Replication Failed
      break;
    case REPLICATED:
      StrAppend(&state_str, "RD-"); // For Replication Done
      break;
    default:
      LOG(DFATAL) << "Unexpected replication state: " << repl_state;
  }
  switch (prep_state) {
    case PREPARED:
      StrAppend(&state_str, "P");
      break;
    case NOT_PREPARED:
      StrAppend(&state_str, "NP");
      break;
    default:
      LOG(DFATAL) << "Unexpected prepare state: " << prep_state;
  }
  return state_str;
}

std::string TransactionDriver::LogPrefix() const {

  ReplicationState repl_state_copy;
  PrepareState prep_state_copy;
  string ts_string;

  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    repl_state_copy = replication_state_;
    prep_state_copy = prepare_state_;
    ts_string = state()->has_timestamp() ? state()->timestamp().ToString() : "No timestamp";
  }

  string state_str = StateString(repl_state_copy, prep_state_copy);
  // We use the tablet and the peer (T, P) to identify ts and tablet and the timestamp (Ts) to
  // (help) identify the transaction. The state string (S) describes the state of the transaction.
  return strings::Substitute("T $0 P $1 S $2 Ts $3: ",
                             consensus_->tablet_id(),
                             consensus_->peer_uuid(),
                             state_str,
                             ts_string);
}

}  // namespace tablet
}  // namespace kudu
