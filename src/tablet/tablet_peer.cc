// Copyright (c) 2013, Cloudera, inc.

#include "tablet/tablet_peer.h"

#include "consensus/local_consensus.h"
#include "gutil/strings/substitute.h"
#include "tablet/tasks/tasks.h"
#include "tablet/tablet_metrics.h"
#include "util/metrics.h"

namespace kudu {
namespace tablet {

using base::subtle::Barrier_AtomicIncrement;
using consensus::CommitMsg;
using consensus::ConsensusOptions;
using consensus::ConsensusContext;
using consensus::LocalConsensus;
using consensus::OpId;
using consensus::ReplicateMsg;
using consensus::OP_ABORT;
using consensus::WRITE_OP;
using log::Log;
using log::LogOptions;
using tserver::TabletServerErrorPB;


// ============================================================================
//  Execution helpers
// ============================================================================

// The callback that triggers ApplyTask to be submitted.
// This callback is used twice, once for Log (through consensus) and once for
// Prepare (through the prepare_executor_ in TabletPeer). So only after
// the second call to OnSuccess() is the Apply task started.
class ApplyOnReplicateAndPrepareCB : public FutureCallback {
 public:
  explicit ApplyOnReplicateAndPrepareCB(TaskExecutor* apply_executor)
    : num_calls_(0), apply_executor_(apply_executor) {
  }

  void OnSuccess();
  void OnFailure(const Status& status);

  // When this callback goes out of scope we must check that it's been called twice,
  // failing the transaction otherwise.
  virtual ~ApplyOnReplicateAndPrepareCB() {}

 protected:
  virtual Task *CreateApplyTask() = 0;

  // On failure one of several things might have happened:
  // 1 - None of Prepare or Append were submitted.
  // 2 - Append was submitted but Prepare failed to submit.
  // 3 - Both Append and Prepare were submitted but one of them failed
  //     afterwards.
  // 4 - Both Append and Prepare succeeded but submitting Apply failed.
  //
  // In case 1 this callback does the cleanup and answers the client. In cases
  // 2,3,4, this callback submits a commit abort message to consensus and quits.
  // The commit callback will answer the client later on.
  virtual void HandleFailure(const Status& status) = 0;

  bool is_completed() const { return num_calls_ >= 2; }
  const Status& status() const { return status_; }

 private:
  Atomic32 num_calls_;
  TaskExecutor* apply_executor_;
  Status status_;
};

void ApplyOnReplicateAndPrepareCB::OnSuccess() {
  // Atomically increase the number of calls. It doesn't matter whether Log
  // or Prepare finished first, we can only proceed when both are done.
  int num_tasks_finished = Barrier_AtomicIncrement(&num_calls_, 1);
  if (num_tasks_finished < 2) {
    // Still waiting on the other task.
    return;
  }
  CHECK_EQ(2, num_tasks_finished);

  if (!status_.ok()) {
    HandleFailure(status_);
    return;
  }

  shared_ptr<Task> apply(CreateApplyTask());
  shared_ptr<Future> future;
  Status s = apply_executor_->Submit(apply, &future);
  if (!s.ok()) {
    OnFailure(s);
  }
}

void ApplyOnReplicateAndPrepareCB::OnFailure(const Status& status) {
  status_ = status;
  int num_tasks_finished = Barrier_AtomicIncrement(&num_calls_, 1);
  if (num_tasks_finished < 2) {
    // Still waiting on the other task.
    return;
  }
  HandleFailure(status_);
}

// ============================================================================
//  Write related helpers
// ============================================================================
class ApplyWriteOnReplicateAndPrepareCB : public ApplyOnReplicateAndPrepareCB {
 public:
  ApplyWriteOnReplicateAndPrepareCB(WriteTransactionContext* tx_ctx,
                                    TaskExecutor* apply_executor)
    : ApplyOnReplicateAndPrepareCB(apply_executor), tx_ctx_(tx_ctx) {
  }
  virtual ~ApplyWriteOnReplicateAndPrepareCB();

  Task *CreateApplyTask() {
    return new ApplyWriteTask(tx_ctx_);
  }

  void HandleFailure(const Status& status);

 private:
  WriteTransactionContext* tx_ctx_;
};

ApplyWriteOnReplicateAndPrepareCB::~ApplyWriteOnReplicateAndPrepareCB() {
  if (!is_completed()) {
    Status s = status();
    if (s.ok()) {
      s = Status::IllegalState(strings::Substitute(
          "Consensus::Append()/PrepareTask callback was only called once for request: $0.",
          tx_ctx_->request()->DebugString()));
    }
    HandleFailure(s);
  }
}

void ApplyWriteOnReplicateAndPrepareCB::HandleFailure(const Status& status) {
  DCHECK(!status.ok());
  // If there is no consensus context nothing got done so just reply to the client.
  if (tx_ctx_->consensus_ctx() == NULL) {
    if (tx_ctx_->rpc_context() != NULL) {
      tx_ctx_->rpc_context()->RespondFailure(status);
    }
    return;
  }

  // if there is no error in the response, set it.
  if (!tx_ctx_->response()->has_error()) {
    TabletServerErrorPB* error = tx_ctx_->response()->mutable_error();
    StatusToPB(status, error->mutable_status());
    error->set_code(TabletServerErrorPB::UNKNOWN_ERROR);
  }

  // Release all row locks (no effect if no locks were acquired).
  tx_ctx_->release_row_locks();

  // ConsensusContext will own this pointer and dispose of it when it is no longer
  // required.
  gscoped_ptr<CommitMsg> commit(new CommitMsg());
  commit->set_op_type(OP_ABORT);
  commit->mutable_write_response()->CopyFrom(*tx_ctx_->response());
  commit->mutable_result()->CopyFrom(tx_ctx_->Result());
  tx_ctx_->consensus_ctx()->Commit(commit.Pass());
}

// Callback that commits the mvcc transaction, responds to the client and
// performs cleanup.
class WriteCommitCallback : public FutureCallback {
 public:
  explicit WriteCommitCallback(WriteTransactionContext *tx_ctx)
    : tx_ctx_(tx_ctx) {
  }

  void OnSuccess();
  void OnFailure(const Status &status);

 private:
  gscoped_ptr<WriteTransactionContext> tx_ctx_;

  DISALLOW_COPY_AND_ASSIGN(WriteCommitCallback);
};

void WriteCommitCallback::OnSuccess() {
  // Now that all of the changes have been applied and the commit is durable
  // make the changes visible to readers.
  tx_ctx_->commit();
  if (PREDICT_TRUE(tx_ctx_->rpc_context() != NULL)) {
    tx_ctx_->rpc_context()->RespondSuccess();
  }

  TabletMetrics* metrics = tx_ctx_->tablet_peer()->tablet()->metrics();
  // Update tablet server metrics.
  if (metrics) {
    // TODO: should we change this so it's actually incremented by the
    // Tablet code itself instead of this wrapper code?
    metrics->rows_inserted->IncrementBy(tx_ctx_->metrics().successful_inserts);
    metrics->rows_updated->IncrementBy(tx_ctx_->metrics().successful_updates);
  }
}

void WriteCommitCallback::OnFailure(const Status &status) {
  // no transaction should have been started.
  CHECK_EQ(tx_ctx_->mvcc_txid(), txid_t::kInvalidTxId);
  //TODO use an application level error status here with better error details.
  if (PREDICT_TRUE(tx_ctx_->rpc_context() != NULL)) {
    tx_ctx_->rpc_context()->RespondFailure(status);
  }
}

// ============================================================================
//  Tablet Peer
// ============================================================================
TabletPeer::TabletPeer(const shared_ptr<Tablet>& tablet,
                       gscoped_ptr<Log> log)
    : tablet_(tablet),
      log_(log.Pass()),
      // prepare executor has a single thread as prepare must be done in order
      // of submission
      prepare_executor_(TaskExecutor::CreateNew(1)) {
  DCHECK(tablet_) << "A TabletPeer must be provided with a Tablet";
  DCHECK(log_) << "A TabletPeer must be provided with a Log";

  errno = 0;
  int n_cpus = sysconf(_SC_NPROCESSORS_CONF);
  CHECK_EQ(errno, 0) << ErrnoToString(errno);
  CHECK_GT(n_cpus, 0);
  apply_executor_.reset(TaskExecutor::CreateNew(n_cpus));
}

// TODO a distributed implementation of consensus will need to receive the
// configuration before Init().
Status TabletPeer::Init() {

  // TODO support different consensus implementations (possibly by adding
  // a TabletPeerOptions).
  consensus_.reset(new LocalConsensus(ConsensusOptions(), log_.get()));

  // set consensus on the tablet to that it can store local state changes
  // in the log.
  tablet_->SetConsensus(consensus_.get());
  return Status::OK();
}

Status TabletPeer::Start() {
  // just return OK since we're only using LocalConsensus.
  return Status::OK();
}

Status TabletPeer::Shutdown() {
  Status s = consensus_->Shutdown();
  if (!s.ok()) {
    LOG(WARNING) << "Consensus shutdown failed: " << s.ToString();
  }
  prepare_executor_->Shutdown();
  apply_executor_->Shutdown();
  VLOG(1) << "TablePeer: " << tablet_->metadata()->oid() << " Shutdown!";
  return Status::OK();
}

Status TabletPeer::ExecuteTransaction(TransactionContext *tx_ctx,
                                      gscoped_ptr<ReplicateMsg> replicate_msg,
                                      const shared_ptr<Task>& prepare_task,
                                      const shared_ptr<FutureCallback>& apply_clbk,
                                      const shared_ptr<FutureCallback>& commit_clbk) {
  DCHECK(apply_clbk != NULL);
  DCHECK(commit_clbk != NULL);

  // The remainder of this method needs to be guarded because, for any given
  // transactions A and B, if A prepares before B on the leader, then A must
  // also replicate before B to other nodes, so that those other nodes
  // serialize the transactions in the same order that the leader does.
  // This relies on the fact that (a) the prepare_executor_ only has a single
  // worker thread, and (b) that Consensus::Append calls do not get reordered
  // internally in the consensus implementation.
  boost::lock_guard<LockType> l(lock_);

  gscoped_ptr<ConsensusContext> context;
  // persist the message through consensus, asynchronously
  Status s = consensus_->Append(replicate_msg.Pass(),
                                apply_clbk,
                                commit_clbk,
                                &context);
  if (!s.ok()) {
    apply_clbk.get()->OnFailure(s);
    return s;
  }

  tx_ctx->set_consensus_ctx(context.Pass());

  // submit the prepare task
  shared_ptr<Future> prepare_task_future;
  s = prepare_executor_->Submit(prepare_task, &prepare_task_future);
  if (!s.ok()) {
    apply_clbk.get()->OnFailure(s);
    return s;
  }

  prepare_task_future->AddListener(apply_clbk);
  return Status::OK();
}

Status TabletPeer::Write(WriteTransactionContext *tx_ctx) {
  // Create the replicate message that will be submitted to consensus
  gscoped_ptr<ReplicateMsg> replicate_msg(new ReplicateMsg());
  replicate_msg->set_op_type(WRITE_OP);
  replicate_msg->mutable_write_request()->CopyFrom(*tx_ctx->request());

  shared_ptr<FutureCallback> commit_clbk(new WriteCommitCallback(tx_ctx));

  // The callback for both the log and the prepare task
  shared_ptr<FutureCallback> apply_clbk(
      new ApplyWriteOnReplicateAndPrepareCB(tx_ctx, apply_executor_.get()));

  shared_ptr<Task> prepare_task(new PrepareWriteTask(tx_ctx));

  return ExecuteTransaction(tx_ctx, replicate_msg.Pass(), prepare_task, apply_clbk, commit_clbk);
}

}  // namespace tablet
}  // namespace kudu
