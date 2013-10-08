// Copyright (c) 2013, Cloudera, inc.

#include "tablet/tablet_peer.h"

#include "consensus/local_consensus.h"
#include "gutil/strings/substitute.h"
#include "tablet/tasks/tasks.h"
#include "util/metrics.h"

// Tablet-specific metrics.
METRIC_DEFINE_counter(rows_inserted, kudu::MetricUnit::kRows,
    "Number of rows inserted into this tablet since service start");
METRIC_DEFINE_counter(rows_updated, kudu::MetricUnit::kRows,
    "Number of row update operations performed on this tablet since service start");

namespace kudu {
namespace tablet {

using base::subtle::Barrier_AtomicIncrement;
using consensus::CommitMsg;
using consensus::ConsensusOptions;
using consensus::ConsensusContext;
using consensus::LocalConsensus;
using consensus::OpId;
using consensus::ReplicateMsg;
using consensus::WRITE_OP;
using consensus::WRITE_ABORT;
using log::Log;
using log::LogOptions;
using tserver::TabletServerErrorPB;

TabletMetrics::TabletMetrics(const MetricContext& metric_ctx)
  : rows_inserted(FindOrCreateCounter(metric_ctx, METRIC_rows_inserted)),
    rows_updated(FindOrCreateCounter(metric_ctx, METRIC_rows_updated)) {
}

TabletPeer::TabletPeer(const shared_ptr<Tablet>& tablet, const MetricContext& metric_ctx)
  : tablet_(tablet),
    prepare_executor_(TaskExecutor::CreateNew(1)),
    metric_ctx_(metric_ctx, strings::Substitute("tablet.tablet-$0", tablet_->tablet_id())),
    tablet_metrics_(metric_ctx_) {

  // prepare executor has a single thread as prepare must be done in order
  // of submission
  errno = 0;
  int n_cpus = sysconf(_SC_NPROCESSORS_CONF);
  CHECK_EQ(errno, 0) << ErrnoToString(errno);
  CHECK_GT(n_cpus, 0);
  apply_executor_.reset(TaskExecutor::CreateNew(n_cpus));
}

// TODO a distributed implementation of consensus will need to receive the
// configuration before Init().
Status TabletPeer::Init() {
  OpId initial_id;
  initial_id.set_term(0);
  initial_id.set_index(0);

  shared_ptr<metadata::TabletSuperBlockPB> super_block;
  RETURN_NOT_OK(tablet_->metadata()->ToSuperBlock(&super_block));

  gscoped_ptr<Log> log;
  RETURN_NOT_OK(Log::Open(LogOptions(),
                          tablet_->metadata()->fs_manager(),
                          *super_block,
                          initial_id,
                          &log));
  log_.reset(log.release());

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

Status TabletPeer::Write(TransactionContext *tx_ctx) {

  // Create the replicate message that will be submitted to consensus
  gscoped_ptr<ReplicateMsg> replicate_msg(new ReplicateMsg());
  replicate_msg->set_op_type(WRITE_OP);
  replicate_msg->mutable_write()->CopyFrom(*tx_ctx->request());

  shared_ptr<FutureCallback> commit_clbk(new CommitCallback(tx_ctx, tablet_metrics_));

  // The callback for both the log and the prepare task
  shared_ptr<FutureCallback> apply_clbk(
      new ApplyOnReplicateAndPrepareCB(tx_ctx, apply_executor_.get()));

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
  shared_ptr<PrepareTask> prepare_task(new PrepareTask(tx_ctx));
  shared_ptr<Future> prepare_task_future;
  s = prepare_executor_->Submit(prepare_task, &prepare_task_future);

  if (!s.ok()) {
    apply_clbk.get()->OnFailure(s);
    return s;
  }

  prepare_task_future->AddListener(apply_clbk);
  return Status::OK();
}

ApplyOnReplicateAndPrepareCB::ApplyOnReplicateAndPrepareCB(
    TransactionContext* tx_ctx,
    TaskExecutor* apply_executor)
    : num_calls_(0),
      tx_ctx_(tx_ctx),
      apply_executor_(apply_executor) {
}

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
    HandleFailure();
    return;
  }

  shared_ptr<ApplyTask> apply(new ApplyTask(tx_ctx_));
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
  HandleFailure();
}

ApplyOnReplicateAndPrepareCB::~ApplyOnReplicateAndPrepareCB() {
  // If for some reason this callback goes out of scope without having been
  // called twice, fail.
  if (num_calls_ < 2) {
    if (status_.ok()) {
      // TODO add a DebugString() method to TabletContext and improve the message
      status_ = Status::IllegalState(strings::Substitute(
          "Consensus::Append()/PrepareTask callback was only called once for request: $0.",
          tx_ctx_->request()->DebugString()));
    }
    HandleFailure();
  }
}

void ApplyOnReplicateAndPrepareCB::HandleFailure() {
  DCHECK(!status_.ok());
  // If there is no consensus context nothing got done so just reply to the
  // client.
  if (tx_ctx_->consensus_ctx() == NULL) {
    if (tx_ctx_->rpc_context() != NULL) {
        tx_ctx_->rpc_context()->RespondFailure(status_);
    }
    return;
  }

  // if there is no error in the response, set it.
  if (!tx_ctx_->response()->has_error()) {
    TabletServerErrorPB* error = tx_ctx_->response()->mutable_error();
    StatusToPB(status_, error->mutable_status());
    error->set_code(TabletServerErrorPB::UNKNOWN_ERROR);
  }

  // Release all row locks (no effect if no locks were acquired).
  tx_ctx_->release_row_locks();

  // ConsensusContext will own this pointer and dispose of it when it is no longer
  // required.
  gscoped_ptr<CommitMsg> commit(new CommitMsg());
  commit->set_op_type(WRITE_ABORT);
  commit->mutable_response()->CopyFrom(*tx_ctx_->response());
  commit->mutable_result()->CopyFrom(tx_ctx_->Result());
  tx_ctx_->consensus_ctx()->Commit(commit.Pass());
}

CommitCallback::CommitCallback(TransactionContext* tx_ctx, const TabletMetrics& metrics)
    : tx_ctx_(tx_ctx),
      tablet_metrics_(metrics) {
}

void CommitCallback::OnSuccess() {
  // Now that all of the changes have been applied and the commit is durable
  // make the changes visible to readers.
  tx_ctx_->commit();
  if (PREDICT_TRUE(tx_ctx_->rpc_context() != NULL)) {
    tx_ctx_->rpc_context()->RespondSuccess();
  }

  // Update tablet server metrics.
  tablet_metrics_.rows_inserted->IncrementBy(tx_ctx_->metrics().successful_inserts);
  tablet_metrics_.rows_updated->IncrementBy(tx_ctx_->metrics().successful_updates);
}

void CommitCallback::OnFailure(const Status &status) {
  // no transaction should have been started.
  CHECK_EQ(tx_ctx_->mvcc_txid(), txid_t::kInvalidTxId);
  //TODO use an application level error status here with better error details.
  if (PREDICT_TRUE(tx_ctx_->rpc_context() != NULL)) {
    tx_ctx_->rpc_context()->RespondFailure(status);
  }
}

}  // namespace tablet
}  // namespace kudu
