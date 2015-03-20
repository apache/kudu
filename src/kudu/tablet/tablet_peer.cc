// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/tablet/tablet_peer.h"

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>

#include "kudu/consensus/consensus.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/local_consensus.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/mathlimits.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/tablet/transactions/transaction_driver.h"
#include "kudu/tablet/transactions/alter_schema_transaction.h"
#include "kudu/tablet/transactions/change_config_transaction.h"
#include "kudu/tablet/transactions/write_transaction.h"
#include "kudu/tablet/tablet_bootstrap.h"
#include "kudu/tablet/tablet_metrics.h"
#include "kudu/tablet/tablet_peer_mm_ops.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/trace.h"

namespace kudu {
namespace tablet {

using consensus::Consensus;
using consensus::ConsensusBootstrapInfo;
using consensus::ConsensusMetadata;
using consensus::ConsensusOptions;
using consensus::ConsensusRound;
using consensus::LocalConsensus;
using consensus::OpId;
using consensus::RaftConsensus;
using consensus::CHANGE_CONFIG_OP;
using consensus::WRITE_OP;
using log::Log;
using log::LogAnchorRegistry;
using metadata::QuorumPB;
using metadata::QuorumPeerPB;
using rpc::Messenger;
using strings::Substitute;
using tserver::TabletServerErrorPB;

// ============================================================================
//  Tablet Peer
// ============================================================================
TabletPeer::TabletPeer(const scoped_refptr<TabletMetadata>& meta,
                       ThreadPool* leader_apply_pool,
                       ThreadPool* replica_apply_pool,
                       MarkDirtyCallback mark_dirty_clbk)
  : meta_(meta),
    tablet_id_(meta->oid()),
    state_(BOOTSTRAPPING),
    status_listener_(new TabletStatusListener(meta)),
    leader_apply_pool_(leader_apply_pool),
    replica_apply_pool_(replica_apply_pool),
    // prepare executor has a single thread as prepare must be done in order
    // of submission
    consensus_ready_latch_(1),
    log_anchor_registry_(new LogAnchorRegistry()),
    mark_dirty_clbk_(mark_dirty_clbk),
    config_sem_(1) {
  CHECK_OK(ThreadPoolBuilder("prepare").set_max_threads(1).Build(&prepare_pool_));
}

TabletPeer::~TabletPeer() {
  UnregisterMaintenanceOps();
  boost::lock_guard<simple_spinlock> lock(lock_);
  CHECK_EQ(state_, SHUTDOWN);
}

Status TabletPeer::Init(const shared_ptr<Tablet>& tablet,
                        const scoped_refptr<server::Clock>& clock,
                        const shared_ptr<Messenger>& messenger,
                        const scoped_refptr<Log>& log,
                        const MetricContext& metric_ctx) {

  DCHECK(tablet) << "A TabletPeer must be provided with a Tablet";
  DCHECK(log) << "A TabletPeer must be provided with a Log";

  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    CHECK_EQ(state_, BOOTSTRAPPING);
    tablet_ = tablet;
    clock_ = clock;
    messenger_ = messenger;
    log_ = log;

    ConsensusOptions options;
    options.tablet_id = meta_->oid();

    TRACE("Creating consensus instance");

    gscoped_ptr<ConsensusMetadata> cmeta;
    RETURN_NOT_OK(ConsensusMetadata::Load(meta_->fs_manager(), tablet_id_, &cmeta));

    if (cmeta->pb().committed_quorum().local()) {
      consensus_.reset(new LocalConsensus(options,
                                          cmeta.Pass(),
                                          meta_->fs_manager()->uuid(),
                                          clock_,
                                          this,
                                          log_.get()));
    } else {
      consensus_ = RaftConsensus::Create(options,
                                         cmeta.Pass(),
                                         meta_->fs_manager()->uuid(),
                                         metric_ctx,
                                         clock_,
                                         this,
                                         messenger_,
                                         log_.get());
    }
  }

  if (tablet_->metrics() != NULL) {
    TRACE("Starting instrumentation");
    txn_tracker_.StartInstrumentation(*tablet_->GetMetricContext());
  }

  TRACE("TabletPeer::Init() finished");
  VLOG(2) << "T " << tablet_id() << " P " << consensus_->peer_uuid() << ": Peer Initted";
  return Status::OK();
}

Status TabletPeer::Start(const ConsensusBootstrapInfo& bootstrap_info) {

  TRACE("Starting consensus");

  VLOG(2) << "T " << tablet_id() << " P " << consensus_->peer_uuid() << ": Peer starting";

  VLOG(2) << "Quorum before starting: " << consensus_->Quorum().DebugString();

  // TODO we likely should only change the state after starting consensus
  // but if we do that a lot of tests fail. We can't include Consensus::Start()
  // with the lock either or we'd get a deadlock. We should investigate why
  // this is happening and move the state change to the right place but this
  // should be good enough for now.
  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    CHECK_EQ(state_, BOOTSTRAPPING);
    state_ = RUNNING;
  }

  RETURN_NOT_OK(consensus_->Start(bootstrap_info));

  return Status::OK();
}

const metadata::QuorumPB TabletPeer::Quorum() const {
  CHECK(consensus_) << "consensus is null";
  return consensus_->Quorum();
}

void TabletPeer::ConsensusStateChanged(metadata::QuorumPeerPB::Role new_role) {
  // We count down the running latch on the role change cuz this signals
  // when consensus is ready and other places are relying on that.
  // TODO remove this latch and make people simply retry.
  if (new_role != QuorumPeerPB::NON_PARTICIPANT) {
    consensus_ready_latch_.CountDown();
  }

  // NOTE: This callback must be called outside the peer lock or we risk
  // a deadlock.
  mark_dirty_clbk_(this);
}

TabletStatePB TabletPeer::Shutdown() {

  LOG(INFO) << "Initiating TabletPeer shutdown for tablet: " << tablet_id_;

  TabletStatePB prev_state;
  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    if (state_ == QUIESCING || state_ == SHUTDOWN) {
      return state_;
    }
    prev_state = state_;
    state_ = QUIESCING;
  }

  if (tablet_) tablet_->UnregisterMaintenanceOps();

  if (consensus_) consensus_->Shutdown();

  // TODO: KUDU-183: Keep track of the pending tasks and send an "abort" message.
  LOG_SLOW_EXECUTION(WARNING, 1000,
      Substitute("TabletPeer: tablet $0: Waiting for Transactions to complete", tablet_id())) {
    txn_tracker_.WaitForAllToFinish();
  }

  prepare_pool_->Shutdown();

  if (log_) {
    WARN_NOT_OK(log_->Close(), "Error closing the Log.");
  }

  if (VLOG_IS_ON(1)) {
    VLOG(1) << "TabletPeer: tablet " << tablet_id() << " shut down!";
  }

  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    state_ = SHUTDOWN;
  }
  return prev_state;
}

Status TabletPeer::CheckRunning() const {
  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    if (state_ != RUNNING) {
      return Status::IllegalState(Substitute("The tablet is not in a running state: $0",
                                             TabletStatePB_Name(state_)));
    }
  }
  return Status::OK();
}

Status TabletPeer::WaitUntilConsensusRunning(const MonoDelta& delta) {
  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    if (state_ == QUIESCING || state_ == SHUTDOWN) {
      return Status::IllegalState("The tablet is already shutting down or shutdown");
    }
  }
  if (!consensus_ready_latch_.WaitFor(delta)) {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return Status::TimedOut(Substitute("The tablet is not in RUNNING state after waiting: $0",
                                       TabletStatePB_Name(state_)));
  }
  return Status::OK();
}

Status TabletPeer::SubmitWrite(WriteTransactionState *state) {
  RETURN_NOT_OK(CheckRunning());

  gscoped_ptr<WriteTransaction> transaction(new WriteTransaction(state, consensus::LEADER));
  scoped_refptr<TransactionDriver> driver;
  NewLeaderTransactionDriver(transaction.PassAs<Transaction>(), &driver);
  return driver->ExecuteAsync();
}

Status TabletPeer::SubmitAlterSchema(AlterSchemaTransactionState *state) {
  RETURN_NOT_OK(CheckRunning());

  gscoped_ptr<AlterSchemaTransaction> transaction(
      new AlterSchemaTransaction(state, consensus::LEADER));
  scoped_refptr<TransactionDriver> driver;
  NewLeaderTransactionDriver(transaction.PassAs<Transaction>(), &driver);
  return driver->ExecuteAsync();
}

Status TabletPeer::SubmitChangeConfig(ChangeConfigTransactionState *state) {
  RETURN_NOT_OK(CheckRunning());

  if (!state->old_quorum().IsInitialized()) {
    RETURN_NOT_OK(state->set_old_quorum(consensus_->Quorum()));
    DCHECK(state->old_quorum().IsInitialized());
  }
  gscoped_ptr<ChangeConfigTransaction> transaction(
      new ChangeConfigTransaction(state, consensus::LEADER, &config_sem_));
  scoped_refptr<TransactionDriver> driver;
  NewLeaderTransactionDriver(transaction.PassAs<Transaction>(), &driver);
  return driver->ExecuteAsync();
}

void TabletPeer::GetTabletStatusPB(TabletStatusPB* status_pb_out) const {
  boost::lock_guard<simple_spinlock> lock(lock_);
  DCHECK(status_pb_out != NULL);
  DCHECK(status_listener_.get() != NULL);
  status_pb_out->set_tablet_id(status_listener_->tablet_id());
  status_pb_out->set_table_name(status_listener_->table_name());
  status_pb_out->set_last_status(status_listener_->last_status());
  status_pb_out->set_start_key(status_listener_->start_key());
  status_pb_out->set_end_key(status_listener_->end_key());
  status_pb_out->set_state(state_);
  if (tablet_) {
    status_pb_out->set_estimated_on_disk_size(tablet_->EstimateOnDiskSize());
  }
}

Status TabletPeer::RunLogGC() {
  if (!CheckRunning().ok()) {
    return Status::OK();
  }
  int64_t min_log_index;
  int32_t num_gced;
  GetEarliestNeededLogIndex(&min_log_index);
  Status s = log_->GC(min_log_index, &num_gced);
  if (!s.ok()) {
    s = s.CloneAndPrepend("Unexpected error while running Log GC from TabletPeer");
    LOG(ERROR) << s.ToString();
  }
  return Status::OK();
}

void TabletPeer::GetInFlightTransactions(Transaction::TraceType trace_type,
                                         vector<consensus::TransactionStatusPB>* out) const {
  vector<scoped_refptr<TransactionDriver> > pending_transactions;
  txn_tracker_.GetPendingTransactions(&pending_transactions);
  BOOST_FOREACH(const scoped_refptr<TransactionDriver>& driver, pending_transactions) {
    if (driver->state() != NULL) {
      consensus::TransactionStatusPB status_pb;
      status_pb.mutable_op_id()->CopyFrom(driver->GetOpId());
      switch (driver->tx_type()) {
        case Transaction::WRITE_TXN:
          status_pb.set_tx_type(consensus::WRITE_OP);
          break;
        case Transaction::ALTER_SCHEMA_TXN:
          status_pb.set_tx_type(consensus::ALTER_SCHEMA_OP);
          break;
        case Transaction::CHANGE_CONFIG_TXN:
          status_pb.set_tx_type(consensus::CHANGE_CONFIG_OP);
          break;
      }
      status_pb.set_description(driver->ToString());
      int64_t running_for_micros =
          MonoTime::Now(MonoTime::FINE).GetDeltaSince(driver->start_time()).ToMicroseconds();
      status_pb.set_running_for_micros(running_for_micros);
      if (trace_type == Transaction::TRACE_TXNS) {
        status_pb.set_trace_buffer(driver->trace()->DumpToString(true));
      }
      out->push_back(status_pb);
    }
  }
}

void TabletPeer::GetEarliestNeededLogIndex(int64_t* min_index) const {
  // First, we anchor on the last OpId in the Log to establish a lower bound
  // and avoid racing with the other checks. This limits the Log GC candidate
  // segments before we check the anchors.
  {
    OpId last_log_op;
    Status s = log_->GetLastEntryOpId(&last_log_op);
    // First check if we ever wrote to the log.
    if (s.IsNotFound()) {
      *min_index = 0;
      return;
    }
    CHECK_OK(s);
    *min_index = last_log_op.index();
  }

  // Next, we interrogate the anchor registry.
  // Returns OK if minimum known, NotFound if no anchors are registered.
  {
    int64_t min_anchor_index;
    Status s = log_anchor_registry_->GetEarliestRegisteredLogIndex(&min_anchor_index);
    if (PREDICT_FALSE(!s.ok())) {
      DCHECK(s.IsNotFound()) << "Unexpected error calling LogAnchorRegistry: " << s.ToString();
    } else {
      *min_index = std::min(*min_index, min_anchor_index);
    }
  }

  // Next, interrogate the TransactionTracker.
  vector<scoped_refptr<TransactionDriver> > pending_transactions;
  txn_tracker_.GetPendingTransactions(&pending_transactions);
  BOOST_FOREACH(const scoped_refptr<TransactionDriver>& driver, pending_transactions) {
    OpId tx_op_id = driver->GetOpId();
    // A transaction which doesn't have an opid hasn't been submitted for replication yet and
    // thus has no need to anchor the log.
    if (tx_op_id.IsInitialized()) {
      *min_index = std::min(*min_index, tx_op_id.index());
    }
  }
}

Status TabletPeer::GetMaxIndexesToSegmentSizeMap(MaxIdxToSegmentSizeMap* idx_size_map) const {
  RETURN_NOT_OK(CheckRunning());
  int64_t min_op_idx;
  GetEarliestNeededLogIndex(&min_op_idx);
  log_->GetMaxIndexesToSegmentSizeMap(min_op_idx, idx_size_map);
  return Status::OK();
}

Status TabletPeer::GetGCableDataSize(int64_t* retention_size) const {
  RETURN_NOT_OK(CheckRunning());
  int64_t min_op_idx;
  GetEarliestNeededLogIndex(&min_op_idx);
  log_->GetGCableDataSize(min_op_idx, retention_size);
  return Status::OK();
}

Status TabletPeer::StartReplicaTransaction(gscoped_ptr<ConsensusRound> round) {
  RETURN_NOT_OK(CheckRunning());

  consensus::ReplicateMsg* replicate_msg = round->replicate_msg();
  DCHECK(replicate_msg->has_timestamp());
  gscoped_ptr<Transaction> transaction;
  switch (replicate_msg->op_type()) {
    case WRITE_OP:
    {
      DCHECK(replicate_msg->has_write_request()) << "WRITE_OP replica"
          " transaction must receive a WriteRequestPB";
      transaction.reset(new WriteTransaction(
          new WriteTransactionState(this, &replicate_msg->write_request()),
          consensus::REPLICA));
      break;
    }
    case CHANGE_CONFIG_OP:
    {
      DCHECK(replicate_msg->has_change_config_request()) << "CHANGE_CONFIG_OP"
          " replica transaction must receive a ChangeConfigRequestPB";
      DCHECK(replicate_msg->change_config_request().old_config().IsInitialized());
      ChangeConfigTransactionState* state = new ChangeConfigTransactionState(
          this,
          &replicate_msg->change_config_request());
      RETURN_NOT_OK(state->set_old_quorum(replicate_msg->change_config_request().old_config()));
      transaction.reset(new ChangeConfigTransaction(state, consensus::REPLICA, &config_sem_));
      break;
    }
    default:
      LOG(FATAL) << "Unsupported Operation Type";
  }

  // TODO(todd) Look at wiring the stuff below on the driver
  TransactionState* state = transaction->state();
  state->set_consensus_round(round.Pass());
  Timestamp ts(replicate_msg->timestamp());
  state->set_timestamp(ts);
  clock_->Update(ts);

  scoped_refptr<TransactionDriver> driver;
  NewReplicaTransactionDriver(transaction.Pass(), &driver);
  // FIXME: Bare ptr is a hack for a ref-counted object.
  state->consensus_round()->SetReplicaCommitContinuation(driver.get());

  RETURN_NOT_OK(driver->ExecuteAsync());
  return Status::OK();
}

void TabletPeer::NewLeaderTransactionDriver(gscoped_ptr<Transaction> transaction,
                                            scoped_refptr<TransactionDriver>* driver) {
  scoped_refptr<TransactionDriver> tx_driver = new TransactionDriver(
    &txn_tracker_,
    consensus_.get(),
    log_.get(),
    prepare_pool_.get(),
    leader_apply_pool_,
    &txn_order_verifier_);
  tx_driver->Init(transaction.Pass(), consensus::LEADER);
  driver->swap(tx_driver);
}

void TabletPeer::NewReplicaTransactionDriver(gscoped_ptr<Transaction> transaction,
                                             scoped_refptr<TransactionDriver>* driver) {
  scoped_refptr<TransactionDriver> tx_driver = new TransactionDriver(
    &txn_tracker_,
    consensus_.get(),
    log_.get(),
    prepare_pool_.get(),
    leader_apply_pool_,
    &txn_order_verifier_);
  tx_driver->Init(transaction.Pass(), consensus::REPLICA);
  driver->swap(tx_driver);
}

void TabletPeer::RegisterMaintenanceOps(MaintenanceManager* maint_mgr) {
  DCHECK(maintenance_ops_.empty());

  gscoped_ptr<MaintenanceOp> mrs_flush_op(new FlushMRSOp(this));
  maint_mgr->RegisterOp(mrs_flush_op.get());
  maintenance_ops_.push_back(mrs_flush_op.release());

  gscoped_ptr<MaintenanceOp> dms_flush_op(new FlushDeltaMemStoresOp(this));
  maint_mgr->RegisterOp(dms_flush_op.get());
  maintenance_ops_.push_back(dms_flush_op.release());

  gscoped_ptr<MaintenanceOp> log_gc(new LogGCOp(this));
  maint_mgr->RegisterOp(log_gc.get());
  maintenance_ops_.push_back(log_gc.release());

  tablet_->RegisterMaintenanceOps(maint_mgr);
}

void TabletPeer::UnregisterMaintenanceOps() {
  BOOST_FOREACH(MaintenanceOp* op, maintenance_ops_) {
    op->Unregister();
  }
  STLDeleteElements(&maintenance_ops_);
}

Status FlushInflightsToLogCallback::WaitForInflightsAndFlushLog() {
  VLOG(1) << "T " << tablet_->metadata()->oid()
      <<  ": Waiting for in-flight transactions to commit.";
  LOG_SLOW_EXECUTION(WARNING, 200, "Committing in-flights took a long time.") {
    tablet_->mvcc_manager()->WaitForAllInFlightToCommit();
  }
  VLOG(1) << "T " << tablet_->metadata()->oid()
      << ": Waiting for the log queue to be flushed.";
  LOG_SLOW_EXECUTION(WARNING, 200, "Flushing the Log queue took a long time.") {
    RETURN_NOT_OK(log_->WaitUntilAllFlushed());
  }
  return Status::OK();
}


}  // namespace tablet
}  // namespace kudu
