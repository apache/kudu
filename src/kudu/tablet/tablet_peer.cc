// Copyright (c) 2013, Cloudera, inc.

#include "kudu/tablet/tablet_peer.h"

#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>

#include "kudu/consensus/consensus.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/local_consensus.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/opid_anchor_registry.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/tablet/transactions/transaction_driver.h"
#include "kudu/tablet/transactions/alter_schema_transaction.h"
#include "kudu/tablet/transactions/change_config_transaction.h"
#include "kudu/tablet/transactions/write_transaction.h"
#include "kudu/tablet/tablet_metrics.h"
#include "kudu/tablet/tablet_bootstrap.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/trace.h"

DEFINE_bool(enable_log_gc, true,
    "Enable garbage collection (deletion) of old write-ahead logs.");

DEFINE_int32(log_gc_sleep_delay_ms, 10000,
    "Number of milliseconds that each Log GC thread will sleep between runs.");

namespace kudu {
namespace tablet {

using consensus::Consensus;
using consensus::ConsensusBootstrapInfo;
using consensus::ConsensusRound;
using consensus::ConsensusOptions;
using consensus::LocalConsensus;
using consensus::OpId;
using consensus::RaftConsensus;
using consensus::CHANGE_CONFIG_OP;
using consensus::WRITE_OP;
using consensus::OP_ABORT;
using log::Log;
using log::OpIdAnchorRegistry;
using metadata::QuorumPB;
using metadata::QuorumPeerPB;
using metadata::TabletMetadata;
using rpc::Messenger;
using strings::Substitute;
using tserver::TabletServerErrorPB;

// ============================================================================
//  Tablet Peer
// ============================================================================
TabletPeer::TabletPeer(const scoped_refptr<TabletMetadata>& meta,
                       const QuorumPeerPB& quorum_peer,
                       TaskExecutor* leader_apply_executor,
                       TaskExecutor* replica_apply_executor,
                       MarkDirtyCallback mark_dirty_clbk)
  : meta_(meta),
    tablet_id_(meta->oid()),
    state_(metadata::BOOTSTRAPPING),
    quorum_peer_(quorum_peer),
    status_listener_(new TabletStatusListener(meta)),
    leader_apply_executor_(leader_apply_executor),
    replica_apply_executor_(replica_apply_executor),
    // prepare executor has a single thread as prepare must be done in order
    // of submission
    log_gc_shutdown_latch_(1),
    tablet_running_latch_(1),
    mark_dirty_clbk_(mark_dirty_clbk),
    config_sem_(1) {
  CHECK_OK(TaskExecutorBuilder("prepare").set_max_threads(1).Build(&prepare_executor_));
  CHECK_OK(TaskExecutorBuilder("log-gc").set_max_threads(1).Build(&log_gc_executor_));
}

TabletPeer::~TabletPeer() {
  boost::lock_guard<simple_spinlock> lock(lock_);
  CHECK_EQ(state_, metadata::SHUTDOWN);
}

Status TabletPeer::Init(const shared_ptr<Tablet>& tablet,
                        const scoped_refptr<server::Clock>& clock,
                        const shared_ptr<Messenger>& messenger,
                        gscoped_ptr<Log> log,
                        const MetricContext& metric_ctx) {
  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    CHECK_EQ(state_, metadata::BOOTSTRAPPING);
    state_ = metadata::CONFIGURING;
    tablet_ = tablet;
    clock_ = clock;
    messenger_ = messenger;
    log_.reset(log.release());

    ConsensusOptions options;
    options.tablet_id = meta_->oid();

    TRACE("Creating consensus instance");
    if (tablet_->metadata()->Quorum().local()) {
      consensus_.reset(new LocalConsensus(options));
    } else {
      gscoped_ptr<consensus::PeerProxyFactory> rpc_factory(
          new consensus::RpcPeerProxyFactory(messenger_));
      consensus_.reset(new RaftConsensus(options,  rpc_factory.Pass(), metric_ctx));
    }
  }

  DCHECK(tablet_) << "A TabletPeer must be provided with a Tablet";
  DCHECK(log_) << "A TabletPeer must be provided with a Log";

  TRACE("Initting consensus impl");
  RETURN_NOT_OK_PREPEND(consensus_->Init(quorum_peer_, clock_, this, log_.get()),
                        "Could not initialize consensus");

  if (tablet_->metrics() != NULL) {
    TRACE("Starting instrumentation");
    txn_tracker_.StartInstrumentation(*tablet_->GetMetricContext());
  }

  TRACE("TabletPeer::Init() finished");
  return Status::OK();
}

// TODO move this to raft_consensus.cc
Status TabletPeer::UpdatePermanentUuids() {
  DCHECK(messenger_.get() != NULL);
  QuorumPB config = meta_->Quorum();
  bool altered = false;
  BOOST_FOREACH(QuorumPeerPB& peer, *config.mutable_peers()) {
    if (!peer.has_permanent_uuid()) {
      LOG(INFO) << peer.ShortDebugString()
                << " has no permanent_uuid. Determining permanent_uuid...";
      RETURN_NOT_OK(consensus::SetPermanentUuidForRemotePeer(messenger_, &peer));
      altered = true;
    }
  }
  if (altered) {
    meta_->SetQuorum(config);
    RETURN_NOT_OK(meta_->Flush());
  }
  return Status::OK();
}

Status TabletPeer::Start(const ConsensusBootstrapInfo& bootstrap_info) {
  // Prevent any SubmitChangeConfig calls to try and modify the config
  // until consensus is booted and the actual configuration is stored in
  // the tablet meta.
  boost::lock_guard<Semaphore> config_lock(config_sem_);

  RETURN_NOT_OK(UpdatePermanentUuids());

  gscoped_ptr<QuorumPB> actual_config;
  TRACE("Starting consensus");

  QuorumPeerPB::Role my_role = consensus::GetRoleInQuorum(quorum_peer_.permanent_uuid(), Quorum());
  RETURN_NOT_OK(StartPendingTransactions(my_role, bootstrap_info));
  RETURN_NOT_OK(consensus_->Start(Quorum(), bootstrap_info.last_commit_id));

  return Status::OK();
}

// TODO KUDU-255 - handle the bootstrap info properly. In particular:
// - Pending transactions whose ids are lower than bootstrap_info.last_commit_id
//   don't need to go through consensus. We can simply trigger the apply for those.
// - Pending transactions whose ids are after the last committed operation id
//   need to start regular transactions that will succeed or fail depending on
//   who is the leader and what its state is.
Status TabletPeer::StartPendingTransactions(QuorumPeerPB::Role my_role,
                                            const ConsensusBootstrapInfo& bootstrap_info) {
  return Status::OK();
}

const metadata::QuorumPB TabletPeer::Quorum() const {
  return meta_->Quorum();
}

const metadata::QuorumPeerPB::Role TabletPeer::role() const {
  boost::lock_guard<simple_spinlock> lock(lock_);
  QuorumPB latest_config = meta_->Quorum();
  if (!latest_config.IsInitialized()) {
    return metadata::QuorumPeerPB::NON_PARTICIPANT;
  } else {
    BOOST_FOREACH(const QuorumPeerPB& peer, latest_config.peers()) {
      if (peer.permanent_uuid() == quorum_peer_.permanent_uuid()) {
        return peer.role();
      }
    }
    return metadata::QuorumPeerPB::NON_PARTICIPANT;
  }
}

Status TabletPeer::SubmitConsensusChangeConfig(gscoped_ptr<QuorumPB> quorum,
                                               const StatusCallback& callback) {
  consensus::ChangeConfigRequestPB* cc_request = new consensus::ChangeConfigRequestPB();
  cc_request->mutable_new_config()->CopyFrom(*quorum);
  cc_request->set_tablet_id(tablet_->metadata()->oid());
  ChangeConfigTransactionState* state = new ChangeConfigTransactionState(this, cc_request);
  state->AddToAutoReleasePool(cc_request);
  state->set_completion_callback(
      gscoped_ptr<TransactionCompletionCallback>(
          new StatusTransactionCompletionCallback(callback)));
  return SubmitChangeConfig(state);
}

void TabletPeer::ConsensusStateChanged(const QuorumPB& old_quorum, const QuorumPB& new_quorum) {
  QuorumPeerPB::Role new_role = consensus::GetRoleInQuorum(quorum_peer_.permanent_uuid(),
                                                           new_quorum);

  LOG(INFO) << "Configuration changed for tablet: " << tablet_id_ << " in peer: "
      << quorum_peer_.permanent_uuid() << "\nChanged from:" << old_quorum.ShortDebugString()
      << "\nChanged to:" << new_quorum.ShortDebugString();

  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    // If we were configuring and now have a state other than non participant
    // set the state to running.
    // TODO this is currently a very simple impl. for the initial state change and should
    // serve as place holder for things that need to happen at a tablet level
    // when roles change, such as:
    // - a change from FOLLOWER to LEADER might require queue coordination
    // - a change from LEADER to FOLLOWER might require emptying queues
    // - a change to NON_PARTICIPANT should likely trigger the destruction of
    //   the tablet.
    if (state_ == metadata::CONFIGURING && new_role != QuorumPeerPB::NON_PARTICIPANT) {
      CHECK_OK(StartLogGCTask());
      state_ = metadata::RUNNING;
      tablet_running_latch_.CountDown();
    }
  }

  // NOTE: This callback must be called outside the peer lock or we risk
  // a deadlock.
  mark_dirty_clbk_(this);
}

metadata::TabletStatePB TabletPeer::Shutdown() {

  LOG(INFO) << "Initiating TabletPeer shutdown for tablet: " << tablet_id_;

  metadata::TabletStatePB prev_state;
  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    if (state_ == metadata::QUIESCING || state_ == metadata::SHUTDOWN) {
      return state_;
    }
    prev_state = state_;
    state_ = metadata::QUIESCING;
  }

  if (tablet_) tablet_->UnregisterMaintenanceOps();

  // Stop Log GC thread before we close the log.
  VLOG(1) << Substitute("TabletPeer: tablet $0: Shutting down Log GC thread...", tablet_id());
  log_gc_shutdown_latch_.CountDown();
  log_gc_executor_->Shutdown();

  if (consensus_) consensus_->Shutdown();

  // TODO: KUDU-183: Keep track of the pending tasks and send an "abort" message.
  LOG_SLOW_EXECUTION(WARNING, 1000,
      Substitute("TabletPeer: tablet $0: Waiting for Transactions to complete", tablet_id())) {
    txn_tracker_.WaitForAllToFinish();
  }

  prepare_executor_->Shutdown();

  if (log_) {
    WARN_NOT_OK(log_->Close(), "Error closing the Log.");
  }

  if (VLOG_IS_ON(1)) {
    VLOG(1) << "TabletPeer: tablet " << tablet_id() << " shut down!";
  }

  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    state_ = metadata::SHUTDOWN;
  }
  return prev_state;
}

Status TabletPeer::CheckRunning() const {
  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    if (state_ != metadata::RUNNING) {
      return Status::ServiceUnavailable(Substitute("The tablet is not in a running state: $0",
                                                   metadata::TabletStatePB_Name(state_)));
    }
  }
  return Status::OK();
}

Status TabletPeer::WaitUntilRunning(const MonoDelta& delta) {
  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    if (state_ == metadata::QUIESCING || state_ == metadata::SHUTDOWN) {
      return Status::IllegalState("The tablet is already shutting down or shutdown");
    }
  }
  if (!tablet_running_latch_.WaitFor(delta)) {
    return Status::TimedOut(Substitute("The tablet is not in RUNNING state after waiting: $0",
                                       metadata::TabletStatePB_Name(state_)));
  }
  return Status::OK();
}

Status TabletPeer::SubmitWrite(WriteTransactionState *state) {
  RETURN_NOT_OK(CheckRunning());

  WriteTransaction* transaction = new WriteTransaction(state, consensus::LEADER);
  scoped_refptr<LeaderTransactionDriver> driver;
  NewLeaderTransactionDriver(transaction, &driver);
  return driver->Execute();
}

Status TabletPeer::SubmitAlterSchema(AlterSchemaTransactionState *state) {
  RETURN_NOT_OK(CheckRunning());

  AlterSchemaTransaction* transaction = new AlterSchemaTransaction(state, consensus::LEADER);
  scoped_refptr<LeaderTransactionDriver> driver;
  NewLeaderTransactionDriver(transaction, &driver);
  return driver->Execute();
}

Status TabletPeer::SubmitChangeConfig(ChangeConfigTransactionState *state) {
  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    if (state_ != metadata::RUNNING && state_ != metadata::CONFIGURING) {
      return Status::ServiceUnavailable(
          Substitute("The tablet is not in a running/configuring state: $0",
                     metadata::TabletStatePB_Name(state_)));
    }
  }

  ChangeConfigTransaction* transaction = new ChangeConfigTransaction(state,
                                                                     consensus::LEADER,
                                                                     &config_sem_);
  scoped_refptr<LeaderTransactionDriver> driver;
  NewLeaderTransactionDriver(transaction, &driver);
  return driver->Execute();
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

Status TabletPeer::StartLogGCTask() {
  if (PREDICT_FALSE(!FLAGS_enable_log_gc)) {
    KLOG_FIRST_N(INFO, 1) << "Log GC is disabled, not deleting old write-ahead logs!";
    return Status::OK();
  }
  shared_ptr<Future> future;
  return log_gc_executor_->Submit(boost::bind(&TabletPeer::RunLogGC, this), &future);
}

Status TabletPeer::RunLogGC() {
  do {
    OpId min_op_id;
    int32_t num_gced;
    GetEarliestNeededOpId(&min_op_id);
    Status s = log_->GC(min_op_id, &num_gced);
    if (!s.ok()) {
      s = s.CloneAndPrepend("Unexpected error while running Log GC from TabletPeer");
      LOG(ERROR) << s.ToString();
    }
    // TODO: Possibly back off if num_gced == 0.
  } while (!log_gc_shutdown_latch_.WaitFor(
      MonoDelta::FromMilliseconds(FLAGS_log_gc_sleep_delay_ms)));
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
      status_pb.set_driver_type(driver->type());
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

void TabletPeer::GetEarliestNeededOpId(consensus::OpId* min_op_id) const {
  min_op_id->Clear();

  // First, we anchor on the last OpId in the Log to establish a lower bound
  // and avoid racing with the other checks. This limits the Log GC candidate
  // segments before we check the anchors.
  Status s = log_->GetLastEntryOpId(min_op_id);

  // Next, we interrogate the anchor registry.
  // Returns OK if minimum known, NotFound if no anchors are registered.
  OpId min_anchor_op_id;
  s = tablet_->opid_anchor_registry()->GetEarliestRegisteredOpId(&min_anchor_op_id);
  if (PREDICT_FALSE(!s.ok())) {
    DCHECK(s.IsNotFound()) << "Unexpected error calling OpIdAnchorRegistry: " << s.ToString();
  }
  CopyIfOpIdLessThan(min_anchor_op_id, min_op_id);

  // Next, interrogate the TransactionTracker.
  vector<scoped_refptr<TransactionDriver> > pending_transactions;
  txn_tracker_.GetPendingTransactions(&pending_transactions);
  BOOST_FOREACH(const scoped_refptr<TransactionDriver>& driver, pending_transactions) {
    OpId tx_op_id = driver->GetOpId();
    CopyIfOpIdLessThan(tx_op_id, min_op_id);
  }

  // Finally, if nothing is known or registered, just don't delete anything.
  if (!min_op_id->IsInitialized()) {
    min_op_id->CopyFrom(consensus::MinimumOpId());
  }
}

bool TabletPeer::IsOpTypeAllowedInState(consensus::OperationType type,
                                        metadata::TabletStatePB state) {
  if (state == metadata::RUNNING) return true;
  if (state == metadata::CONFIGURING) {
    return type == CHANGE_CONFIG_OP;
  }
  return false;
}

Status TabletPeer::StartReplicaTransaction(gscoped_ptr<ConsensusRound> round) {
  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    consensus::OperationType op_type = round->replicate_op()->replicate().op_type();
    if (!IsOpTypeAllowedInState(op_type, state_)) {
      return Status::ServiceUnavailable(
          Substitute("Tablet is not ready to accept operation. OpType: $0 Tablet State: $1",
                     consensus::OperationType_Name(op_type),
                     metadata::TabletStatePB_Name(state_)));
    }
  }
  consensus::ReplicateMsg* replicate_msg = round->replicate_op()->mutable_replicate();
  Transaction* transaction;
  switch (replicate_msg->op_type()) {
    case WRITE_OP:
    {
      DCHECK(replicate_msg->has_write_request()) << "WRITE_OP replica"
          " transaction must receive a WriteRequestPB";
      transaction = new WriteTransaction(
          new WriteTransactionState(this, replicate_msg->mutable_write_request()),
          consensus::REPLICA);
      break;
    }
    case CHANGE_CONFIG_OP:
    {
      DCHECK(replicate_msg->has_change_config_request()) << "CHANGE_CONFIG_OP"
          " replica transaction must receive a ChangeConfigRequestPB";
      transaction = new ChangeConfigTransaction(
          new ChangeConfigTransactionState(this, replicate_msg->mutable_change_config_request()),
          consensus::REPLICA,
          &config_sem_);
       break;
    }
    default:
      LOG(FATAL) << "Unsupported Operation Type";
  }

  // TODO(todd) Look at wiring the stuff below on the driver
  TransactionState* state = transaction->state();
  state->set_consensus_round(round.Pass());

  scoped_refptr<ReplicaTransactionDriver> driver;
  NewReplicaTransactionDriver(transaction, &driver);
  // FIXME: Bare ptr is a hack for a ref-counted object.
  state->consensus_round()->SetReplicaCommitContinuation(driver.get());
  state->consensus_round()->SetCommitCallback(driver->commit_finished_callback());

  RETURN_NOT_OK(driver->Execute());
  return Status::OK();
}



void TabletPeer::NewLeaderTransactionDriver(Transaction* transaction,
                                            scoped_refptr<LeaderTransactionDriver>* driver) {
  LeaderTransactionDriver::Create(transaction,
                                  &txn_tracker_,
                                  consensus_.get(),
                                  prepare_executor_.get(),
                                  leader_apply_executor_,
                                  &prepare_replicate_lock_,
                                  driver);
}

void TabletPeer::NewReplicaTransactionDriver(Transaction* transaction,
                                             scoped_refptr<ReplicaTransactionDriver>* driver) {
  ReplicaTransactionDriver::Create(transaction,
                                   &txn_tracker_,
                                   consensus_.get(),
                                   prepare_executor_.get(),
                                   replica_apply_executor_,
                                   driver);
}

}  // namespace tablet
}  // namespace kudu
