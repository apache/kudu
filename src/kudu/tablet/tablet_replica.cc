// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/tablet/tablet_replica.h"

#include <algorithm>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>

#include "kudu/consensus/consensus_meta_manager.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/mathlimits.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/remote_method.h"
#include "kudu/rpc/rpc_service.h"
#include "kudu/rpc/service_pool.h"
#include "kudu/tablet/transactions/alter_schema_transaction.h"
#include "kudu/tablet/transactions/transaction_driver.h"
#include "kudu/tablet/transactions/write_transaction.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/tablet_bootstrap.h"
#include "kudu/tablet/tablet_metrics.h"
#include "kudu/tablet/tablet_replica_mm_ops.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/trace.h"

namespace kudu {
namespace tablet {

METRIC_DEFINE_histogram(tablet, op_prepare_queue_length, "Operation Prepare Queue Length",
                        MetricUnit::kTasks,
                        "Number of operations waiting to be prepared within this tablet. "
                        "High queue lengths indicate that the server is unable to process "
                        "operations as fast as they are being written to the WAL.",
                        10000, 2);

METRIC_DEFINE_histogram(tablet, op_prepare_queue_time, "Operation Prepare Queue Time",
                        MetricUnit::kMicroseconds,
                        "Time that operations spent waiting in the prepare queue before being "
                        "processed. High queue times indicate that the server is unable to "
                        "process operations as fast as they are being written to the WAL.",
                        10000000, 2);

METRIC_DEFINE_histogram(tablet, op_prepare_run_time, "Operation Prepare Run Time",
                        MetricUnit::kMicroseconds,
                        "Time that operations spent being prepared in the tablet. "
                        "High values may indicate that the server is under-provisioned or "
                        "that operations are experiencing high contention with one another for "
                        "locks.",
                        10000000, 2);

using consensus::ConsensusBootstrapInfo;
using consensus::ConsensusOptions;
using consensus::ConsensusRound;
using consensus::OpId;
using consensus::PeerProxyFactory;
using consensus::RaftConfigPB;
using consensus::RaftPeerPB;
using consensus::RaftConsensus;
using consensus::RpcPeerProxyFactory;
using consensus::TimeManager;
using consensus::ALTER_SCHEMA_OP;
using consensus::WRITE_OP;
using log::Log;
using log::LogAnchorRegistry;
using rpc::Messenger;
using rpc::ResultTracker;
using std::map;
using std::shared_ptr;
using std::unique_ptr;
using strings::Substitute;

TabletReplica::TabletReplica(
    const scoped_refptr<TabletMetadata>& meta,
    const scoped_refptr<consensus::ConsensusMetadataManager>& cmeta_manager,
    consensus::RaftPeerPB local_peer_pb,
    ThreadPool* apply_pool,
    Callback<void(const std::string& reason)> mark_dirty_clbk)
    : meta_(meta),
      cmeta_manager_(cmeta_manager),
      tablet_id_(meta->tablet_id()),
      local_peer_pb_(std::move(local_peer_pb)),
      state_(NOT_STARTED),
      last_status_("Tablet initializing..."),
      apply_pool_(apply_pool),
      log_anchor_registry_(new LogAnchorRegistry()),
      mark_dirty_clbk_(std::move(mark_dirty_clbk)) {}

TabletReplica::~TabletReplica() {
  std::lock_guard<simple_spinlock> lock(lock_);
  // We should either have called Shutdown(), or we should have never called
  // Init().
  CHECK(!tablet_)
      << "TabletReplica not fully shut down. State: "
      << TabletStatePB_Name(state_);
}

Status TabletReplica::Init(const shared_ptr<Tablet>& tablet,
                           const scoped_refptr<server::Clock>& clock,
                           const shared_ptr<Messenger>& messenger,
                           const scoped_refptr<ResultTracker>& result_tracker,
                           const scoped_refptr<Log>& log,
                           const scoped_refptr<MetricEntity>& metric_entity,
                           ThreadPool* raft_pool,
                           ThreadPool* prepare_pool) {

  DCHECK(tablet) << "A TabletReplica must be provided with a Tablet";
  DCHECK(log) << "A TabletReplica must be provided with a Log";

  prepare_pool_token_ = prepare_pool->NewTokenWithMetrics(
      ThreadPool::ExecutionMode::SERIAL,
      {
          METRIC_op_prepare_queue_length.Instantiate(metric_entity),
          METRIC_op_prepare_queue_time.Instantiate(metric_entity),
          METRIC_op_prepare_run_time.Instantiate(metric_entity)
      });
  {
    std::lock_guard<simple_spinlock> lock(lock_);
    CHECK_EQ(BOOTSTRAPPING, state_);
    tablet_ = tablet;
    clock_ = clock;
    messenger_ = messenger;
    log_ = log;
    result_tracker_ = result_tracker;

    TRACE("Creating consensus instance");
    ConsensusOptions options;
    options.tablet_id = meta_->tablet_id();
    consensus_.reset(new RaftConsensus(options, local_peer_pb_, cmeta_manager_, raft_pool));
    RETURN_NOT_OK(consensus_->Init());
  }

  if (tablet_->metrics() != nullptr) {
    TRACE("Starting instrumentation");
    txn_tracker_.StartInstrumentation(tablet_->GetMetricEntity());
  }
  txn_tracker_.StartMemoryTracking(tablet_->mem_tracker());

  TRACE("TabletReplica::Init() finished");
  VLOG(2) << "T " << tablet_id() << " P " << consensus_->peer_uuid() << ": Peer Initted";
  return Status::OK();
}

Status TabletReplica::Start(const ConsensusBootstrapInfo& bootstrap_info) {
  std::lock_guard<simple_spinlock> l(state_change_lock_);
  TRACE("Starting consensus");

  VLOG(2) << "T " << tablet_id() << " P " << consensus_->peer_uuid() << ": Peer starting";

  VLOG(2) << "RaftConfig before starting: " << SecureDebugString(consensus_->CommittedConfig());

  gscoped_ptr<PeerProxyFactory> peer_proxy_factory(new RpcPeerProxyFactory(messenger_));
  scoped_refptr<TimeManager> time_manager(new TimeManager(
      clock_, tablet_->mvcc_manager()->GetCleanTimestamp()));

  RETURN_NOT_OK(consensus_->Start(
      bootstrap_info,
      std::move(peer_proxy_factory),
      log_,
      std::move(time_manager),
      this,
      tablet_->GetMetricEntity(),
      mark_dirty_clbk_));

  {
    std::lock_guard<simple_spinlock> lock(lock_);
    CHECK_EQ(state_, BOOTSTRAPPING);
    state_ = RUNNING;
  }

  // Because we changed the tablet state, we need to re-report the tablet to the master.
  mark_dirty_clbk_.Run("Started TabletReplica");

  return Status::OK();
}

const consensus::RaftConfigPB TabletReplica::RaftConfig() const {
  CHECK(consensus_) << "consensus is null";
  return consensus_->CommittedConfig();
}

void TabletReplica::Shutdown() {

  LOG(INFO) << "Initiating TabletReplica shutdown for tablet: " << tablet_id_;

  {
    std::unique_lock<simple_spinlock> lock(lock_);
    if (state_ == QUIESCING || state_ == SHUTDOWN) {
      lock.unlock();
      WaitUntilShutdown();
      return;
    }
    state_ = QUIESCING;
  }

  std::lock_guard<simple_spinlock> l(state_change_lock_);
  // Even though Tablet::Shutdown() also unregisters its ops, we have to do it here
  // to ensure that any currently running operation finishes before we proceed with
  // the rest of the shutdown sequence. In particular, a maintenance operation could
  // indirectly end up calling into the log, which we are about to shut down.
  if (tablet_) tablet_->UnregisterMaintenanceOps();
  UnregisterMaintenanceOps();

  if (consensus_) consensus_->Shutdown();

  // TODO: KUDU-183: Keep track of the pending tasks and send an "abort" message.
  LOG_SLOW_EXECUTION(WARNING, 1000,
      Substitute("TabletReplica: tablet $0: Waiting for Transactions to complete", tablet_id())) {
    txn_tracker_.WaitForAllToFinish();
  }

  if (prepare_pool_token_) {
    prepare_pool_token_->Shutdown();
  }

  if (log_) {
    WARN_NOT_OK(log_->Close(), "Error closing the Log.");
  }

  if (VLOG_IS_ON(1)) {
    VLOG(1) << "TabletReplica: tablet " << tablet_id() << " shut down!";
  }

  if (tablet_) {
    tablet_->Shutdown();
  }

  // Only mark the peer as SHUTDOWN when all other components have shut down.
  {
    std::lock_guard<simple_spinlock> lock(lock_);
    // Release mem tracker resources.
    consensus_.reset();
    tablet_.reset();
    state_ = SHUTDOWN;
  }
}

void TabletReplica::WaitUntilShutdown() {
  while (true) {
    {
      std::lock_guard<simple_spinlock> lock(lock_);
      if (state_ == SHUTDOWN) {
        return;
      }
    }
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
}

Status TabletReplica::CheckRunning() const {
  {
    std::lock_guard<simple_spinlock> lock(lock_);
    if (state_ != RUNNING) {
      return Status::IllegalState(Substitute("The tablet is not in a running state: $0",
                                             TabletStatePB_Name(state_)));
    }
  }
  return Status::OK();
}

Status TabletReplica::WaitUntilConsensusRunning(const MonoDelta& timeout) {
  MonoTime start(MonoTime::Now());

  int backoff_exp = 0;
  const int kMaxBackoffExp = 8;
  while (true) {
    bool has_consensus = false;
    TabletStatePB cached_state;
    {
      std::lock_guard<simple_spinlock> lock(lock_);
      cached_state = state_;
      if (consensus_) {
        has_consensus = true; // consensus_ is a set-once object.
      }
    }
    if (cached_state == QUIESCING || cached_state == SHUTDOWN) {
      return Status::IllegalState(
          Substitute("The tablet is already shutting down or shutdown. State: $0",
                     TabletStatePB_Name(cached_state)));
    }
    if (cached_state == RUNNING && has_consensus && consensus_->IsRunning()) {
      break;
    }
    MonoTime now(MonoTime::Now());
    MonoDelta elapsed(now - start);
    if (elapsed > timeout) {
      return Status::TimedOut(Substitute("Raft Consensus is not running after waiting for $0: $1",
                                         elapsed.ToString(), TabletStatePB_Name(cached_state)));
    }
    SleepFor(MonoDelta::FromMilliseconds(1L << backoff_exp));
    backoff_exp = std::min(backoff_exp + 1, kMaxBackoffExp);
  }
  return Status::OK();
}

Status TabletReplica::SubmitWrite(unique_ptr<WriteTransactionState> state) {
  RETURN_NOT_OK(CheckRunning());

  state->SetResultTracker(result_tracker_);
  gscoped_ptr<WriteTransaction> transaction(new WriteTransaction(std::move(state),
                                                                 consensus::LEADER));
  scoped_refptr<TransactionDriver> driver;
  RETURN_NOT_OK(NewLeaderTransactionDriver(transaction.PassAs<Transaction>(),
                                           &driver));
  return driver->ExecuteAsync();
}

Status TabletReplica::SubmitAlterSchema(unique_ptr<AlterSchemaTransactionState> state) {
  RETURN_NOT_OK(CheckRunning());

  gscoped_ptr<AlterSchemaTransaction> transaction(
      new AlterSchemaTransaction(std::move(state), consensus::LEADER));
  scoped_refptr<TransactionDriver> driver;
  RETURN_NOT_OK(NewLeaderTransactionDriver(transaction.PassAs<Transaction>(), &driver));
  return driver->ExecuteAsync();
}

void TabletReplica::GetTabletStatusPB(TabletStatusPB* status_pb_out) const {
  std::lock_guard<simple_spinlock> lock(lock_);
  DCHECK(status_pb_out != nullptr);
  status_pb_out->set_tablet_id(meta_->tablet_id());
  status_pb_out->set_table_name(meta_->table_name());
  status_pb_out->set_last_status(last_status_);
  meta_->partition().ToPB(status_pb_out->mutable_partition());
  status_pb_out->set_state(state_);
  status_pb_out->set_tablet_data_state(meta_->tablet_data_state());
  if (tablet_) {
    status_pb_out->set_estimated_on_disk_size(tablet_->OnDiskSize());
  }
}

Status TabletReplica::RunLogGC() {
  if (!CheckRunning().ok()) {
    return Status::OK();
  }
  int32_t num_gced;
  log::RetentionIndexes retention = GetRetentionIndexes();
  Status s = log_->GC(retention, &num_gced);
  if (!s.ok()) {
    s = s.CloneAndPrepend("Unexpected error while running Log GC from TabletReplica");
    LOG(ERROR) << s.ToString();
  }
  return Status::OK();
}

void TabletReplica::SetStatusMessage(const std::string& status) {
  std::lock_guard<simple_spinlock> lock(lock_);
  last_status_ = status;
}

string TabletReplica::last_status() const {
  std::lock_guard<simple_spinlock> lock(lock_);
  return last_status_;
}

void TabletReplica::SetFailed(const Status& error) {
  std::lock_guard<simple_spinlock> lock(lock_);
  CHECK(!error.ok());
  state_ = FAILED;
  error_ = error;
  last_status_ = error.ToString();
}

string TabletReplica::HumanReadableState() const {
  std::lock_guard<simple_spinlock> lock(lock_);
  TabletDataState data_state = meta_->tablet_data_state();
  // If failed, any number of things could have gone wrong.
  if (state_ == FAILED) {
    return Substitute("$0 ($1): $2", TabletStatePB_Name(state_),
                      TabletDataState_Name(data_state),
                      error_.ToString());
  // If it's copying, or tombstoned, that is the important thing
  // to show.
  } else if (data_state != TABLET_DATA_READY) {
    return TabletDataState_Name(data_state);
  }
  // Otherwise, the tablet's data is in a "normal" state, so we just display
  // the runtime state (BOOTSTRAPPING, RUNNING, etc).
  return TabletStatePB_Name(state_);
}

void TabletReplica::GetInFlightTransactions(Transaction::TraceType trace_type,
                                            vector<consensus::TransactionStatusPB>* out) const {
  vector<scoped_refptr<TransactionDriver> > pending_transactions;
  txn_tracker_.GetPendingTransactions(&pending_transactions);
  for (const scoped_refptr<TransactionDriver>& driver : pending_transactions) {
    if (driver->state() != nullptr) {
      consensus::TransactionStatusPB status_pb;
      status_pb.mutable_op_id()->CopyFrom(driver->GetOpId());
      switch (driver->tx_type()) {
        case Transaction::WRITE_TXN:
          status_pb.set_tx_type(consensus::WRITE_OP);
          break;
        case Transaction::ALTER_SCHEMA_TXN:
          status_pb.set_tx_type(consensus::ALTER_SCHEMA_OP);
          break;
      }
      status_pb.set_description(driver->ToString());
      int64_t running_for_micros =
          (MonoTime::Now() - driver->start_time()).ToMicroseconds();
      status_pb.set_running_for_micros(running_for_micros);
      if (trace_type == Transaction::TRACE_TXNS) {
        status_pb.set_trace_buffer(driver->trace()->DumpToString());
      }
      out->push_back(status_pb);
    }
  }
}

log::RetentionIndexes TabletReplica::GetRetentionIndexes() const {
  // Let consensus set a minimum index that should be anchored.
  // This ensures that we:
  //   (a) don't GC any operations which are still in flight
  //   (b) don't GC any operations that are needed to catch up lagging peers.
  log::RetentionIndexes ret = consensus_->GetRetentionIndexes();

  // If we never have written to the log, no need to proceed.
  if (ret.for_durability == 0) return ret;

  // Next, we interrogate the anchor registry.
  // Returns OK if minimum known, NotFound if no anchors are registered.
  {
    int64_t min_anchor_index;
    Status s = log_anchor_registry_->GetEarliestRegisteredLogIndex(&min_anchor_index);
    if (PREDICT_FALSE(!s.ok())) {
      DCHECK(s.IsNotFound()) << "Unexpected error calling LogAnchorRegistry: " << s.ToString();
    } else {
      ret.for_durability = std::min(ret.for_durability, min_anchor_index);
    }
  }

  // Next, interrogate the TransactionTracker.
  vector<scoped_refptr<TransactionDriver> > pending_transactions;
  txn_tracker_.GetPendingTransactions(&pending_transactions);
  for (const scoped_refptr<TransactionDriver>& driver : pending_transactions) {
    OpId tx_op_id = driver->GetOpId();
    // A transaction which doesn't have an opid hasn't been submitted for replication yet and
    // thus has no need to anchor the log.
    if (tx_op_id.IsInitialized()) {
      ret.for_durability = std::min(ret.for_durability, tx_op_id.index());
    }
  }

  return ret;
}

Status TabletReplica::GetReplaySizeMap(map<int64_t, int64_t>* replay_size_map) const {
  RETURN_NOT_OK(CheckRunning());
  log_->GetReplaySizeMap(replay_size_map);
  return Status::OK();
}

Status TabletReplica::GetGCableDataSize(int64_t* retention_size) const {
  RETURN_NOT_OK(CheckRunning());
  *retention_size = log_->GetGCableDataSize(GetRetentionIndexes());
  return Status::OK();
}

Status TabletReplica::StartReplicaTransaction(const scoped_refptr<ConsensusRound>& round) {
  {
    std::lock_guard<simple_spinlock> lock(lock_);
    if (state_ != RUNNING && state_ != BOOTSTRAPPING) {
      return Status::IllegalState(TabletStatePB_Name(state_));
    }
  }

  consensus::ReplicateMsg* replicate_msg = round->replicate_msg();
  DCHECK(replicate_msg->has_timestamp());
  gscoped_ptr<Transaction> transaction;
  switch (replicate_msg->op_type()) {
    case WRITE_OP:
    {
      DCHECK(replicate_msg->has_write_request()) << "WRITE_OP replica"
          " transaction must receive a WriteRequestPB";
      unique_ptr<WriteTransactionState> tx_state(
          new WriteTransactionState(
              this,
              &replicate_msg->write_request(),
              replicate_msg->has_request_id() ? &replicate_msg->request_id() : nullptr));
      tx_state->SetResultTracker(result_tracker_);

      transaction.reset(new WriteTransaction(std::move(tx_state), consensus::REPLICA));
      break;
    }
    case ALTER_SCHEMA_OP:
    {
      DCHECK(replicate_msg->has_alter_schema_request()) << "ALTER_SCHEMA_OP replica"
          " transaction must receive an AlterSchemaRequestPB";
      unique_ptr<AlterSchemaTransactionState> tx_state(
          new AlterSchemaTransactionState(this, &replicate_msg->alter_schema_request(),
                                          nullptr));
      transaction.reset(
          new AlterSchemaTransaction(std::move(tx_state), consensus::REPLICA));
      break;
    }
    default:
      LOG(FATAL) << "Unsupported Operation Type";
  }

  // TODO(todd) Look at wiring the stuff below on the driver
  TransactionState* state = transaction->state();
  state->set_consensus_round(round);

  scoped_refptr<TransactionDriver> driver;
  RETURN_NOT_OK(NewReplicaTransactionDriver(std::move(transaction), &driver));

  // Unretained is required to avoid a refcount cycle.
  state->consensus_round()->SetConsensusReplicatedCallback(
      Bind(&TransactionDriver::ReplicationFinished, Unretained(driver.get())));

  RETURN_NOT_OK(driver->ExecuteAsync());
  return Status::OK();
}

Status TabletReplica::NewLeaderTransactionDriver(gscoped_ptr<Transaction> transaction,
                                                 scoped_refptr<TransactionDriver>* driver) {
  scoped_refptr<TransactionDriver> tx_driver = new TransactionDriver(
    &txn_tracker_,
    consensus_.get(),
    log_.get(),
    prepare_pool_token_.get(),
    apply_pool_,
    &txn_order_verifier_);
  RETURN_NOT_OK(tx_driver->Init(std::move(transaction), consensus::LEADER));
  driver->swap(tx_driver);

  return Status::OK();
}

Status TabletReplica::NewReplicaTransactionDriver(gscoped_ptr<Transaction> transaction,
                                                  scoped_refptr<TransactionDriver>* driver) {
  scoped_refptr<TransactionDriver> tx_driver = new TransactionDriver(
    &txn_tracker_,
    consensus_.get(),
    log_.get(),
    prepare_pool_token_.get(),
    apply_pool_,
    &txn_order_verifier_);
  RETURN_NOT_OK(tx_driver->Init(std::move(transaction), consensus::REPLICA));
  driver->swap(tx_driver);

  return Status::OK();
}

void TabletReplica::RegisterMaintenanceOps(MaintenanceManager* maint_mgr) {
  // Taking state_change_lock_ ensures that we don't shut down concurrently with
  // this last start-up task.
  std::lock_guard<simple_spinlock> l(state_change_lock_);

  if (state() != RUNNING) {
    LOG(WARNING) << "Not registering maintenance operations for " << tablet_
                 << ": tablet not in RUNNING state";
    return;
  }

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

void TabletReplica::UnregisterMaintenanceOps() {
  DCHECK(state_change_lock_.is_locked());
  for (MaintenanceOp* op : maintenance_ops_) {
    op->Unregister();
  }
  STLDeleteElements(&maintenance_ops_);
}

Status FlushInflightsToLogCallback::WaitForInflightsAndFlushLog() {
  // This callback is triggered prior to any TabletMetadata flush.
  // The guarantee that we are trying to enforce is this:
  //
  //   If an operation has been flushed to stable storage (eg a DRS or DeltaFile)
  //   then its COMMIT message must be present in the log.
  //
  // The purpose for this is so that, during bootstrap, we can accurately identify
  // whether each operation has been flushed. If we don't see a COMMIT message for
  // an operation, then we assume it was not completely applied and needs to be
  // re-applied. Thus, if we had something on disk but with no COMMIT message,
  // we'd attempt to double-apply the write, resulting in an error (eg trying to
  // delete an already-deleted row).
  //
  // So, to enforce this property, we do two steps:
  //
  // 1) Wait for any operations which are already mid-Apply() to Commit() in MVCC.
  //
  // Because the operations always enqueue their COMMIT message to the log
  // before calling Commit(), this ensures that any in-flight operations have
  // their commit messages "en route".
  //
  // NOTE: we only wait for those operations that have started their Apply() phase.
  // Any operations which haven't yet started applying haven't made any changes
  // to in-memory state: thus, they obviously couldn't have made any changes to
  // on-disk storage either (data can only get to the disk by going through an in-memory
  // store). Only those that have started Apply() could have potentially written some
  // data which is now on disk.
  //
  // Perhaps more importantly, if we waited on operations that hadn't started their
  // Apply() phase, we might be waiting forever -- for example, if a follower has been
  // partitioned from its leader, it may have operations sitting around in flight
  // for quite a long time before eventually aborting or committing. This would
  // end up blocking all flushes if we waited on it.
  //
  // 2) Flush the log
  //
  // This ensures that the above-mentioned commit messages are not just enqueued
  // to the log, but also on disk.
  VLOG(1) << "T " << tablet_->metadata()->tablet_id()
      <<  ": Waiting for in-flight transactions to commit.";
  LOG_SLOW_EXECUTION(WARNING, 200, "Committing in-flights took a long time.") {
    tablet_->mvcc_manager()->WaitForApplyingTransactionsToCommit();
  }
  VLOG(1) << "T " << tablet_->metadata()->tablet_id()
      << ": Waiting for the log queue to be flushed.";
  LOG_SLOW_EXECUTION(WARNING, 200, "Flushing the Log queue took a long time.") {
    RETURN_NOT_OK(log_->WaitUntilAllFlushed());
  }
  return Status::OK();
}


}  // namespace tablet
}  // namespace kudu
