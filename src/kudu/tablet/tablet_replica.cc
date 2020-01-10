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
#include <functional>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <type_traits>
#include <vector>

#include <glog/logging.h>

#include "kudu/common/partition.h"
#include "kudu/common/timestamp.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus_meta_manager.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/result_tracker.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/tablet_replica_mm_ops.h"
#include "kudu/tablet/transactions/alter_schema_transaction.h"
#include "kudu/tablet/transactions/transaction_driver.h"
#include "kudu/tablet/transactions/write_transaction.h"
#include "kudu/util/logging.h"
#include "kudu/util/maintenance_manager.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/trace.h"

METRIC_DEFINE_histogram(tablet, op_prepare_queue_length, "Operation Prepare Queue Length",
                        kudu::MetricUnit::kTasks,
                        "Number of operations waiting to be prepared within this tablet. "
                        "High queue lengths indicate that the server is unable to process "
                        "operations as fast as they are being written to the WAL.",
                        kudu::MetricLevel::kInfo,
                        10000, 2);

METRIC_DEFINE_histogram(tablet, op_prepare_queue_time, "Operation Prepare Queue Time",
                        kudu::MetricUnit::kMicroseconds,
                        "Time that operations spent waiting in the prepare queue before being "
                        "processed. High queue times indicate that the server is unable to "
                        "process operations as fast as they are being written to the WAL.",
                        kudu::MetricLevel::kInfo,
                        10000000, 2);

METRIC_DEFINE_histogram(tablet, op_prepare_run_time, "Operation Prepare Run Time",
                        kudu::MetricUnit::kMicroseconds,
                        "Time that operations spent being prepared in the tablet. "
                        "High values may indicate that the server is under-provisioned or "
                        "that operations are experiencing high contention with one another for "
                        "locks.",
                        kudu::MetricLevel::kInfo,
                        10000000, 2);

METRIC_DEFINE_gauge_size(tablet, on_disk_size, "Tablet Size On Disk",
                         kudu::MetricUnit::kBytes,
                         "Space used by this tablet on disk, including metadata.",
                         kudu::MetricLevel::kInfo);
METRIC_DEFINE_gauge_string(tablet, state, "Tablet State",
                           kudu::MetricUnit::kState,
                           "State of this tablet.",
                           kudu::MetricLevel::kInfo);
METRIC_DEFINE_gauge_uint64(tablet, live_row_count, "Tablet Live Row Count",
                           kudu::MetricUnit::kRows,
                           "Number of live rows in this tablet, excludes deleted rows.",
                           kudu::MetricLevel::kInfo);

namespace kudu {
namespace tablet {

using consensus::ConsensusBootstrapInfo;
using consensus::ConsensusOptions;
using consensus::ConsensusRound;
using consensus::OpId;
using consensus::PeerProxyFactory;
using consensus::RaftConfigPB;
using consensus::RaftPeerPB;
using consensus::RaftConsensus;
using consensus::RpcPeerProxyFactory;
using consensus::ServerContext;
using consensus::TimeManager;
using consensus::ALTER_SCHEMA_OP;
using consensus::WRITE_OP;
using log::Log;
using log::LogAnchorRegistry;
using pb_util::SecureDebugString;
using rpc::Messenger;
using rpc::ResultTracker;
using std::map;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

TabletReplica::TabletReplica(
    scoped_refptr<TabletMetadata> meta,
    scoped_refptr<consensus::ConsensusMetadataManager> cmeta_manager,
    consensus::RaftPeerPB local_peer_pb,
    ThreadPool* apply_pool,
    Callback<void(const std::string& reason)> mark_dirty_clbk)
    : meta_(DCHECK_NOTNULL(std::move(meta))),
      cmeta_manager_(DCHECK_NOTNULL(std::move(cmeta_manager))),
      local_peer_pb_(std::move(local_peer_pb)),
      log_anchor_registry_(new LogAnchorRegistry()),
      apply_pool_(apply_pool),
      mark_dirty_clbk_(std::move(mark_dirty_clbk)),
      state_(NOT_INITIALIZED),
      last_status_("Tablet initializing...") {
}

TabletReplica::~TabletReplica() {
  // We are required to call Shutdown() before destroying a TabletReplica.
  CHECK(state_ == SHUTDOWN || state_ == FAILED)
      << "TabletReplica not fully shut down. State: "
      << TabletStatePB_Name(state_);
}

Status TabletReplica::Init(ServerContext server_ctx) {
  CHECK_EQ(NOT_INITIALIZED, state_);
  TRACE("Creating consensus instance");
  SetStatusMessage("Initializing consensus...");
  ConsensusOptions options;
  options.tablet_id = meta_->tablet_id();
  shared_ptr<RaftConsensus> consensus;
  RETURN_NOT_OK(RaftConsensus::Create(std::move(options),
                                      local_peer_pb_,
                                      cmeta_manager_,
                                      std::move(server_ctx),
                                      &consensus));
  consensus_ = std::move(consensus);
  set_state(INITIALIZED);
  SetStatusMessage("Initialized. Waiting to start...");
  return Status::OK();
}

Status TabletReplica::Start(const ConsensusBootstrapInfo& bootstrap_info,
                            shared_ptr<Tablet> tablet,
                            clock::Clock* clock,
                            shared_ptr<Messenger> messenger,
                            scoped_refptr<ResultTracker> result_tracker,
                            scoped_refptr<Log> log,
                            ThreadPool* prepare_pool,
                            DnsResolver* resolver) {
  DCHECK(tablet) << "A TabletReplica must be provided with a Tablet";
  DCHECK(log) << "A TabletReplica must be provided with a Log";

  {
    std::lock_guard<simple_spinlock> state_change_guard(state_change_lock_);

    scoped_refptr<MetricEntity> metric_entity;
    unique_ptr<PeerProxyFactory> peer_proxy_factory;
    unique_ptr<TimeManager> time_manager;
    {
      std::lock_guard<simple_spinlock> l(lock_);
      CHECK_EQ(BOOTSTRAPPING, state_);

      tablet_ = DCHECK_NOTNULL(std::move(tablet));
      clock_ = DCHECK_NOTNULL(clock);
      messenger_ = DCHECK_NOTNULL(std::move(messenger));
      result_tracker_ = std::move(result_tracker); // Passed null in tablet_replica-test
      log_ = DCHECK_NOTNULL(log); // Not moved because it's passed to RaftConsensus::Start() below.

      metric_entity = tablet_->GetMetricEntity();
      prepare_pool_token_ = prepare_pool->NewTokenWithMetrics(
          ThreadPool::ExecutionMode::SERIAL,
          {
              METRIC_op_prepare_queue_length.Instantiate(metric_entity),
              METRIC_op_prepare_queue_time.Instantiate(metric_entity),
              METRIC_op_prepare_run_time.Instantiate(metric_entity)
          });

      if (tablet_->metrics() != nullptr) {
        TRACE("Starting instrumentation");
        txn_tracker_.StartInstrumentation(tablet_->GetMetricEntity());

        METRIC_on_disk_size.InstantiateFunctionGauge(
            tablet_->GetMetricEntity(), Bind(&TabletReplica::OnDiskSize, Unretained(this)))
            ->AutoDetach(&metric_detacher_);
        METRIC_state.InstantiateFunctionGauge(
            tablet_->GetMetricEntity(), Bind(&TabletReplica::StateName, Unretained(this)))
            ->AutoDetach(&metric_detacher_);
        if (tablet_->metadata()->supports_live_row_count()) {
          METRIC_live_row_count.InstantiateFunctionGauge(
              tablet_->GetMetricEntity(),
              Bind(&TabletReplica::CountLiveRowsNoFail, Unretained(this)))
              ->AutoDetach(&metric_detacher_);
        } else {
          METRIC_live_row_count.InstantiateInvalid(tablet_->GetMetricEntity(), 0);
        }
      }
      txn_tracker_.StartMemoryTracking(tablet_->mem_tracker());

      TRACE("Starting consensus");
      VLOG(2) << "T " << tablet_id() << " P " << consensus_->peer_uuid() << ": Peer starting";
      VLOG(2) << "RaftConfig before starting: " << SecureDebugString(consensus_->CommittedConfig());

      peer_proxy_factory.reset(new RpcPeerProxyFactory(messenger_, resolver));
      time_manager.reset(new TimeManager(clock_, tablet_->mvcc_manager()->GetCleanTimestamp()));
    }

    // We cannot hold 'lock_' while we call RaftConsensus::Start() because it
    // may invoke TabletReplica::StartFollowerTransaction() during startup,
    // causing a self-deadlock. We take a ref to members protected by 'lock_'
    // before unlocking.
    RETURN_NOT_OK(consensus_->Start(
        bootstrap_info,
        std::move(peer_proxy_factory),
        log,
        std::move(time_manager),
        this,
        metric_entity,
        mark_dirty_clbk_));

    std::lock_guard<simple_spinlock> l(lock_);

    // If an error has been set (e.g. due to a disk failure from a separate
    // thread), error out.
    RETURN_NOT_OK(error_);

    CHECK_EQ(BOOTSTRAPPING, state_); // We are still protected by 'state_change_lock_'.
    set_state(RUNNING);
  }

  // Because we changed the tablet state, we need to re-report the tablet to the master.
  mark_dirty_clbk_.Run("Started TabletReplica");

  return Status::OK();
}

string TabletReplica::StateName() const {
  return TabletStatePB_Name(state());
}

const consensus::RaftConfigPB TabletReplica::RaftConfig() const {
  CHECK(consensus_) << "consensus is null";
  return consensus_->CommittedConfig();
}

void TabletReplica::Stop() {
  {
    std::unique_lock<simple_spinlock> lock(lock_);
    if (state_ == STOPPING || state_ == STOPPED ||
        state_ == SHUTDOWN || state_ == FAILED) {
      lock.unlock();
      WaitUntilStopped();
      return;
    }
    LOG_WITH_PREFIX(INFO) << "stopping tablet replica";
    set_state(STOPPING);
  }

  std::lock_guard<simple_spinlock> l(state_change_lock_);
  // Even though Tablet::Shutdown() also unregisters its ops, we have to do it here
  // to ensure that any currently running operation finishes before we proceed with
  // the rest of the shutdown sequence. In particular, a maintenance operation could
  // indirectly end up calling into the log, which we are about to shut down.
  if (tablet_) tablet_->UnregisterMaintenanceOps();
  UnregisterMaintenanceOps();

  if (consensus_) consensus_->Stop();

  // TODO(KUDU-183): Keep track of the pending tasks and send an "abort" message.
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

  if (tablet_) {
    tablet_->Shutdown();
  }

  // Only mark the peer as STOPPED when all other components have shut down.
  {
    std::lock_guard<simple_spinlock> lock(lock_);
    tablet_.reset();
    set_state(STOPPED);
  }

  VLOG(1) << "TabletReplica: tablet " << tablet_id() << " shut down!";
}

void TabletReplica::Shutdown() {
  Stop();
  if (consensus_) consensus_->Shutdown();
  std::lock_guard<simple_spinlock> lock(lock_);
  if (state_ == SHUTDOWN || state_ == FAILED) return;
  if (!error_.ok()) {
    set_state(FAILED);
    return;
  }
  set_state(SHUTDOWN);
}

void TabletReplica::WaitUntilStopped() {
  while (true) {
    {
      std::lock_guard<simple_spinlock> lock(lock_);
      if (state_ == STOPPED || state_ == SHUTDOWN || state_ == FAILED) {
        return;
      }
    }
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
}

string TabletReplica::LogPrefix() const {
  return meta_->LogPrefix();
}

void TabletReplica::set_state(TabletStatePB new_state) {
  switch (new_state) {
    case NOT_INITIALIZED:
      LOG(FATAL) << "Cannot transition to NOT_INITIALIZED state";
      return;
    case INITIALIZED:
      CHECK_EQ(NOT_INITIALIZED, state_);
      break;
    case BOOTSTRAPPING:
      CHECK_EQ(INITIALIZED, state_);
      break;
    case RUNNING:
      CHECK_EQ(BOOTSTRAPPING, state_);
      break;
    case STOPPING:
      CHECK_NE(STOPPED, state_);
      CHECK_NE(SHUTDOWN, state_);
      break;
    case STOPPED:
      CHECK_EQ(STOPPING, state_);
      break;
    case SHUTDOWN: FALLTHROUGH_INTENDED;
    case FAILED:
      CHECK_EQ(STOPPED, state_) << TabletStatePB_Name(state_);
      break;
    default:
      break;
  }
  state_ = new_state;
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
    if (cached_state == STOPPING || cached_state == STOPPED) {
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
  DCHECK(status_pb_out != nullptr);
  {
    std::lock_guard<simple_spinlock> lock(lock_);
    status_pb_out->set_state(state_);
    status_pb_out->set_last_status(last_status_);
  }
  const string& tablet_id = meta_->tablet_id();
  status_pb_out->set_tablet_id(tablet_id);
  status_pb_out->set_table_name(meta_->table_name());
  meta_->partition().ToPB(status_pb_out->mutable_partition());
  status_pb_out->set_tablet_data_state(meta_->tablet_data_state());
  status_pb_out->set_estimated_on_disk_size(OnDiskSize());
  // There are circumstances where the call to 'FindDataDirsByTabletId' may
  // fail, like if the tablet is tombstoned or failed. It's alright to return
  // an empty 'data_dirs' in this case-- the state and last status will inform
  // the caller.
  vector<string> data_dirs;
  ignore_result(
      meta_->fs_manager()->dd_manager()->FindDataDirsByTabletId(tablet_id,
                                                                &data_dirs));
  for (auto& dir : data_dirs) {
    status_pb_out->add_data_dirs(std::move(dir));
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

void TabletReplica::SetBootstrapping() {
  std::lock_guard<simple_spinlock> lock(lock_);
  set_state(BOOTSTRAPPING);
}

void TabletReplica::SetStatusMessage(const std::string& status) {
  std::lock_guard<simple_spinlock> lock(lock_);
  last_status_ = status;
}

string TabletReplica::last_status() const {
  std::lock_guard<simple_spinlock> lock(lock_);
  return last_status_;
}

void TabletReplica::SetError(const Status& error) {
  std::lock_guard<simple_spinlock> lock(lock_);
  CHECK(!error.ok());
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
  VLOG_WITH_PREFIX(4) << "Log GC: With Consensus retention: "
                      << Substitute("{dur: $0, peers: $1}", ret.for_durability, ret.for_peers);
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
  VLOG_WITH_PREFIX(4) << "Log GC: With Anchor retention: "
                      << Substitute("{dur: $0, peers: $1}", ret.for_durability, ret.for_peers);

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
  VLOG_WITH_PREFIX(4) << "Log GC: With Transaction retention: "
                      << Substitute("{dur: $0, peers: $1}", ret.for_durability, ret.for_peers);

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

Status TabletReplica::StartFollowerTransaction(const scoped_refptr<ConsensusRound>& round) {
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

  // A raw pointer is required to avoid a refcount cycle.
  state->consensus_round()->SetConsensusReplicatedCallback(
      std::bind(&TransactionDriver::ReplicationFinished,
                driver.get(),
                std::placeholders::_1));

  RETURN_NOT_OK(driver->ExecuteAsync());
  return Status::OK();
}

void TabletReplica::FinishConsensusOnlyRound(ConsensusRound* round) {
  consensus::ReplicateMsg* replicate_msg = round->replicate_msg();
  consensus::OperationType op_type = replicate_msg->op_type();

  // The timestamp of a Raft no-op used to assert term leadership is guaranteed
  // to be lower than the timestamps of writes in the same terms and those
  // thereafter. As such, we are able to bump the MVCC safe time with the
  // timestamps of such no-ops, as further transaction timestamps are
  // guaranteed to be higher than them.
  //
  // It is important for MVCC safe time updates to be serialized with respect
  // to transactions. To ensure that we only advance the safe time with the
  // no-op of term N after all transactions of term N-1 have been prepared, we
  // run the adjustment function on the prepare thread, which is the same
  // mechanism we use to serialize transactions.
  //
  // If the 'timestamp_in_opid_order' flag is unset, the no-op is assumed to be
  // the Raft leadership no-op from a version of Kudu that only supported creating
  // a no-op to assert a new leadership term, in which case it would be in order.
  if (op_type == consensus::NO_OP &&
      (!replicate_msg->noop_request().has_timestamp_in_opid_order() ||
       replicate_msg->noop_request().timestamp_in_opid_order())) {
    DCHECK(replicate_msg->has_noop_request());
    int64_t ts = replicate_msg->timestamp();
    // We are guaranteed that the prepare pool token is running now because
    // TabletReplica::Stop() stops RaftConsensus before it stops the prepare
    // pool token and this callback is invoked while the RaftConsensus lock is
    // held.
    CHECK_OK(prepare_pool_token_->SubmitFunc([this, ts] {
      std::lock_guard<simple_spinlock> l(lock_);
      if (state_ == RUNNING || state_ == BOOTSTRAPPING) {
        tablet_->mvcc_manager()->AdjustSafeTime(Timestamp(ts));
      }
    }));
  }
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
  std::lock_guard<simple_spinlock> state_change_lock(state_change_lock_);

  if (state() != RUNNING) {
    LOG(WARNING) << "Not registering maintenance operations for " << tablet_
                 << ": tablet not in RUNNING state";
    return;
  }

  vector<MaintenanceOp*> maintenance_ops;

  gscoped_ptr<MaintenanceOp> mrs_flush_op(new FlushMRSOp(this));
  maint_mgr->RegisterOp(mrs_flush_op.get());
  maintenance_ops.push_back(mrs_flush_op.release());

  gscoped_ptr<MaintenanceOp> dms_flush_op(new FlushDeltaMemStoresOp(this));
  maint_mgr->RegisterOp(dms_flush_op.get());
  maintenance_ops.push_back(dms_flush_op.release());

  gscoped_ptr<MaintenanceOp> log_gc(new LogGCOp(this));
  maint_mgr->RegisterOp(log_gc.get());
  maintenance_ops.push_back(log_gc.release());

  std::shared_ptr<Tablet> tablet;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    DCHECK(maintenance_ops_.empty());
    maintenance_ops_.swap(maintenance_ops);
    tablet = tablet_;
  }
  tablet->RegisterMaintenanceOps(maint_mgr);
}

void TabletReplica::CancelMaintenanceOpsForTests() {
  std::lock_guard<simple_spinlock> l(lock_);
  for (MaintenanceOp* op : maintenance_ops_) {
    op->CancelAndDisable();
  }
}

void TabletReplica::UnregisterMaintenanceOps() {
  DCHECK(state_change_lock_.is_locked());
  vector<MaintenanceOp*> maintenance_ops;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    maintenance_ops.swap(maintenance_ops_);
  }
  for (MaintenanceOp* op : maintenance_ops) {
    op->Unregister();
  }
  STLDeleteElements(&maintenance_ops);
}

size_t TabletReplica::OnDiskSize() const {
  size_t ret = 0;

  // Consensus metadata.
  if (consensus_ != nullptr) {
    ret += consensus_->MetadataOnDiskSize();
  }

  shared_ptr<Tablet> tablet;
  scoped_refptr<Log> log;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    tablet = tablet_;
    log = log_;
  }
  if (tablet) {
    ret += tablet->OnDiskSize();
  }
  if (log) {
    ret += log->OnDiskSize();
  }
  return ret;
}

Status TabletReplica::CountLiveRows(uint64_t* live_row_count) const {
  shared_ptr<Tablet> tablet;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    tablet = tablet_;
  }

  if (!tablet) {
    return Status::IllegalState("The tablet is shutdown.");
  }

  return tablet->CountLiveRows(live_row_count);
}

uint64_t TabletReplica::CountLiveRowsNoFail() const {
  uint64_t live_row_count = 0;
  ignore_result(CountLiveRows(&live_row_count));
  return live_row_count;
}

void TabletReplica::UpdateTabletStats(vector<string>* dirty_tablets) {
  // It's necessary to check the state before visiting the "consensus_".
  if (RUNNING != state()) {
    return;
  }

  ReportedTabletStatsPB pb;
  pb.set_on_disk_size(OnDiskSize());
  uint64_t live_row_count;
  Status s = CountLiveRows(&live_row_count);
  if (s.ok()) {
    pb.set_live_row_count(live_row_count);
  }

  // We cannot hold 'lock_' while calling RaftConsensus::role() because
  // it may invoke TabletReplica::StartFollowerTransaction() and lead to
  // a deadlock.
  RaftPeerPB::Role role = consensus_->role();

  std::lock_guard<simple_spinlock> l(lock_);
  if (stats_pb_.on_disk_size() != pb.on_disk_size() ||
      stats_pb_.live_row_count() != pb.live_row_count()) {
    if (consensus::RaftPeerPB_Role_LEADER == role) {
      dirty_tablets->emplace_back(tablet_id());
    }
    stats_pb_.Swap(&pb);
  }
}

ReportedTabletStatsPB TabletReplica::GetTabletStats() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return stats_pb_;
}

void TabletReplica::MakeUnavailable(const Status& error) {
  std::shared_ptr<Tablet> tablet;
  {
    std::lock_guard<simple_spinlock> lock(lock_);
    tablet = tablet_;
    for (MaintenanceOp* op : maintenance_ops_) {
      op->CancelAndDisable();
    }
  }
  // Stop the Tablet from doing further I/O.
  if (tablet) tablet->Stop();

  // Set the error; when the replica is shut down, it will end up FAILED.
  SetError(error);
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
    RETURN_NOT_OK(tablet_->mvcc_manager()->WaitForApplyingTransactionsToCommit());
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
