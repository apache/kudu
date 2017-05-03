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

#include "kudu/tserver/ts_tablet_manager.h"

#include <algorithm>
#include <boost/bind.hpp>
#include <boost/optional.hpp>
#include <glog/logging.h>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/master/master.pb.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/result_tracker.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/tablet_bootstrap.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/heartbeater.h"
#include "kudu/tserver/tablet_copy_client.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tablet_service.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/metrics.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/trace.h"

DEFINE_int32(num_tablets_to_copy_simultaneously, 10,
             "Number of threads available to copy tablets from remote servers.");
TAG_FLAG(num_tablets_to_copy_simultaneously, advanced);

DEFINE_int32(num_tablets_to_open_simultaneously, 0,
             "Number of threads available to open tablets during startup. If this "
             "is set to 0 (the default), then the number of bootstrap threads will "
             "be set based on the number of data directories. If the data directories "
             "are on some very fast storage device such as SSD or a RAID array, it "
             "may make sense to manually tune this.");
TAG_FLAG(num_tablets_to_open_simultaneously, advanced);

DEFINE_int32(tablet_start_warn_threshold_ms, 500,
             "If a tablet takes more than this number of millis to start, issue "
             "a warning with a trace.");
TAG_FLAG(tablet_start_warn_threshold_ms, hidden);

DEFINE_double(fault_crash_after_blocks_deleted, 0.0,
              "Fraction of the time when the tablet will crash immediately "
              "after deleting the data blocks during tablet deletion. "
              "(For testing only!)");
TAG_FLAG(fault_crash_after_blocks_deleted, unsafe);

DEFINE_double(fault_crash_after_wal_deleted, 0.0,
              "Fraction of the time when the tablet will crash immediately "
              "after deleting the WAL segments during tablet deletion. "
              "(For testing only!)");
TAG_FLAG(fault_crash_after_wal_deleted, unsafe);

DEFINE_double(fault_crash_after_cmeta_deleted, 0.0,
              "Fraction of the time when the tablet will crash immediately "
              "after deleting the consensus metadata during tablet deletion. "
              "(For testing only!)");
TAG_FLAG(fault_crash_after_cmeta_deleted, unsafe);

DEFINE_double(fault_crash_after_tc_files_fetched, 0.0,
              "Fraction of the time when the tablet will crash immediately "
              "after fetching the files during a tablet copy but before "
              "marking the superblock as TABLET_DATA_READY. "
              "(For testing only!)");
TAG_FLAG(fault_crash_after_tc_files_fetched, unsafe);

namespace kudu {
namespace tserver {

METRIC_DEFINE_histogram(server, op_apply_queue_length, "Operation Apply Queue Length",
                        MetricUnit::kTasks,
                        "Number of operations waiting to be applied to the tablet. "
                        "High queue lengths indicate that the server is unable to process "
                        "operations as fast as they are being written to the WAL.",
                        10000, 2);

METRIC_DEFINE_histogram(server, op_apply_queue_time, "Operation Apply Queue Time",
                        MetricUnit::kMicroseconds,
                        "Time that operations spent waiting in the apply queue before being "
                        "processed. High queue times indicate that the server is unable to "
                        "process operations as fast as they are being written to the WAL.",
                        10000000, 2);

METRIC_DEFINE_histogram(server, op_apply_run_time, "Operation Apply Run Time",
                        MetricUnit::kMicroseconds,
                        "Time that operations spent being applied to the tablet. "
                        "High values may indicate that the server is under-provisioned or "
                        "that operations consist of very large batches.",
                        10000000, 2);

using consensus::ConsensusMetadata;
using consensus::ConsensusStatePB;
using consensus::OpId;
using consensus::RaftConfigPB;
using consensus::RaftPeerPB;
using consensus::StartTabletCopyRequestPB;
using log::Log;
using master::ReportedTabletPB;
using master::TabletReportPB;
using rpc::ResultTracker;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;
using tablet::Tablet;
using tablet::TABLET_DATA_COPYING;
using tablet::TABLET_DATA_DELETED;
using tablet::TABLET_DATA_READY;
using tablet::TABLET_DATA_TOMBSTONED;
using tablet::TabletDataState;
using tablet::TabletMetadata;
using tablet::TabletReplica;
using tablet::TabletStatusListener;
using tablet::TabletStatusPB;
using tserver::TabletCopyClient;

TSTabletManager::TSTabletManager(FsManager* fs_manager,
                                 TabletServer* server,
                                 MetricRegistry* metric_registry)
  : fs_manager_(fs_manager),
    server_(server),
    metric_registry_(metric_registry),
    state_(MANAGER_INITIALIZING) {

  CHECK_OK(ThreadPoolBuilder("apply").Build(&apply_pool_));
  apply_pool_->SetQueueLengthHistogram(
      METRIC_op_apply_queue_length.Instantiate(server_->metric_entity()));
  apply_pool_->SetQueueTimeMicrosHistogram(
      METRIC_op_apply_queue_time.Instantiate(server_->metric_entity()));
  apply_pool_->SetRunTimeMicrosHistogram(
      METRIC_op_apply_run_time.Instantiate(server_->metric_entity()));
}

TSTabletManager::~TSTabletManager() {
}

Status TSTabletManager::Init() {
  CHECK_EQ(state(), MANAGER_INITIALIZING);

  // Start the tablet copy thread pool. We set a max queue size of 0 so that if
  // the number of requests exceeds the number of threads, a
  // SERVICE_UNAVAILABLE error may be returned to the remote caller.
  RETURN_NOT_OK(ThreadPoolBuilder("tablet-copy")
                .set_max_queue_size(0)
                .set_max_threads(FLAGS_num_tablets_to_copy_simultaneously)
                .Build(&tablet_copy_pool_));

  // Start the threadpool we'll use to open tablets.
  // This has to be done in Init() instead of the constructor, since the
  // FsManager isn't initialized until this point.
  int max_open_threads = FLAGS_num_tablets_to_open_simultaneously;
  if (max_open_threads == 0) {
    // Default to the number of disks.
    max_open_threads = fs_manager_->GetDataRootDirs().size();
  }
  RETURN_NOT_OK(ThreadPoolBuilder("tablet-open")
                .set_max_threads(max_open_threads)
                .Build(&open_tablet_pool_));

  // Search for tablets in the metadata dir.
  vector<string> tablet_ids;
  RETURN_NOT_OK(fs_manager_->ListTabletIds(&tablet_ids));

  InitLocalRaftPeerPB();

  vector<scoped_refptr<TabletMetadata> > metas;

  // First, load all of the tablet metadata. We do this before we start
  // submitting the actual OpenTablet() tasks so that we don't have to compete
  // for disk resources, etc, with bootstrap processes and running tablets.
  for (const string& tablet_id : tablet_ids) {
    scoped_refptr<TabletMetadata> meta;
    RETURN_NOT_OK_PREPEND(OpenTabletMeta(tablet_id, &meta),
                          "Failed to open tablet metadata for tablet: " + tablet_id);
    if (PREDICT_FALSE(meta->tablet_data_state() != TABLET_DATA_READY)) {
      RETURN_NOT_OK(HandleNonReadyTabletOnStartup(meta));
      continue;
    }
    metas.push_back(meta);
  }

  // Now submit the "Open" task for each.
  for (const scoped_refptr<TabletMetadata>& meta : metas) {
    scoped_refptr<TransitionInProgressDeleter> deleter;
    {
      std::lock_guard<rw_spinlock> lock(lock_);
      CHECK_OK(StartTabletStateTransitionUnlocked(meta->tablet_id(), "opening tablet", &deleter));
    }

    scoped_refptr<TabletReplica> replica = CreateAndRegisterTabletReplica(meta, NEW_REPLICA);
    RETURN_NOT_OK(open_tablet_pool_->SubmitFunc(boost::bind(&TSTabletManager::OpenTablet,
                                                this, meta, deleter)));
  }

  {
    std::lock_guard<rw_spinlock> lock(lock_);
    state_ = MANAGER_RUNNING;
  }

  return Status::OK();
}

Status TSTabletManager::WaitForAllBootstrapsToFinish() {
  CHECK_EQ(state(), MANAGER_RUNNING);

  open_tablet_pool_->Wait();

  shared_lock<rw_spinlock> l(lock_);
  for (const TabletMap::value_type& entry : tablet_map_) {
    if (entry.second->state() == tablet::FAILED) {
      return entry.second->error();
    }
  }

  return Status::OK();
}

Status TSTabletManager::CreateNewTablet(const string& table_id,
                                        const string& tablet_id,
                                        const Partition& partition,
                                        const string& table_name,
                                        const Schema& schema,
                                        const PartitionSchema& partition_schema,
                                        RaftConfigPB config,
                                        scoped_refptr<TabletReplica>* replica) {
  CHECK_EQ(state(), MANAGER_RUNNING);
  CHECK(IsRaftConfigMember(server_->instance_pb().permanent_uuid(), config));

  // Set the initial opid_index for a RaftConfigPB to -1.
  config.set_opid_index(consensus::kInvalidOpIdIndex);

  scoped_refptr<TransitionInProgressDeleter> deleter;
  {
    // acquire the lock in exclusive mode as we'll add a entry to the
    // transition_in_progress_ set if the lookup fails.
    std::lock_guard<rw_spinlock> lock(lock_);
    TRACE("Acquired tablet manager lock");

    // Sanity check that the tablet isn't already registered.
    scoped_refptr<TabletReplica> junk;
    if (LookupTabletUnlocked(tablet_id, &junk)) {
      return Status::AlreadyPresent("Tablet already registered", tablet_id);
    }

    // Sanity check that the tablet's creation isn't already in progress
    RETURN_NOT_OK(StartTabletStateTransitionUnlocked(tablet_id, "creating tablet", &deleter));
  }

  // Create the metadata.
  TRACE("Creating new metadata...");
  scoped_refptr<TabletMetadata> meta;
  RETURN_NOT_OK_PREPEND(
    TabletMetadata::CreateNew(fs_manager_,
                              tablet_id,
                              table_name,
                              table_id,
                              schema,
                              partition_schema,
                              partition,
                              TABLET_DATA_READY,
                              &meta),
    "Couldn't create tablet metadata");

  // We must persist the consensus metadata to disk before starting a new
  // tablet's TabletReplica and Consensus implementation.
  unique_ptr<ConsensusMetadata> cmeta;
  RETURN_NOT_OK_PREPEND(ConsensusMetadata::Create(fs_manager_, tablet_id, fs_manager_->uuid(),
                                                  config, consensus::kMinimumTerm, &cmeta),
                        "Unable to create new ConsensusMeta for tablet " + tablet_id);
  scoped_refptr<TabletReplica> new_replica = CreateAndRegisterTabletReplica(meta, NEW_REPLICA);

  // We can run this synchronously since there is nothing to bootstrap.
  RETURN_NOT_OK(open_tablet_pool_->SubmitFunc(boost::bind(&TSTabletManager::OpenTablet,
                                              this, meta, deleter)));

  if (replica) {
    *replica = new_replica;
  }
  return Status::OK();
}

Status TSTabletManager::CheckLeaderTermNotLower(const string& tablet_id,
                                                int64_t leader_term,
                                                int64_t last_logged_term) {
  if (PREDICT_FALSE(leader_term < last_logged_term)) {
    Status s = Status::InvalidArgument(
        Substitute("Leader has replica of tablet $0 with term $1, which "
                   "is lower than last-logged term $2 on local replica. Rejecting "
                   "tablet copy request",
                   tablet_id, leader_term, last_logged_term));
    LOG(WARNING) << LogPrefix(tablet_id) << "Tablet Copy: " << s.ToString();
    return s;
  }
  return Status::OK();
}

// Tablet Copy runnable that will run on a ThreadPool.
class TabletCopyRunnable : public Runnable {
 public:
  TabletCopyRunnable(TSTabletManager* ts_tablet_manager,
                     const StartTabletCopyRequestPB* req,
                     std::function<void(const Status&, TabletServerErrorPB::Code)> cb)
      : ts_tablet_manager_(ts_tablet_manager),
        req_(req),
        cb_(std::move(cb)) {
  }

  virtual ~TabletCopyRunnable() {
    // If the Runnable is destroyed without the Run() method being invoked, we
    // must invoke the user callback ourselves in order to free request
    // resources. This may happen when the ThreadPool is shut down while the
    // Runnable is enqueued.
    if (!cb_invoked_) {
      cb_(Status::ServiceUnavailable("Tablet server shutting down"),
          TabletServerErrorPB::THROTTLED);
    }
  }

  virtual void Run() override {
    ts_tablet_manager_->RunTabletCopy(req_, cb_);
    cb_invoked_ = true;
  }

  // Disable automatic invocation of the callback by the destructor.
  void DisableCallback() {
    cb_invoked_ = true;
  }

 private:
  TSTabletManager* const ts_tablet_manager_;
  const StartTabletCopyRequestPB* const req_;
  const std::function<void(const Status&, TabletServerErrorPB::Code)> cb_;
  bool cb_invoked_ = false;

  DISALLOW_COPY_AND_ASSIGN(TabletCopyRunnable);
};

void TSTabletManager::StartTabletCopy(
    const StartTabletCopyRequestPB* req,
    std::function<void(const Status&, TabletServerErrorPB::Code)> cb) {
  shared_ptr<TabletCopyRunnable> runnable(new TabletCopyRunnable(this, req, cb));
  Status s = tablet_copy_pool_->Submit(runnable);
  if (PREDICT_TRUE(s.ok())) {
    return;
  }

  // We were unable to submit the TabletCopyRunnable to the ThreadPool. We will
  // invoke the callback ourselves, so disable the automatic callback mechanism.
  runnable->DisableCallback();

  // Thread pool is at capacity.
  if (s.IsServiceUnavailable()) {
    cb(s, TabletServerErrorPB::THROTTLED);
    return;
  }
  cb(s, TabletServerErrorPB::UNKNOWN_ERROR);
}

#define CALLBACK_AND_RETURN(status) \
  do { \
    cb(status, error_code); \
    return; \
  } while (0)

#define CALLBACK_RETURN_NOT_OK(expr) \
  do { \
    Status _s = (expr); \
    if (PREDICT_FALSE(!_s.ok())) { \
      CALLBACK_AND_RETURN(_s); \
    } \
  } while (0)

void TSTabletManager::RunTabletCopy(
    const StartTabletCopyRequestPB* req,
    std::function<void(const Status&, TabletServerErrorPB::Code)> cb) {

  TabletServerErrorPB::Code error_code = TabletServerErrorPB::UNKNOWN_ERROR;

  // Copy these strings so they stay valid even after responding to the request.
  string tablet_id = req->tablet_id(); // NOLINT(*)
  string copy_source_uuid = req->copy_peer_uuid(); // NOLINT(*)
  HostPort copy_source_addr;
  CALLBACK_RETURN_NOT_OK(HostPortFromPB(req->copy_peer_addr(), &copy_source_addr));
  int64_t leader_term = req->caller_term();

  scoped_refptr<TabletReplica> old_replica;
  scoped_refptr<TabletMetadata> meta;
  bool replacing_tablet = false;
  scoped_refptr<TransitionInProgressDeleter> deleter;
  {
    std::lock_guard<rw_spinlock> lock(lock_);
    if (LookupTabletUnlocked(tablet_id, &old_replica)) {
      meta = old_replica->tablet_metadata();
      replacing_tablet = true;
    }
    Status ret = StartTabletStateTransitionUnlocked(tablet_id, "copying tablet",
                                                    &deleter);
    if (!ret.ok()) {
      error_code = TabletServerErrorPB::ALREADY_INPROGRESS;
      CALLBACK_AND_RETURN(ret);
    }
  }

  if (replacing_tablet) {
    // Make sure the existing tablet replica is shut down and tombstoned.
    TabletDataState data_state = meta->tablet_data_state();
    switch (data_state) {
      case TABLET_DATA_COPYING:
        // This should not be possible due to the transition_in_progress_ "lock".
        LOG(FATAL) << LogPrefix(tablet_id) << "Tablet Copy: "
                   << "Found tablet in TABLET_DATA_COPYING state during StartTabletCopy()";
      case TABLET_DATA_TOMBSTONED: {
        int64_t last_logged_term = meta->tombstone_last_logged_opid().term();
        CALLBACK_RETURN_NOT_OK(CheckLeaderTermNotLower(tablet_id, leader_term, last_logged_term));
        break;
      }
      case TABLET_DATA_READY: {
        Log* log = old_replica->log();
        if (!log) {
          CALLBACK_AND_RETURN(
              Status::IllegalState("Log unavailable. Tablet is not running", tablet_id));
        }
        OpId last_logged_opid;
        log->GetLatestEntryOpId(&last_logged_opid);
        int64_t last_logged_term = last_logged_opid.term();
        CALLBACK_RETURN_NOT_OK(CheckLeaderTermNotLower(tablet_id, leader_term, last_logged_term));

        // Tombstone the tablet and store the last-logged OpId.
        old_replica->Shutdown();
        // TODO(mpercy): Because we begin shutdown of the tablet after we check our
        // last-logged term against the leader's term, there may be operations
        // in flight and it may be possible for the same check in the tablet
        // copy client Start() method to fail. This will leave the replica in
        // a tombstoned state, and then the leader with the latest log entries
        // will simply tablet copy this replica again. We could try to
        // check again after calling Shutdown(), and if the check fails, try to
        // reopen the tablet. For now, we live with the (unlikely) race.
        Status s = DeleteTabletData(meta, TABLET_DATA_TOMBSTONED, last_logged_opid);
        if (PREDICT_FALSE(!s.ok())) {
          CALLBACK_AND_RETURN(
              s.CloneAndPrepend(Substitute("Unable to delete on-disk data from tablet $0",
                                           tablet_id)));
        }
        break;
      }
      default:
        CALLBACK_AND_RETURN(Status::IllegalState(
            Substitute("Found tablet in unsupported state for tablet copy. "
                        "Tablet: $0, tablet data state: $1",
                        tablet_id, TabletDataState_Name(data_state))));
    }
  }

  const string kSrcPeerInfo = Substitute("$0 ($1)", copy_source_uuid, copy_source_addr.ToString());
  string init_msg = LogPrefix(tablet_id) +
                    Substitute("Initiating tablet copy from peer $0", kSrcPeerInfo);
  LOG(INFO) << init_msg;
  TRACE(init_msg);

  TabletCopyClient tc_client(tablet_id, fs_manager_, server_->messenger());

  // Download and persist the remote superblock in TABLET_DATA_COPYING state.
  if (replacing_tablet) {
    CALLBACK_RETURN_NOT_OK(tc_client.SetTabletToReplace(meta, leader_term));
  }
  CALLBACK_RETURN_NOT_OK(tc_client.Start(copy_source_addr, &meta));

  // From this point onward, the superblock is persisted in TABLET_DATA_COPYING
  // state, and we need to tombtone the tablet if additional steps prior to
  // getting to a TABLET_DATA_READY state fail.

  // Registering a non-initialized TabletReplica offers visibility through the Web UI.
  RegisterTabletReplicaMode mode = replacing_tablet ? REPLACEMENT_REPLICA : NEW_REPLICA;
  scoped_refptr<TabletReplica> replica = CreateAndRegisterTabletReplica(meta, mode);

  // Now we invoke the StartTabletCopy callback and respond success to the
  // remote caller. Then we proceed to do most of the actual tablet copying work.
  cb(Status::OK(), TabletServerErrorPB::UNKNOWN_ERROR);
  cb = [](const Status&, TabletServerErrorPB::Code) {
    LOG(FATAL) << "Callback invoked twice from TSTabletManager::RunTabletCopy()";
  };

  // From this point onward, we do not notify the caller about progress or success.

  // Download all of the remote files.
  Status s = tc_client.FetchAll(implicit_cast<TabletStatusListener*>(replica.get()));
  if (!s.ok()) {
    LOG(WARNING) << LogPrefix(tablet_id) << "Tablet Copy: Unable to fetch data from remote peer "
                                         << kSrcPeerInfo << ": " << s.ToString();
    return;
  }

  MAYBE_FAULT(FLAGS_fault_crash_after_tc_files_fetched);

  // Write out the last files to make the new replica visible and update the
  // TabletDataState in the superblock to TABLET_DATA_READY.
  s = tc_client.Finish();
  if (!s.ok()) {
    LOG(WARNING) << LogPrefix(tablet_id) << "Tablet Copy: Failure calling Finish(): "
                                         << s.ToString();
    return;
  }

  // startup it's still in a valid, fully-copied state.
  OpenTablet(meta, deleter);
}

// Create and register a new TabletReplica, given tablet metadata.
scoped_refptr<TabletReplica> TSTabletManager::CreateAndRegisterTabletReplica(
    const scoped_refptr<TabletMetadata>& meta, RegisterTabletReplicaMode mode) {
  scoped_refptr<TabletReplica> replica(
      new TabletReplica(meta,
                        local_peer_pb_,
                        apply_pool_.get(),
                        Bind(&TSTabletManager::MarkTabletDirty,
                             Unretained(this),
                             meta->tablet_id())));
  RegisterTablet(meta->tablet_id(), replica, mode);
  return replica;
}

Status TSTabletManager::DeleteTablet(
    const string& tablet_id,
    TabletDataState delete_type,
    const boost::optional<int64_t>& cas_config_opid_index_less_or_equal,
    boost::optional<TabletServerErrorPB::Code>* error_code) {

  if (delete_type != TABLET_DATA_DELETED && delete_type != TABLET_DATA_TOMBSTONED) {
    return Status::InvalidArgument("DeleteTablet() requires an argument that is one of "
                                   "TABLET_DATA_DELETED or TABLET_DATA_TOMBSTONED",
                                   Substitute("Given: $0 ($1)",
                                              TabletDataState_Name(delete_type), delete_type));
  }

  TRACE("Deleting tablet $0", tablet_id);

  scoped_refptr<TabletReplica> replica;
  scoped_refptr<TransitionInProgressDeleter> deleter;
  {
    // Acquire the lock in exclusive mode as we'll add a entry to the
    // transition_in_progress_ map.
    std::lock_guard<rw_spinlock> lock(lock_);
    TRACE("Acquired tablet manager lock");
    RETURN_NOT_OK(CheckRunningUnlocked(error_code));

    if (!LookupTabletUnlocked(tablet_id, &replica)) {
      *error_code = TabletServerErrorPB::TABLET_NOT_FOUND;
      return Status::NotFound("Tablet not found", tablet_id);
    }
    // Sanity check that the tablet's deletion isn't already in progress
    Status s = StartTabletStateTransitionUnlocked(tablet_id, "deleting tablet", &deleter);
    if (PREDICT_FALSE(!s.ok())) {
      *error_code = TabletServerErrorPB::TABLET_NOT_RUNNING;
      return s;
    }
  }

  // If the tablet is already deleted, the CAS check isn't possible because
  // consensus and therefore the log is not available.
  TabletDataState data_state = replica->tablet_metadata()->tablet_data_state();
  bool tablet_deleted = (data_state == TABLET_DATA_DELETED || data_state == TABLET_DATA_TOMBSTONED);

  // They specified an "atomic" delete. Check the committed config's opid_index.
  // TODO: There's actually a race here between the check and shutdown, but
  // it's tricky to fix. We could try checking again after the shutdown and
  // restarting the tablet if the local replica committed a higher config
  // change op during that time, or potentially something else more invasive.
  if (cas_config_opid_index_less_or_equal && !tablet_deleted) {
    scoped_refptr<consensus::Consensus> consensus = replica->shared_consensus();
    if (!consensus) {
      *error_code = TabletServerErrorPB::TABLET_NOT_RUNNING;
      return Status::IllegalState("Consensus not available. Tablet shutting down");
    }
    RaftConfigPB committed_config = consensus->CommittedConfig();
    if (committed_config.opid_index() > *cas_config_opid_index_less_or_equal) {
      *error_code = TabletServerErrorPB::CAS_FAILED;
      return Status::IllegalState(Substitute("Request specified cas_config_opid_index_less_or_equal"
                                             " of $0 but the committed config has opid_index of $1",
                                             *cas_config_opid_index_less_or_equal,
                                             committed_config.opid_index()));
    }
  }

  replica->Shutdown();

  boost::optional<OpId> opt_last_logged_opid;
  if (replica->log()) {
    OpId last_logged_opid;
    replica->log()->GetLatestEntryOpId(&last_logged_opid);
    opt_last_logged_opid = last_logged_opid;
  }

  Status s = DeleteTabletData(replica->tablet_metadata(), delete_type, opt_last_logged_opid);
  if (PREDICT_FALSE(!s.ok())) {
    s = s.CloneAndPrepend(Substitute("Unable to delete on-disk data from tablet $0",
                                     tablet_id));
    LOG(WARNING) << s.ToString();
    replica->SetFailed(s);
    return s;
  }

  replica->StatusMessage("Deleted tablet blocks from disk");

  // We only remove DELETED tablets from the tablet map.
  if (delete_type == TABLET_DATA_DELETED) {
    std::lock_guard<rw_spinlock> lock(lock_);
    RETURN_NOT_OK(CheckRunningUnlocked(error_code));
    CHECK_EQ(1, tablet_map_.erase(tablet_id)) << tablet_id;
    InsertOrDie(&perm_deleted_tablet_ids_, tablet_id);
  }

  return Status::OK();
}

string TSTabletManager::LogPrefix(const string& tablet_id, FsManager *fs_manager) {
  DCHECK(fs_manager != nullptr);
  return Substitute("T $0 P $1: ", tablet_id, fs_manager->uuid());
}

Status TSTabletManager::CheckRunningUnlocked(
    boost::optional<TabletServerErrorPB::Code>* error_code) const {
  if (state_ == MANAGER_RUNNING) {
    return Status::OK();
  }
  *error_code = TabletServerErrorPB::TABLET_NOT_RUNNING;
  return Status::ServiceUnavailable(Substitute("Tablet Manager is not running: $0",
                                               TSTabletManagerStatePB_Name(state_)));
}

Status TSTabletManager::StartTabletStateTransitionUnlocked(
    const string& tablet_id,
    const string& reason,
    scoped_refptr<TransitionInProgressDeleter>* deleter) {
  DCHECK(lock_.is_write_locked());
  if (ContainsKey(perm_deleted_tablet_ids_, tablet_id)) {
    // When a table is deleted, the master sends a DeleteTablet() RPC to every
    // replica of every tablet with the TABLET_DATA_DELETED parameter, which
    // indicates a "permanent" tablet deletion. If a follower services
    // DeleteTablet() before the leader does, it's possible for the leader to
    // react to the missing replica by asking the follower to tablet copy
    // itself.
    //
    // If the tablet was permanently deleted, we should not allow it to
    // transition back to "liveness" because that can result in flapping back
    // and forth between deletion and tablet copying.
    return Status::IllegalState(
        Substitute("Tablet $0 was permanently deleted. Cannot transition from state $1.",
                   tablet_id, TabletDataState_Name(TABLET_DATA_DELETED)));
  }

  if (!InsertIfNotPresent(&transition_in_progress_, tablet_id, reason)) {
    return Status::IllegalState(
        Substitute("State transition of tablet $0 already in progress: $1",
                    tablet_id, transition_in_progress_[tablet_id]));
  }
  deleter->reset(new TransitionInProgressDeleter(&transition_in_progress_, &lock_, tablet_id));
  return Status::OK();
}

Status TSTabletManager::OpenTabletMeta(const string& tablet_id,
                                       scoped_refptr<TabletMetadata>* metadata) {
  LOG(INFO) << LogPrefix(tablet_id) << "Loading tablet metadata";
  TRACE("Loading metadata...");
  scoped_refptr<TabletMetadata> meta;
  RETURN_NOT_OK_PREPEND(TabletMetadata::Load(fs_manager_, tablet_id, &meta),
                        strings::Substitute("Failed to load tablet metadata for tablet id $0",
                                            tablet_id));
  TRACE("Metadata loaded");
  metadata->swap(meta);
  return Status::OK();
}

void TSTabletManager::OpenTablet(const scoped_refptr<TabletMetadata>& meta,
                                 const scoped_refptr<TransitionInProgressDeleter>& deleter) {
  string tablet_id = meta->tablet_id();
  TRACE_EVENT1("tserver", "TSTabletManager::OpenTablet",
               "tablet_id", tablet_id);

  scoped_refptr<TabletReplica> replica;
  CHECK(LookupTablet(tablet_id, &replica))
      << "Tablet not registered prior to OpenTabletAsync call: " << tablet_id;

  shared_ptr<Tablet> tablet;
  scoped_refptr<Log> log;

  LOG(INFO) << LogPrefix(tablet_id) << "Bootstrapping tablet";
  TRACE("Bootstrapping tablet");

  consensus::ConsensusBootstrapInfo bootstrap_info;
  Status s;
  LOG_TIMING_PREFIX(INFO, LogPrefix(tablet_id), "bootstrapping tablet") {
    // Disable tracing for the bootstrap, since this would result in
    // potentially millions of transaction traces being attached to the
    // TabletCopy trace.
    ADOPT_TRACE(nullptr);
    // TODO: handle crash mid-creation of tablet? do we ever end up with a
    // partially created tablet here?
    replica->SetBootstrapping();
    s = BootstrapTablet(meta,
                        scoped_refptr<server::Clock>(server_->clock()),
                        server_->mem_tracker(),
                        server_->result_tracker(),
                        metric_registry_,
                        implicit_cast<TabletStatusListener*>(replica.get()),
                        &tablet,
                        &log,
                        replica->log_anchor_registry(),
                        &bootstrap_info);
    if (!s.ok()) {
      LOG(ERROR) << LogPrefix(tablet_id) << "Tablet failed to bootstrap: "
                 << s.ToString();
      replica->SetFailed(s);
      return;
    }
  }

  MonoTime start(MonoTime::Now());
  LOG_TIMING_PREFIX(INFO, LogPrefix(tablet_id), "starting tablet") {
    TRACE("Initializing tablet replica");
    s =  replica->Init(tablet,
                       scoped_refptr<server::Clock>(server_->clock()),
                       server_->messenger(),
                       server_->result_tracker(),
                       log,
                       tablet->GetMetricEntity());

    if (!s.ok()) {
      LOG(ERROR) << LogPrefix(tablet_id) << "Tablet failed to init: "
                 << s.ToString();
      replica->SetFailed(s);
      return;
    }

    TRACE("Starting tablet replica");
    s = replica->Start(bootstrap_info);
    if (!s.ok()) {
      LOG(ERROR) << LogPrefix(tablet_id) << "Tablet failed to start: "
                 << s.ToString();
      replica->SetFailed(s);
      return;
    }

    replica->RegisterMaintenanceOps(server_->maintenance_manager());
  }

  int elapsed_ms = (MonoTime::Now() - start).ToMilliseconds();
  if (elapsed_ms > FLAGS_tablet_start_warn_threshold_ms) {
    LOG(WARNING) << LogPrefix(tablet_id) << "Tablet startup took " << elapsed_ms << "ms";
    if (Trace::CurrentTrace()) {
      LOG(WARNING) << LogPrefix(tablet_id) << "Trace:" << std::endl
                   << Trace::CurrentTrace()->DumpToString();
    }
  }
}

void TSTabletManager::Shutdown() {
  {
    std::lock_guard<rw_spinlock> lock(lock_);
    switch (state_) {
      case MANAGER_QUIESCING: {
        VLOG(1) << "Tablet manager shut down already in progress..";
        return;
      }
      case MANAGER_SHUTDOWN: {
        VLOG(1) << "Tablet manager has already been shut down.";
        return;
      }
      case MANAGER_INITIALIZING:
      case MANAGER_RUNNING: {
        LOG(INFO) << "Shutting down tablet manager...";
        state_ = MANAGER_QUIESCING;
        break;
      }
      default: {
        LOG(FATAL) << "Invalid state: " << TSTabletManagerStatePB_Name(state_);
      }
    }
  }

  // Stop copying tablets.
  // TODO(mpercy): Cancel all outstanding tablet copy tasks (KUDU-1795).
  tablet_copy_pool_->Shutdown();

  // Shut down the bootstrap pool, so no new tablets are registered after this point.
  open_tablet_pool_->Shutdown();

  // Take a snapshot of the replicas list -- that way we don't have to hold
  // on to the lock while shutting them down, which might cause a lock
  // inversion. (see KUDU-308 for example).
  vector<scoped_refptr<TabletReplica> > replicas_to_shutdown;
  GetTabletReplicas(&replicas_to_shutdown);

  for (const scoped_refptr<TabletReplica>& replica : replicas_to_shutdown) {
    replica->Shutdown();
  }

  // Shut down the apply pool.
  apply_pool_->Shutdown();

  {
    std::lock_guard<rw_spinlock> l(lock_);
    // We don't expect anyone else to be modifying the map after we start the
    // shut down process.
    CHECK_EQ(tablet_map_.size(), replicas_to_shutdown.size())
      << "Map contents changed during shutdown!";
    tablet_map_.clear();

    state_ = MANAGER_SHUTDOWN;
  }
}

void TSTabletManager::RegisterTablet(const std::string& tablet_id,
                                     const scoped_refptr<TabletReplica>& replica,
                                     RegisterTabletReplicaMode mode) {
  std::lock_guard<rw_spinlock> lock(lock_);
  // If we are replacing a tablet replica, we delete the existing one first.
  if (mode == REPLACEMENT_REPLICA && tablet_map_.erase(tablet_id) != 1) {
    LOG(FATAL) << "Unable to remove previous tablet replica " << tablet_id << ": not registered!";
  }
  if (!InsertIfNotPresent(&tablet_map_, tablet_id, replica)) {
    LOG(FATAL) << "Unable to register tablet replica " << tablet_id << ": already registered!";
  }

  TabletDataState data_state = replica->tablet_metadata()->tablet_data_state();
  LOG(INFO) << LogPrefix(tablet_id) << Substitute("Registered tablet (data state: $0)",
                                                  TabletDataState_Name(data_state));
}

bool TSTabletManager::LookupTablet(const string& tablet_id,
                                   scoped_refptr<TabletReplica>* replica) const {
  shared_lock<rw_spinlock> l(lock_);
  return LookupTabletUnlocked(tablet_id, replica);
}

bool TSTabletManager::LookupTabletUnlocked(const string& tablet_id,
                                           scoped_refptr<TabletReplica>* replica) const {
  const scoped_refptr<TabletReplica>* found = FindOrNull(tablet_map_, tablet_id);
  if (!found) {
    return false;
  }
  *replica = *found;
  return true;
}

Status TSTabletManager::GetTabletReplica(const string& tablet_id,
                                         scoped_refptr<tablet::TabletReplica>* replica) const {
  if (!LookupTablet(tablet_id, replica)) {
    return Status::NotFound("Tablet not found", tablet_id);
  }
  TabletDataState data_state = (*replica)->tablet_metadata()->tablet_data_state();
  if (data_state != TABLET_DATA_READY) {
    return Status::IllegalState("Tablet data state not TABLET_DATA_READY: " +
                                TabletDataState_Name(data_state),
                                tablet_id);
  }
  return Status::OK();
}

const NodeInstancePB& TSTabletManager::NodeInstance() const {
  return server_->instance_pb();
}

void TSTabletManager::GetTabletReplicas(vector<scoped_refptr<TabletReplica> >* replicas) const {
  shared_lock<rw_spinlock> l(lock_);
  AppendValuesFromMap(tablet_map_, replicas);
}

void TSTabletManager::MarkTabletDirty(const std::string& tablet_id, const std::string& reason) {
  VLOG(2) << Substitute("$0 Marking dirty. Reason: $1. Will report this "
      "tablet to the Master in the next heartbeat",
      LogPrefix(tablet_id), reason);
  server_->heartbeater()->MarkTabletDirty(tablet_id, reason);
  server_->heartbeater()->TriggerASAP();
}

int TSTabletManager::GetNumLiveTablets() const {
  int count = 0;
  shared_lock<rw_spinlock> l(lock_);
  for (const auto& entry : tablet_map_) {
    tablet::TabletStatePB state = entry.second->state();
    if (state == tablet::BOOTSTRAPPING ||
        state == tablet::RUNNING) {
      count++;
    }
  }
  return count;
}

void TSTabletManager::InitLocalRaftPeerPB() {
  DCHECK_EQ(state(), MANAGER_INITIALIZING);
  local_peer_pb_.set_permanent_uuid(fs_manager_->uuid());
  Sockaddr addr = server_->first_rpc_address();
  HostPort hp;
  CHECK_OK(HostPortFromSockaddrReplaceWildcard(addr, &hp));
  CHECK_OK(HostPortToPB(hp, local_peer_pb_.mutable_last_known_addr()));
}

void TSTabletManager::CreateReportedTabletPB(const string& tablet_id,
                                             const scoped_refptr<TabletReplica>& replica,
                                             ReportedTabletPB* reported_tablet) const {
  reported_tablet->set_tablet_id(tablet_id);
  reported_tablet->set_state(replica->state());
  reported_tablet->set_tablet_data_state(replica->tablet_metadata()->tablet_data_state());
  if (replica->state() == tablet::FAILED) {
    AppStatusPB* error_status = reported_tablet->mutable_error();
    StatusToPB(replica->error(), error_status);
  }
  reported_tablet->set_schema_version(replica->tablet_metadata()->schema_version());

  // We cannot get consensus state information unless the TabletReplica is running.
  scoped_refptr<consensus::Consensus> consensus = replica->shared_consensus();
  if (consensus) {
    *reported_tablet->mutable_committed_consensus_state() =
        consensus->ConsensusState(consensus::CONSENSUS_CONFIG_COMMITTED);
  }
}

void TSTabletManager::PopulateFullTabletReport(TabletReportPB* report) const {
  shared_lock<rw_spinlock> shared_lock(lock_);
  for (const auto& e : tablet_map_) {
    CreateReportedTabletPB(e.first, e.second, report->add_updated_tablets());
  }
}

void TSTabletManager::PopulateIncrementalTabletReport(TabletReportPB* report,
                                                      const vector<string>& tablet_ids) const {
  shared_lock<rw_spinlock> shared_lock(lock_);
  for (const auto& id : tablet_ids) {
    const scoped_refptr<tablet::TabletReplica>* replica =
        FindOrNull(tablet_map_, id);
    if (replica) {
      // Dirty entry, report on it.
      CreateReportedTabletPB(id, *replica, report->add_updated_tablets());
    } else {
      // Removed.
      report->add_removed_tablet_ids(id);
    }
  }
}

Status TSTabletManager::HandleNonReadyTabletOnStartup(const scoped_refptr<TabletMetadata>& meta) {
  const string& tablet_id = meta->tablet_id();
  TabletDataState data_state = meta->tablet_data_state();
  CHECK(data_state == TABLET_DATA_DELETED ||
        data_state == TABLET_DATA_TOMBSTONED ||
        data_state == TABLET_DATA_COPYING)
      << "Unexpected TabletDataState in tablet " << tablet_id << ": "
      << TabletDataState_Name(data_state) << " (" << data_state << ")";

  // If the tablet is already fully tombstoned with no remaining data or WAL,
  // then no need to roll anything forward.
  bool skip_deletion = meta->IsTombstonedWithNoBlocks() &&
      !Log::HasOnDiskData(meta->fs_manager(), tablet_id);

  LOG_IF(WARNING, !skip_deletion)
      << LogPrefix(tablet_id) << "Tablet Manager startup: Rolling forward tablet deletion "
      << "of type " << TabletDataState_Name(data_state);

  if (data_state == TABLET_DATA_COPYING) {
    // We tombstone tablets that failed to copy.
    data_state = TABLET_DATA_TOMBSTONED;
  }

  if (!skip_deletion) {
    // Passing no OpId will retain the last_logged_opid that was previously in the metadata.
    RETURN_NOT_OK(DeleteTabletData(meta, data_state, boost::none));
  }

  // Register TOMBSTONED tablets so that they get reported to the Master, which
  // allows us to permanently delete replica tombstones when a table gets
  // deleted.
  if (data_state == TABLET_DATA_TOMBSTONED) {
    CreateAndRegisterTabletReplica(meta, NEW_REPLICA);
  }

  return Status::OK();
}

Status TSTabletManager::DeleteTabletData(const scoped_refptr<TabletMetadata>& meta,
                                         TabletDataState data_state,
                                         const boost::optional<OpId>& last_logged_opid) {
  const string& tablet_id = meta->tablet_id();
  LOG(INFO) << LogPrefix(tablet_id, meta->fs_manager())
            << "Deleting tablet data with delete state "
            << TabletDataState_Name(data_state);
  CHECK(data_state == TABLET_DATA_DELETED ||
        data_state == TABLET_DATA_TOMBSTONED ||
        data_state == TABLET_DATA_COPYING)
      << "Unexpected data_state to delete tablet " << meta->tablet_id() << ": "
      << TabletDataState_Name(data_state) << " (" << data_state << ")";

  // Note: Passing an unset 'last_logged_opid' will retain the last_logged_opid
  // that was previously in the metadata.
  RETURN_NOT_OK(meta->DeleteTabletData(data_state, last_logged_opid));
  LOG(INFO) << LogPrefix(tablet_id, meta->fs_manager())
            << "Tablet deleted. Last logged OpId: "
            << meta->tombstone_last_logged_opid();
  MAYBE_FAULT(FLAGS_fault_crash_after_blocks_deleted);

  RETURN_NOT_OK(Log::DeleteOnDiskData(meta->fs_manager(), meta->tablet_id()));
  MAYBE_FAULT(FLAGS_fault_crash_after_wal_deleted);

  // We do not delete the superblock or the consensus metadata when tombstoning
  // a tablet or marking it as entering the tablet copy process.
  if (data_state == TABLET_DATA_COPYING ||
      data_state == TABLET_DATA_TOMBSTONED) {
    return Status::OK();
  }

  // Only TABLET_DATA_DELETED tablets get this far.
  DCHECK_EQ(TABLET_DATA_DELETED, data_state);
  RETURN_NOT_OK(ConsensusMetadata::DeleteOnDiskData(meta->fs_manager(), meta->tablet_id()));
  MAYBE_FAULT(FLAGS_fault_crash_after_cmeta_deleted);
  return meta->DeleteSuperBlock();
}

TransitionInProgressDeleter::TransitionInProgressDeleter(
    TransitionInProgressMap* map, rw_spinlock* lock, string entry)
    : in_progress_(map), lock_(lock), entry_(std::move(entry)) {}

TransitionInProgressDeleter::~TransitionInProgressDeleter() {
  std::lock_guard<rw_spinlock> lock(*lock_);
  CHECK(in_progress_->erase(entry_));
}

} // namespace tserver
} // namespace kudu
