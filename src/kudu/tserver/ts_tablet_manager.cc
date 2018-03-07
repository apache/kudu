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

#include <cstdint>
#include <memory>
#include <mutex>
#include <ostream>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <boost/bind.hpp> // IWYU pragma: keep
#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/clock/clock.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/consensus_meta_manager.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.pb.h"
#include "kudu/rpc/result_tracker.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_bootstrap.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/heartbeater.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/threadpool.h"
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

DEFINE_int32(num_tablets_to_delete_simultaneously, 0,
             "Number of threads available to delete tablets. If this is set to 0 (the "
             "default), then the number of delete threads will be set based on the number "
             "of data directories. If the data directories are on some very fast storage "
             "device such as SSD or a RAID array, it may make sense to manually tune this.");
TAG_FLAG(num_tablets_to_delete_simultaneously, advanced);

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

DEFINE_int32(tablet_state_walk_min_period_ms, 1000,
             "Minimum amount of time in milliseconds between walks of the "
             "tablet map to update tablet state counts.");
TAG_FLAG(tablet_state_walk_min_period_ms, advanced);

DEFINE_int32(delete_tablet_inject_latency_ms, 0,
             "Amount of delay in milliseconds to inject into delete tablet operations.");
TAG_FLAG(delete_tablet_inject_latency_ms, unsafe);

DECLARE_bool(raft_prepare_replacement_before_eviction);

METRIC_DEFINE_gauge_int32(server, tablets_num_not_initialized,
                          "Number of Not Initialized Tablets",
                          kudu::MetricUnit::kTablets,
                          "Number of tablets currently not initialized");

METRIC_DEFINE_gauge_int32(server, tablets_num_initialized,
                          "Number of Initialized Tablets",
                          kudu::MetricUnit::kTablets,
                          "Number of tablets currently initialized");

METRIC_DEFINE_gauge_int32(server, tablets_num_bootstrapping,
                          "Number of Bootstrapping Tablets",
                          kudu::MetricUnit::kTablets,
                          "Number of tablets currently bootstrapping");

METRIC_DEFINE_gauge_int32(server, tablets_num_running,
                          "Number of Running Tablets",
                          kudu::MetricUnit::kTablets,
                          "Number of tablets currently running");

METRIC_DEFINE_gauge_int32(server, tablets_num_failed,
                          "Number of Failed Tablets",
                          kudu::MetricUnit::kTablets,
                          "Number of failed tablets");

METRIC_DEFINE_gauge_int32(server, tablets_num_stopping,
                          "Number of Stopping Tablets",
                          kudu::MetricUnit::kTablets,
                          "Number of tablets currently stopping");

METRIC_DEFINE_gauge_int32(server, tablets_num_stopped,
                          "Number of Stopped Tablets",
                          kudu::MetricUnit::kTablets,
                          "Number of tablets currently stopped");

METRIC_DEFINE_gauge_int32(server, tablets_num_shutdown,
                          "Number of Shut Down Tablets",
                          kudu::MetricUnit::kTablets,
                          "Number of tablets currently shut down");

using std::set;
using std::shared_ptr;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {

using consensus::ConsensusMetadata;
using consensus::ConsensusMetadataCreateMode;
using consensus::ConsensusMetadataManager;
using consensus::ConsensusStatePB;
using consensus::EXCLUDE_HEALTH_REPORT;
using consensus::INCLUDE_HEALTH_REPORT;
using consensus::OpId;
using consensus::OpIdToString;
using consensus::RECEIVED_OPID;
using consensus::RaftConfigPB;
using consensus::RaftConsensus;
using consensus::StartTabletCopyRequestPB;
using consensus::kMinimumTerm;
using fs::DataDirManager;
using log::Log;
using master::ReportedTabletPB;
using master::TabletReportPB;
using tablet::Tablet;
using tablet::TABLET_DATA_COPYING;
using tablet::TABLET_DATA_DELETED;
using tablet::TABLET_DATA_READY;
using tablet::TABLET_DATA_TOMBSTONED;
using tablet::TabletDataState;
using tablet::TabletMetadata;
using tablet::TabletReplica;
using tserver::TabletCopyClient;

namespace tserver {

TSTabletManager::TSTabletManager(TabletServer* server)
  : fs_manager_(server->fs_manager()),
    cmeta_manager_(new ConsensusMetadataManager(fs_manager_)),
    server_(server),
    metric_registry_(server->metric_registry()),
    tablet_copy_metrics_(server->metric_entity()),
    state_(MANAGER_INITIALIZING) {
  METRIC_tablets_num_not_initialized.InstantiateFunctionGauge(
          server->metric_entity(),
          Bind(&TSTabletManager::RefreshTabletStateCacheAndReturnCount,
               Unretained(this), tablet::NOT_INITIALIZED))
      ->AutoDetach(&metric_detacher_);
  METRIC_tablets_num_initialized.InstantiateFunctionGauge(
          server->metric_entity(),
          Bind(&TSTabletManager::RefreshTabletStateCacheAndReturnCount,
               Unretained(this), tablet::INITIALIZED))
      ->AutoDetach(&metric_detacher_);
  METRIC_tablets_num_bootstrapping.InstantiateFunctionGauge(
          server->metric_entity(),
          Bind(&TSTabletManager::RefreshTabletStateCacheAndReturnCount,
               Unretained(this), tablet::BOOTSTRAPPING))
      ->AutoDetach(&metric_detacher_);
  METRIC_tablets_num_running.InstantiateFunctionGauge(
          server->metric_entity(),
          Bind(&TSTabletManager::RefreshTabletStateCacheAndReturnCount,
               Unretained(this), tablet::RUNNING))
      ->AutoDetach(&metric_detacher_);
  METRIC_tablets_num_failed.InstantiateFunctionGauge(
          server->metric_entity(),
          Bind(&TSTabletManager::RefreshTabletStateCacheAndReturnCount,
               Unretained(this), tablet::FAILED))
      ->AutoDetach(&metric_detacher_);
  METRIC_tablets_num_stopping.InstantiateFunctionGauge(
          server->metric_entity(),
          Bind(&TSTabletManager::RefreshTabletStateCacheAndReturnCount,
               Unretained(this), tablet::STOPPING))
      ->AutoDetach(&metric_detacher_);
  METRIC_tablets_num_stopped.InstantiateFunctionGauge(
          server->metric_entity(),
          Bind(&TSTabletManager::RefreshTabletStateCacheAndReturnCount,
               Unretained(this), tablet::STOPPED))
      ->AutoDetach(&metric_detacher_);
  METRIC_tablets_num_shutdown.InstantiateFunctionGauge(
          server->metric_entity(),
          Bind(&TSTabletManager::RefreshTabletStateCacheAndReturnCount,
               Unretained(this), tablet::SHUTDOWN))
      ->AutoDetach(&metric_detacher_);
}

// Base class for Runnables submitted against TSTabletManager threadpools whose
// whose callback must fire, for example if the callback responds to an RPC.
class TabletManagerRunnable : public Runnable {
public:
  TabletManagerRunnable(TSTabletManager* ts_tablet_manager,
                        std::function<void(const Status&, TabletServerErrorPB::Code)> cb)
      : ts_tablet_manager_(ts_tablet_manager),
        cb_(std::move(cb)) {
  }

  virtual ~TabletManagerRunnable() {
    // If the Runnable is destroyed without the Run() method being invoked, we
    // must invoke the user callback ourselves in order to free request
    // resources. This may happen when the ThreadPool is shut down while the
    // Runnable is enqueued.
    if (!cb_invoked_) {
      cb_(Status::ServiceUnavailable("Tablet server shutting down"),
          TabletServerErrorPB::THROTTLED);
    }
  }

  // Disable automatic invocation of the callback by the destructor.
  // Does not disable invocation of the callback by Run().
  void DisableCallback() {
    cb_invoked_ = true;
  }

protected:
  TSTabletManager* const ts_tablet_manager_;
  const std::function<void(const Status&, TabletServerErrorPB::Code)> cb_;
  bool cb_invoked_ = false;

  DISALLOW_COPY_AND_ASSIGN(TabletManagerRunnable);
};

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

  // Start the threadpools we'll use to open and delete tablets.
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
  int max_delete_threads = FLAGS_num_tablets_to_delete_simultaneously;
  if (max_delete_threads == 0) {
    // Default to the number of disks.
    max_delete_threads = fs_manager_->GetDataRootDirs().size();
  }
  RETURN_NOT_OK(ThreadPoolBuilder("tablet-delete")
                .set_max_threads(max_delete_threads)
                .Build(&delete_tablet_pool_));

  // Search for tablets in the metadata dir.
  vector<string> tablet_ids;
  RETURN_NOT_OK(fs_manager_->ListTabletIds(&tablet_ids));

  InitLocalRaftPeerPB();

  vector<scoped_refptr<TabletMetadata> > metas;

  // First, load all of the tablet metadata. We do this before we start
  // submitting the actual OpenTablet() tasks so that we don't have to compete
  // for disk resources, etc, with bootstrap processes and running tablets.
  int loaded_count = 0;
  for (const string& tablet_id : tablet_ids) {
    KLOG_EVERY_N_SECS(INFO, 1) << Substitute("Loading tablet metadata ($0/$1 complete)",
                                             loaded_count, tablet_ids.size());
    scoped_refptr<TabletMetadata> meta;
    RETURN_NOT_OK_PREPEND(OpenTabletMeta(tablet_id, &meta),
                          "Failed to open tablet metadata for tablet: " + tablet_id);
    loaded_count++;
    if (PREDICT_FALSE(meta->tablet_data_state() != TABLET_DATA_READY)) {
      RETURN_NOT_OK(HandleNonReadyTabletOnStartup(meta));
      continue;
    }
    metas.push_back(meta);
  }
  LOG(INFO) << Substitute("Loaded tablet metadata ($0 live tablets)", metas.size());

  // Now submit the "Open" task for each.
  for (const scoped_refptr<TabletMetadata>& meta : metas) {
    scoped_refptr<TransitionInProgressDeleter> deleter;
    {
      std::lock_guard<RWMutex> lock(lock_);
      CHECK_OK(StartTabletStateTransitionUnlocked(meta->tablet_id(), "opening tablet", &deleter));
    }

    scoped_refptr<TabletReplica> replica;
    RETURN_NOT_OK(CreateAndRegisterTabletReplica(meta, NEW_REPLICA, &replica));
    RETURN_NOT_OK(open_tablet_pool_->SubmitFunc(boost::bind(&TSTabletManager::OpenTablet,
                                                            this, replica, deleter)));
  }

  {
    std::lock_guard<RWMutex> lock(lock_);
    state_ = MANAGER_RUNNING;
  }

  return Status::OK();
}

Status TSTabletManager::WaitForAllBootstrapsToFinish() {
  CHECK_EQ(state(), MANAGER_RUNNING);

  open_tablet_pool_->Wait();

  shared_lock<RWMutex> l(lock_);
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
    std::lock_guard<RWMutex> lock(lock_);
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
                              boost::none,
                              &meta),
    "Couldn't create tablet metadata");

  // We must persist the consensus metadata to disk before starting a new
  // tablet's TabletReplica and RaftConsensus implementation.
  RETURN_NOT_OK_PREPEND(cmeta_manager_->Create(tablet_id, config, kMinimumTerm),
                        "Unable to create new ConsensusMetadata for tablet " + tablet_id);
  scoped_refptr<TabletReplica> new_replica;
  RETURN_NOT_OK(CreateAndRegisterTabletReplica(meta, NEW_REPLICA, &new_replica));

  // We can run this synchronously since there is nothing to bootstrap.
  RETURN_NOT_OK(open_tablet_pool_->SubmitFunc(boost::bind(&TSTabletManager::OpenTablet,
                                                          this, new_replica, deleter)));

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
class TabletCopyRunnable : public TabletManagerRunnable {
 public:
  TabletCopyRunnable(TSTabletManager* ts_tablet_manager,
                     const StartTabletCopyRequestPB* req,
                     std::function<void(const Status&, TabletServerErrorPB::Code)> cb)
      : TabletManagerRunnable(ts_tablet_manager, cb),
        req_(req) {
  }

  void Run() override {
    ts_tablet_manager_->RunTabletCopy(req_, cb_);
    cb_invoked_ = true;
  }

 private:
  const StartTabletCopyRequestPB* const req_;

  DISALLOW_COPY_AND_ASSIGN(TabletCopyRunnable);
};

void TSTabletManager::StartTabletCopy(
    const StartTabletCopyRequestPB* req,
    std::function<void(const Status&, TabletServerErrorPB::Code)> cb) {
  // Attempt to submit the tablet copy task to the threadpool. The threadpool
  // is configured with 0 queue slots, so if there is not a thread immediately
  // available the submit will fail. When successful, the table copy task will
  // immediately check whether the tablet is already being copied, and if so,
  // return ALREADY_INPROGRESS.
  string tablet_id = req->tablet_id();
  shared_ptr<TabletCopyRunnable> runnable(new TabletCopyRunnable(this, req, cb));
  Status s = tablet_copy_pool_->Submit(runnable);
  if (PREDICT_TRUE(s.ok())) {
    return;
  }

  // We were unable to submit the tablet copy task to the thread pool. We will
  // invoke the callback ourselves, so disable the automatic callback mechanism.
  runnable->DisableCallback();

  // Check if the tablet is already in transition (i.e. being copied).
  boost::optional<string> transition;
  {
    // Lock must be dropped before executing callbacks.
    shared_lock<RWMutex> lock(lock_);
    auto* t = FindOrNull(transition_in_progress_, tablet_id);
    if (t) {
      transition = *t;
    }
  }
  if (transition) {
    cb(Status::IllegalState(
          strings::Substitute("State transition of tablet $0 already in progress: $1",
                              tablet_id, *transition)),
          TabletServerErrorPB::ALREADY_INPROGRESS);
    return;
  }

  // The tablet is not already being copied, but there are no remaining slots in
  // the threadpool.
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

#define CALLBACK_RETURN_NOT_OK_WITH_ERROR(expr, error) \
  do { \
    Status _s = (expr); \
    if (PREDICT_FALSE(!_s.ok())) { \
      error_code = (error); \
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
    std::lock_guard<RWMutex> lock(lock_);
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
        boost::optional<OpId> last_logged_opid = meta->tombstone_last_logged_opid();
        if (last_logged_opid) {
          CALLBACK_RETURN_NOT_OK_WITH_ERROR(CheckLeaderTermNotLower(tablet_id, leader_term,
                                                                    last_logged_opid->term()),
                                            TabletServerErrorPB::INVALID_CONFIG);
        }
        // Shut down the old TabletReplica so that it is no longer allowed to
        // mutate the ConsensusMetadata.
        old_replica->Shutdown();
        break;
      }
      case TABLET_DATA_READY: {
        shared_ptr<RaftConsensus> consensus = old_replica->shared_consensus();
        if (!consensus) {
          CALLBACK_AND_RETURN(
              Status::IllegalState("consensus unavailable: tablet not running", tablet_id));
        }
        boost::optional<OpId> opt_last_logged_opid = consensus->GetLastOpId(RECEIVED_OPID);
        if (!opt_last_logged_opid) {
          CALLBACK_AND_RETURN(
              Status::IllegalState("cannot determine last-logged opid: tablet not running",
                                   tablet_id));
        }
        CHECK(opt_last_logged_opid);
        CALLBACK_RETURN_NOT_OK_WITH_ERROR(
            CheckLeaderTermNotLower(tablet_id, leader_term, opt_last_logged_opid->term()),
            TabletServerErrorPB::INVALID_CONFIG);

        // Shut down the old TabletReplica so that it is no longer allowed to
        // mutate the ConsensusMetadata.
        old_replica->Shutdown();

        // Note that this leaves the data dir manager without any references to
        // tablet_id. This is okay because the tablet_copy_client should
        // generate a new disk group during the call to Start().

        // Tombstone the tablet and store the last-logged OpId.
        // TODO(mpercy): Because we begin shutdown of the tablet after we check our
        // last-logged term against the leader's term, there may be operations
        // in flight and it may be possible for the same check in the tablet
        // copy client Start() method to fail. This will leave the replica in
        // a tombstoned state, and then the leader with the latest log entries
        // will simply tablet copy this replica again. We could try to
        // check again after calling Shutdown(), and if the check fails, try to
        // reopen the tablet. For now, we live with the (unlikely) race.
        Status s = DeleteTabletData(meta, cmeta_manager_, TABLET_DATA_TOMBSTONED,
                                    opt_last_logged_opid);
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

  // The TabletCopyClient instance should be kept alive until the tablet
  // is successfully copied over and opened/started. This is because we want
  // to maintain the LogAnchor until the replica starts up. Upon destruction,
  // the TabletCopyClient instance sends an RPC explicitly ending the tablet
  // copy session. The source replica then destroys the corresponding
  // TabletCopySourceSession object, releasing its LogAnchor and allowing
  // the WAL segments being copied to be GCed.
  //
  // See below for more details on why anchoring of WAL segments is necessary.
  //
  // * Assume there are WAL segments 0-10 when tablet copy starts. Tablet copy
  //   will anchor 0 until its destroyed, meaning the source replica wont
  //   delete it.
  //
  // * When tablet copy is done the tablet still needs to bootstrap which will
  //   take some time.
  //
  // * When tablet bootstrap is done, the new replica will need to continue
  //   catching up to the leader, this time through consensus. It needs segment
  //   11 to be still available. We need the anchor to still be alive at this
  //   point, otherwise there is nothing preventing the leader from deleting
  //   segment 11 and thus making the new replica unable to catch up. Yes, it's
  //   not optimal: we're anchoring 0 and we might only need to anchor 10/11.
  //   However, having no anchor at all is likely to cause replicas to start
  //   fail copying.
  //
  // NOTE:
  //   Ideally, we should wait until the leader starts tracking of the target
  //   replica's log watermark. As for current implementation, the intent is
  //   to at least try preventing GC of logs before the tablet replica connects
  //   to the leader.
  //
  // TODO(aserbin): make this robust and more optimal than it is now.
  TabletCopyClient tc_client(tablet_id, fs_manager_, cmeta_manager_,
                             server_->messenger(), &tablet_copy_metrics_);

  // Download and persist the remote superblock in TABLET_DATA_COPYING state.
  if (replacing_tablet) {
    CALLBACK_RETURN_NOT_OK(tc_client.SetTabletToReplace(meta, leader_term));
  }
  CALLBACK_RETURN_NOT_OK(tc_client.Start(copy_source_addr, &meta));

  // After calling TabletCopyClient::Start(), the superblock is persisted in
  // TABLET_DATA_COPYING state. TabletCopyClient will automatically tombstone
  // the tablet by implicitly calling Abort() on itself if it is destroyed
  // prior to calling TabletCopyClient::Finish(), which if successful
  // transitions the tablet into the TABLET_DATA_READY state.

  // Registering an unstarted TabletReplica allows for tombstoned voting and
  // offers visibility through the Web UI.
  RegisterTabletReplicaMode mode = replacing_tablet ? REPLACEMENT_REPLICA : NEW_REPLICA;
  scoped_refptr<TabletReplica> replica;
  CALLBACK_RETURN_NOT_OK(CreateAndRegisterTabletReplica(meta, mode, &replica));

  // Now we invoke the StartTabletCopy callback and respond success to the
  // remote caller, since StartTabletCopy() is an asynchronous RPC call. Then
  // we proceed with the Tablet Copy process.
  cb(Status::OK(), TabletServerErrorPB::UNKNOWN_ERROR);
  cb = [](const Status&, TabletServerErrorPB::Code) {
    LOG(FATAL) << "Callback invoked twice from TSTabletManager::RunTabletCopy()";
  };

  // From this point onward, we do not notify the caller about progress or success.

  // Go through and synchronously download the remote blocks and WAL segments.
  Status s = tc_client.FetchAll(replica);
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

  // Bootstrap and start the fully-copied tablet.
  OpenTablet(replica, deleter);
}

// Create and register a new TabletReplica, given tablet metadata.
Status TSTabletManager::CreateAndRegisterTabletReplica(
    scoped_refptr<TabletMetadata> meta,
    RegisterTabletReplicaMode mode,
    scoped_refptr<TabletReplica>* replica_out) {
  const string& tablet_id = meta->tablet_id();
  scoped_refptr<TabletReplica> replica(
      new TabletReplica(std::move(meta),
                        cmeta_manager_,
                        local_peer_pb_,
                        server_->tablet_apply_pool(),
                        Bind(&TSTabletManager::MarkTabletDirty,
                             Unretained(this),
                             tablet_id)));
  Status s = replica->Init(server_->raft_pool());
  if (PREDICT_FALSE(!s.ok())) {
    replica->SetError(s);
    replica->Shutdown();
  }
  RegisterTablet(tablet_id, replica, mode);
  *replica_out = std::move(replica);
  return Status::OK();
}

Status TSTabletManager::BeginReplicaStateTransition(
    const string& tablet_id,
    const string& reason,
    scoped_refptr<TabletReplica>* replica,
    scoped_refptr<TransitionInProgressDeleter>* deleter,
    TabletServerErrorPB::Code* error_code) {
  // Acquire the lock in exclusive mode as we'll add a entry to the
  // transition_in_progress_ map.
  std::lock_guard<RWMutex> lock(lock_);
  TRACE("Acquired tablet manager lock");
  RETURN_NOT_OK(CheckRunningUnlocked(error_code));

  if (!LookupTabletUnlocked(tablet_id, replica)) {
    if (error_code) {
      *error_code = TabletServerErrorPB::TABLET_NOT_FOUND;
    }
    return Status::NotFound("Tablet not found", tablet_id);
  }
  // Sanity check that the tablet's transition isn't already in progress
  Status s = StartTabletStateTransitionUnlocked(tablet_id, reason, deleter);
  if (PREDICT_FALSE(!s.ok())) {
    if (error_code) {
      *error_code = TabletServerErrorPB::TABLET_NOT_RUNNING;
    }
    return s;
  }
  return Status::OK();
}

// Delete Tablet runnable that will run on a ThreadPool.
class DeleteTabletRunnable : public TabletManagerRunnable {
public:
  DeleteTabletRunnable(TSTabletManager* ts_tablet_manager,
                       const std::string& tablet_id,
                       tablet::TabletDataState delete_type,
                       const boost::optional<int64_t>& cas_config_index,
                       std::function<void(const Status&, TabletServerErrorPB::Code)> cb)
      : TabletManagerRunnable(ts_tablet_manager, cb),
        tablet_id_(tablet_id),
        delete_type_(delete_type),
        cas_config_index_(cas_config_index) {
  }

  void Run() override {
    TabletServerErrorPB::Code code = TabletServerErrorPB::UNKNOWN_ERROR;
    Status s = ts_tablet_manager_->DeleteTablet(tablet_id_, delete_type_, cas_config_index_, &code);
    cb_(s, code);
    cb_invoked_ = true;
  }

private:
  const string tablet_id_;
  const tablet::TabletDataState delete_type_;
  const boost::optional<int64_t> cas_config_index_;

  DISALLOW_COPY_AND_ASSIGN(DeleteTabletRunnable);
};

void TSTabletManager::DeleteTabletAsync(
    const std::string& tablet_id,
    tablet::TabletDataState delete_type,
    const boost::optional<int64_t>& cas_config_index,
    std::function<void(const Status&, TabletServerErrorPB::Code)> cb) {
  auto runnable = std::make_shared<DeleteTabletRunnable>(this, tablet_id, delete_type,
                                                         cas_config_index, cb);
  Status s = delete_tablet_pool_->Submit(runnable);
  if (PREDICT_TRUE(s.ok())) {
    return;
  }

  // Threadpool submission failed, so we'll invoke the callback ourselves.
  runnable->DisableCallback();
  cb(s, s.IsServiceUnavailable() ? TabletServerErrorPB::THROTTLED :
                                   TabletServerErrorPB::UNKNOWN_ERROR);
}

Status TSTabletManager::DeleteTablet(
    const string& tablet_id,
    TabletDataState delete_type,
    const boost::optional<int64_t>& cas_config_index,
    TabletServerErrorPB::Code* error_code) {

  if (delete_type != TABLET_DATA_DELETED && delete_type != TABLET_DATA_TOMBSTONED) {
    return Status::InvalidArgument("DeleteTablet() requires an argument that is one of "
                                   "TABLET_DATA_DELETED or TABLET_DATA_TOMBSTONED",
                                   Substitute("Given: $0 ($1)",
                                              TabletDataState_Name(delete_type), delete_type));
  }

  TRACE("Deleting tablet $0", tablet_id);

  if (PREDICT_FALSE(FLAGS_delete_tablet_inject_latency_ms > 0)) {
    LOG(WARNING) << "Injecting " << FLAGS_delete_tablet_inject_latency_ms
                 << "ms of latency into DeleteTablet";
    SleepFor(MonoDelta::FromMilliseconds(FLAGS_delete_tablet_inject_latency_ms));
  }

  scoped_refptr<TabletReplica> replica;
  scoped_refptr<TransitionInProgressDeleter> deleter;
  RETURN_NOT_OK(BeginReplicaStateTransition(tablet_id, "deleting tablet", &replica,
                                            &deleter, error_code));

  // If the tablet has been deleted or forcefully shut down, the CAS check
  // isn't possible because consensus and therefore the log is not available.
  TabletDataState data_state = replica->tablet_metadata()->tablet_data_state();
  bool tablet_already_deleted = (data_state == TABLET_DATA_DELETED ||
                                 data_state == TABLET_DATA_TOMBSTONED);

  // If a tablet is already tombstoned, then a request to tombstone
  // the same tablet should become a no-op.
  if (delete_type == TABLET_DATA_TOMBSTONED && data_state == TABLET_DATA_TOMBSTONED) {
    return Status::OK();
  }

  // They specified an "atomic" delete. Check the committed config's opid_index.
  // TODO(mpercy): There's actually a race here between the check and shutdown,
  // but it's tricky to fix. We could try checking again after the shutdown and
  // restarting the tablet if the local replica committed a higher config change
  // op during that time, or potentially something else more invasive.
  shared_ptr<RaftConsensus> consensus = replica->shared_consensus();
  if (cas_config_index && !tablet_already_deleted) {
    if (!consensus) {
      *error_code = TabletServerErrorPB::TABLET_NOT_RUNNING;
      return Status::IllegalState("Raft Consensus not available. Tablet shutting down");
    }
    RaftConfigPB committed_config = consensus->CommittedConfig();
    if (committed_config.opid_index() > *cas_config_index) {
      *error_code = TabletServerErrorPB::CAS_FAILED;
      return Status::IllegalState(Substitute("Request specified cas_config_opid_index_less_or_equal"
                                             " of $0 but the committed config has opid_index of $1",
                                             *cas_config_index,
                                             committed_config.opid_index()));
    }
  }

  replica->Stop();

  boost::optional<OpId> opt_last_logged_opid;
  if (consensus) {
    opt_last_logged_opid = consensus->GetLastOpId(RECEIVED_OPID);
    DCHECK(!opt_last_logged_opid || opt_last_logged_opid->IsInitialized());
  }

  Status s = DeleteTabletData(replica->tablet_metadata(), cmeta_manager_, delete_type,
                              opt_last_logged_opid);
  if (PREDICT_FALSE(!s.ok())) {
    s = s.CloneAndPrepend(Substitute("Unable to delete on-disk data from tablet $0",
                                     tablet_id));
    // TODO(awong): A failure status here indicates a failure to update the
    // tablet metadata, consensus metadta, or WAL (failures to remove blocks
    // only log warnings). Once the above are no longer points of failure,
    // handle errors here accordingly.
    //
    // If this fails, there is no guarantee that the on-disk metadata reflects
    // that the tablet is deleted. To be safe, crash here.
    LOG(FATAL) << Substitute("Failed to delete tablet data for $0: ",
        tablet_id) << s.ToString();
  }

  replica->SetStatusMessage("Deleted tablet blocks from disk");

  // Only DELETED tablets are fully shut down and removed from the tablet map.
  if (delete_type == TABLET_DATA_DELETED) {
    replica->Shutdown();
    std::lock_guard<RWMutex> lock(lock_);
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
    TabletServerErrorPB::Code* error_code) const {
  if (state_ == MANAGER_RUNNING) {
    return Status::OK();
  }
  if (error_code) {
    *error_code = TabletServerErrorPB::TABLET_NOT_RUNNING;
  }
  return Status::ServiceUnavailable(Substitute("Tablet Manager is not running: $0",
                                               TSTabletManagerStatePB_Name(state_)));
}

Status TSTabletManager::StartTabletStateTransitionUnlocked(
    const string& tablet_id,
    const string& reason,
    scoped_refptr<TransitionInProgressDeleter>* deleter) {
  lock_.AssertAcquiredForWriting();
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
    return Status::AlreadyPresent(
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

// Note: 'deleter' is not used in the body of OpenTablet(), but is required
// anyway because its destructor performs cleanup that should only happen when
// OpenTablet() completes.
void TSTabletManager::OpenTablet(const scoped_refptr<TabletReplica>& replica,
                                 const scoped_refptr<TransitionInProgressDeleter>& /*deleter*/) {
  const string& tablet_id = replica->tablet_id();
  TRACE_EVENT1("tserver", "TSTabletManager::OpenTablet",
               "tablet_id", tablet_id);

  shared_ptr<Tablet> tablet;
  scoped_refptr<Log> log;

  LOG(INFO) << LogPrefix(tablet_id) << "Bootstrapping tablet";
  TRACE("Bootstrapping tablet");

  scoped_refptr<ConsensusMetadata> cmeta;
  Status s = cmeta_manager_->Load(replica->tablet_id(), &cmeta);
  auto fail_tablet = MakeScopedCleanup([&]() {
    // If something goes wrong, clean up the replica's internal members and mark
    // it FAILED.
    replica->SetError(s);
    replica->Shutdown();
  });
  if (PREDICT_FALSE(!s.ok())) {
    LOG(ERROR) << LogPrefix(tablet_id) << "Failed to load consensus metadata: " << s.ToString();
    return;
  }

  consensus::ConsensusBootstrapInfo bootstrap_info;
  LOG_TIMING_PREFIX(INFO, LogPrefix(tablet_id), "bootstrapping tablet") {
    // Disable tracing for the bootstrap, since this would result in
    // potentially millions of transaction traces being attached to the
    // TabletCopy trace.
    ADOPT_TRACE(nullptr);

    // TODO(mpercy): Handle crash mid-creation of tablet? Do we ever end up
    // with a partially created tablet here?
    replica->SetBootstrapping();
    s = BootstrapTablet(replica->tablet_metadata(),
                        replica->consensus()->CommittedConfig(),
                        scoped_refptr<clock::Clock>(server_->clock()),
                        server_->mem_tracker(),
                        server_->result_tracker(),
                        metric_registry_,
                        replica,
                        &tablet,
                        &log,
                        replica->log_anchor_registry(),
                        &bootstrap_info);
    if (!s.ok()) {
      LOG(ERROR) << LogPrefix(tablet_id) << "Tablet failed to bootstrap: "
                 << s.ToString();
      return;
    }
  }

  MonoTime start(MonoTime::Now());
  LOG_TIMING_PREFIX(INFO, LogPrefix(tablet_id), "starting tablet") {
    TRACE("Starting tablet replica");
    s = replica->Start(bootstrap_info,
                       tablet,
                       scoped_refptr<clock::Clock>(server_->clock()),
                       server_->messenger(),
                       server_->result_tracker(),
                       log,
                       server_->tablet_prepare_pool());
    if (!s.ok()) {
      LOG(ERROR) << LogPrefix(tablet_id) << "Tablet failed to start: "
                 << s.ToString();
      return;
    }

    replica->RegisterMaintenanceOps(server_->maintenance_manager());
  }

  // Now that the tablet has successfully opened, cancel the cleanup.
  fail_tablet.cancel();

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
    std::lock_guard<RWMutex> lock(lock_);
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

  // Shut down the delete pool, so no new tablets are deleted after this point.
  delete_tablet_pool_->Shutdown();

  // Take a snapshot of the replicas list -- that way we don't have to hold
  // on to the lock while shutting them down, which might cause a lock
  // inversion. (see KUDU-308 for example).
  vector<scoped_refptr<TabletReplica> > replicas_to_shutdown;
  GetTabletReplicas(&replicas_to_shutdown);

  for (const scoped_refptr<TabletReplica>& replica : replicas_to_shutdown) {
    replica->Shutdown();
  }

  {
    std::lock_guard<RWMutex> l(lock_);
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
  std::lock_guard<RWMutex> lock(lock_);
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
  shared_lock<RWMutex> l(lock_);
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
  return Status::OK();
}

const NodeInstancePB& TSTabletManager::NodeInstance() const {
  return server_->instance_pb();
}

void TSTabletManager::GetTabletReplicas(vector<scoped_refptr<TabletReplica> >* replicas) const {
  shared_lock<RWMutex> l(lock_);
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
  shared_lock<RWMutex> l(lock_);
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

void TSTabletManager::CreateReportedTabletPB(const scoped_refptr<TabletReplica>& replica,
                                             ReportedTabletPB* reported_tablet) const {
  reported_tablet->set_tablet_id(replica->tablet_id());
  reported_tablet->set_state(replica->state());
  reported_tablet->set_tablet_data_state(replica->tablet_metadata()->tablet_data_state());
  const Status& error = replica->error();
  if (!error.ok()) {
    StatusToPB(error, reported_tablet->mutable_error());
  }
  reported_tablet->set_schema_version(replica->tablet_metadata()->schema_version());

  // We cannot get consensus state information unless the TabletReplica is running.
  shared_ptr<consensus::RaftConsensus> consensus = replica->shared_consensus();
  if (consensus) {
    auto include_health = FLAGS_raft_prepare_replacement_before_eviction ?
                          INCLUDE_HEALTH_REPORT : EXCLUDE_HEALTH_REPORT;
    ConsensusStatePB cstate;
    Status s = consensus->ConsensusState(&cstate, include_health);
    if (PREDICT_TRUE(s.ok())) {
      *reported_tablet->mutable_consensus_state() = std::move(cstate);
    }
  }
}

void TSTabletManager::PopulateFullTabletReport(TabletReportPB* report) const {
  // Creating the tablet report can be slow in the case that it is in the
  // middle of flushing its consensus metadata. We don't want to hold
  // lock_ for too long, even in read mode, since it can cause other readers
  // to block if there is a waiting writer (see KUDU-2193). So, we just make
  // a local copy of the set of replicas.
  vector<scoped_refptr<tablet::TabletReplica>> to_report;
  GetTabletReplicas(&to_report);
  for (const auto& replica : to_report) {
    CreateReportedTabletPB(replica, report->add_updated_tablets());
  }
}

void TSTabletManager::PopulateIncrementalTabletReport(TabletReportPB* report,
                                                      const vector<string>& tablet_ids) const {
  // See comment in PopulateFullTabletReport for rationale on making a local
  // copy of the set of tablets to report.
  vector<scoped_refptr<tablet::TabletReplica>> to_report;
  to_report.reserve(tablet_ids.size());
  {
    shared_lock<RWMutex> shared_lock(lock_);
    for (const auto& id : tablet_ids) {
      const scoped_refptr<tablet::TabletReplica>* replica =
          FindOrNull(tablet_map_, id);
      if (replica) {
        // Dirty entry, report on it.
        to_report.push_back(*replica);
      } else {
        // Removed.
        report->add_removed_tablet_ids(id);
      }
    }
  }
  for (const auto& replica : to_report) {
    CreateReportedTabletPB(replica, report->add_updated_tablets());
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

  if (data_state == TABLET_DATA_TOMBSTONED) {
    // It is possible for tombstoned replicas to legitimately not have a cmeta
    // file as a result of crashing during a first tablet copy, or failing a
    // tablet copy operation in an older version of Kudu. Not having a cmeta
    // file results in those tombstoned replicas being unable to vote in Raft
    // leader elections. We remedy this by creating a cmeta object (with an
    // empty config) at startup time. The empty config is safe for a tombstoned
    // replica, because the config doesn't affect a replica's ability to vote
    // in a leader election. Additionally, if the tombstoned replica were ever
    // to be overwritten by a tablet copy operation, that would also result in
    // overwriting the config stored in the local cmeta with a valid Raft
    // config. Finally, all of this assumes that the nonexistence of a cmeta
    // file guarantees that the replica has never voted in a leader election.
    //
    // As an optimization, the cmeta is created with the NO_FLUSH_ON_CREATE
    // flag, meaning that it will only be flushed to disk if the replica ever
    // votes.
    RETURN_NOT_OK(cmeta_manager_->LoadOrCreate(tablet_id, RaftConfigPB(), kMinimumTerm,
                                               ConsensusMetadataCreateMode::NO_FLUSH_ON_CREATE));
  }

  if (!skip_deletion) {
    // Passing no OpId will retain the last_logged_opid that was previously in the metadata.
    RETURN_NOT_OK(DeleteTabletData(meta, cmeta_manager_, data_state, boost::none));
  }

  // Register TOMBSTONED tablets so that they get reported to the Master, which
  // allows us to permanently delete replica tombstones when a table gets
  // deleted.
  if (data_state == TABLET_DATA_TOMBSTONED) {
    scoped_refptr<TabletReplica> dummy;
    RETURN_NOT_OK(CreateAndRegisterTabletReplica(meta, NEW_REPLICA, &dummy));
    dummy->SetStatusMessage("Tombstoned");
  }

  return Status::OK();
}

Status TSTabletManager::DeleteTabletData(
    const scoped_refptr<TabletMetadata>& meta,
    const scoped_refptr<consensus::ConsensusMetadataManager>& cmeta_manager,
    TabletDataState delete_type,
    boost::optional<OpId> last_logged_opid) {
  const string& tablet_id = meta->tablet_id();
  LOG(INFO) << LogPrefix(tablet_id, meta->fs_manager())
            << "Deleting tablet data with delete state "
            << TabletDataState_Name(delete_type);
  CHECK(delete_type == TABLET_DATA_DELETED ||
        delete_type == TABLET_DATA_TOMBSTONED ||
        delete_type == TABLET_DATA_COPYING)
      << "Unexpected delete_type to delete tablet " << tablet_id << ": "
      << TabletDataState_Name(delete_type) << " (" << delete_type << ")";

  // Note: Passing an unset 'last_logged_opid' will retain the last_logged_opid
  // that was previously in the metadata.
  RETURN_NOT_OK(meta->DeleteTabletData(delete_type, last_logged_opid));
  last_logged_opid = meta->tombstone_last_logged_opid();
  LOG(INFO) << LogPrefix(tablet_id, meta->fs_manager())
            << "tablet deleted with delete type "
            << TabletDataState_Name(delete_type) << ": "
            << "last-logged OpId "
            << (last_logged_opid ? OpIdToString(*last_logged_opid) : "unknown");
  MAYBE_FAULT(FLAGS_fault_crash_after_blocks_deleted);

  CHECK_OK(Log::DeleteOnDiskData(meta->fs_manager(), tablet_id));
  MAYBE_FAULT(FLAGS_fault_crash_after_wal_deleted);

  // We do not delete the superblock or the consensus metadata when tombstoning
  // a tablet or marking it as entering the tablet copy process.
  if (delete_type == TABLET_DATA_COPYING ||
      delete_type == TABLET_DATA_TOMBSTONED) {
    return Status::OK();
  }

  // Only TABLET_DATA_DELETED tablets get this far.
  DCHECK_EQ(TABLET_DATA_DELETED, delete_type);

  LOG(INFO) << LogPrefix(tablet_id, meta->fs_manager()) << "Deleting consensus metadata";
  Status s = cmeta_manager->Delete(tablet_id);
  // NotFound means we already deleted the cmeta in a previous attempt.
  if (PREDICT_FALSE(!s.ok() && !s.IsNotFound())) {
    if (s.IsDiskFailure()) {
      LOG(FATAL) << LogPrefix(tablet_id, meta->fs_manager())
                 << "consensus metadata is on a failed disk";
    }
    return s;
  }
  MAYBE_FAULT(FLAGS_fault_crash_after_cmeta_deleted);
  s = meta->DeleteSuperBlock();
  if (PREDICT_FALSE(!s.ok())) {
    if (s.IsDiskFailure()) {
      LOG(FATAL) << LogPrefix(tablet_id, meta->fs_manager())
                 << "tablet metadata is on a failed disk";
    }
    return s;
  }
  return Status::OK();
}

void TSTabletManager::FailTabletsInDataDir(const string& uuid) {
  DataDirManager* dd_manager = fs_manager_->dd_manager();
  int uuid_idx;
  CHECK(dd_manager->FindUuidIndexByUuid(uuid, &uuid_idx))
      << Substitute("No data directory found with UUID $0", uuid);
  if (fs_manager_->dd_manager()->IsDataDirFailed(uuid_idx)) {
    LOG(WARNING) << "Data directory is already marked failed.";
    return;
  }
  // Fail the directory to prevent other tablets from being placed in it.
  dd_manager->MarkDataDirFailed(uuid_idx);
  set<string> tablets = dd_manager->FindTabletsByDataDirUuidIdx(uuid_idx);
  LOG(INFO) << Substitute("Data dir $0 has $1 tablets", uuid, tablets.size());
  for (const string& tablet_id : dd_manager->FindTabletsByDataDirUuidIdx(uuid_idx)) {
    FailTabletAndScheduleShutdown(tablet_id);
  }
}

void TSTabletManager::FailTabletAndScheduleShutdown(const string& tablet_id) {
  LOG(INFO) << LogPrefix(tablet_id, fs_manager_) << "failing tablet";
  scoped_refptr<TabletReplica> replica;
  if (LookupTablet(tablet_id, &replica)) {
    // Stop further IO to the replica and set an error in the replica.
    // When the replica is shutdown, this will leave it in a FAILED state.
    replica->MakeUnavailable(Status::IOError("failing tablet"));

    // Submit a request to actually shut down the tablet asynchronously.
    CHECK_OK(open_tablet_pool_->SubmitFunc([tablet_id, this]() {
      scoped_refptr<TabletReplica> replica;
      scoped_refptr<TransitionInProgressDeleter> deleter;
      TabletServerErrorPB::Code error;
      Status s;
      // Transition tablet state to ensure nothing else (e.g. tablet copies,
      // deletions, etc) happens concurrently.
      while (true) {
        s = BeginReplicaStateTransition(tablet_id, "failing tablet",
                                        &replica, &deleter, &error);
        if (!s.IsAlreadyPresent()) {
          break;
        }
        SleepFor(MonoDelta::FromMilliseconds(10));
      }
      // Success: we started the transition.
      //
      // Only proceed if there is no Tablet (e.g. a bootstrap terminated early
      // due to error before creating the Tablet) or if the tablet has been
      // stopped (e.g. due to the above call to MakeUnavailable).
      std::shared_ptr<Tablet> tablet = replica->shared_tablet();
      if (s.ok() && (!tablet || tablet->HasBeenStopped())) {
        replica->Shutdown();
      }
      // Else: the tablet is healthy, or is already either not running or
      // deleted (e.g. because another thread was able to successfully create a
      // new replica).
    }));
  }
}

int TSTabletManager::RefreshTabletStateCacheAndReturnCount(tablet::TabletStatePB st) {
  MonoDelta period = MonoDelta::FromMilliseconds(FLAGS_tablet_state_walk_min_period_ms);
  std::lock_guard<RWMutex> lock(lock_);
  if (last_walked_ + period < MonoTime::Now()) {
    // Old cache: regenerate counts.
    tablet_state_counts_.clear();
    for (const auto& entry : tablet_map_) {
      tablet_state_counts_[entry.second->state()]++;
    }
    last_walked_ = MonoTime::Now();
  }
  return FindWithDefault(tablet_state_counts_, st, 0);
}

TransitionInProgressDeleter::TransitionInProgressDeleter(
    TransitionInProgressMap* map, RWMutex* lock, string entry)
    : in_progress_(map), lock_(lock), entry_(std::move(entry)) {}

TransitionInProgressDeleter::~TransitionInProgressDeleter() {
  std::lock_guard<RWMutex> lock(*lock_);
  CHECK(in_progress_->erase(entry_));
}

} // namespace tserver
} // namespace kudu
