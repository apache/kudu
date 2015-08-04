// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/tserver/ts_tablet_manager.h"

#include <algorithm>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <glog/logging.h>
#include <string>
#include <tr1/memory>
#include <vector>

#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/master/master.pb.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_bootstrap.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_peer.h"
#include "kudu/tserver/heartbeater.h"
#include "kudu/tserver/remote_bootstrap_client.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/metrics.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/trace.h"

DEFINE_int32(num_tablets_to_open_simultaneously, 50,
    "Number of threads available to open tablets.");
DEFINE_int32(tablet_start_warn_threshold_ms, 500,
             "If a tablet takes more than this number of millis to start, issue "
             "a warning with a trace.");

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
using log::Log;
using master::ReportedTabletPB;
using master::TabletReportPB;
using std::string;
using std::tr1::shared_ptr;
using std::vector;
using strings::Substitute;
using tablet::Tablet;
using tablet::TabletMetadata;
using tablet::TabletPeer;
using tablet::TabletStatusListener;
using tablet::TabletStatusPB;
using tserver::RemoteBootstrapClient;

namespace {
// helper to delete the creation-in-progress entry from the corresponding
// set when the CreateNewTablet method completes.
struct CreatesInProgressDeleter {
  CreatesInProgressDeleter(CreatesInProgressSet* set,
                           rw_spinlock* lock,
                           const string& entry)
      : set_(set),
        lock_(lock),
        entry_(entry) {
  }

  ~CreatesInProgressDeleter() {
    boost::lock_guard<rw_spinlock> lock(*lock_);
    CHECK(set_->erase(entry_));
  }

  CreatesInProgressSet* set_;
  rw_spinlock* lock_;
  string entry_;
};
} // anonymous namespace

TSTabletManager::TSTabletManager(FsManager* fs_manager,
                                 TabletServer* server,
                                 MetricRegistry* metric_registry)
  : fs_manager_(fs_manager),
    server_(server),
    next_report_seq_(0),
    metric_registry_(metric_registry),
    state_(MANAGER_INITIALIZING) {
  // TODO base the number of parallel tablet bootstraps on something related
  // to the number of physical devices.
  // Right now it's set to be the same as the number of RPC handlers so that we
  // can process as many of them in parallel as we can.
  CHECK_OK(ThreadPoolBuilder("tablet-bootstrap")
      .set_max_threads(FLAGS_num_tablets_to_open_simultaneously)
      .Build(&open_tablet_pool_));
  // TODO currently this is initialized to default values: no
  // minimum number of threads, 500 ms idle timeout, and maximum
  // number of threads equal to number of CPU cores. Instead, it
  // likewise makes more sense to set this equal to the number of
  // physical storage devices available to us.
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

  // Search for tablets in the metadata dir.
  vector<string> tablet_ids;
  RETURN_NOT_OK(fs_manager_->ListTabletIds(&tablet_ids));

  InitLocalRaftPeerPB();

  // Register the tablets and trigger the asynchronous bootstrap
  BOOST_FOREACH(const string& tablet_id, tablet_ids) {
    scoped_refptr<TabletMetadata> meta;
    RETURN_NOT_OK_PREPEND(OpenTabletMeta(tablet_id, &meta),
                          "Failed to open tablet metadata for tablet: " + tablet_id);
    scoped_refptr<TabletPeer> tablet_peer = CreateAndRegisterTabletPeer(meta);

    RETURN_NOT_OK(open_tablet_pool_->SubmitFunc(boost::bind(&TSTabletManager::OpenTablet,
                                                this,
                                                meta)));
  }

  {
    boost::lock_guard<rw_spinlock> lock(lock_);
    state_ = MANAGER_RUNNING;
  }

  return Status::OK();
}

Status TSTabletManager::WaitForAllBootstrapsToFinish() {
  CHECK_EQ(state(), MANAGER_RUNNING);

  open_tablet_pool_->Wait();

  Status s = Status::OK();

  boost::shared_lock<rw_spinlock> shared_lock(lock_);
  BOOST_FOREACH(const TabletMap::value_type& entry, tablet_map_) {
    if (entry.second->state() == tablet::FAILED) {
      if (s.ok()) {
        s = entry.second->error();
      }
    }
  }

  return s;
}

Status TSTabletManager::CreateNewTablet(const string& table_id,
                                        const string& tablet_id,
                                        const string& start_key, const string& end_key,
                                        const string& table_name,
                                        const Schema& schema,
                                        RaftConfigPB config,
                                        scoped_refptr<TabletPeer>* tablet_peer) {
  CHECK_EQ(state(), MANAGER_RUNNING);

  // If the consensus configuration is specified to use local consensus, verify that the peer
  // matches up with our local info.
  if (config.local()) {
    CHECK_EQ(1, config.peers_size());
    CHECK_EQ(server_->instance_pb().permanent_uuid(), config.peers(0).permanent_uuid());
  }

  // Set the initial opid_index for a RaftConfigPB to -1.
  config.set_opid_index(consensus::kInvalidOpIdIndex);

  {
    // acquire the lock in exclusive mode as we'll add a entry to the
    // creates_in_progress_ set if the lookup fails.
    boost::lock_guard<rw_spinlock> lock(lock_);
    TRACE("Acquired tablet manager lock");

    // Sanity check that the tablet isn't already registered.
    scoped_refptr<TabletPeer> junk;
    if (LookupTabletUnlocked(tablet_id, &junk)) {
      return Status::AlreadyPresent("Tablet already registered", tablet_id);
    }

    // Sanity check that the tablet's creation isn't already in progress
    if (!InsertIfNotPresent(&creates_in_progress_, tablet_id)) {
      return Status::AlreadyPresent("Creation of tablet already in progress",
                                    tablet_id);
    }
  }

  CreatesInProgressDeleter deleter(&creates_in_progress_, &lock_, tablet_id);

  // Create the metadata.
  TRACE("Creating new metadata...");
  scoped_refptr<TabletMetadata> meta;
  RETURN_NOT_OK_PREPEND(
    TabletMetadata::CreateNew(fs_manager_,
                              tablet_id,
                              table_name,
                              schema,
                              start_key,
                              end_key,
                              tablet::TABLET_DATA_READY,
                              &meta),
    "Couldn't create tablet metadata");

  // We must persist the consensus metadata to disk before starting a new
  // tablet's TabletPeer and Consensus implementation.
  gscoped_ptr<ConsensusMetadata> cmeta;
  RETURN_NOT_OK_PREPEND(ConsensusMetadata::Create(fs_manager_, tablet_id, fs_manager_->uuid(),
                                                  config, consensus::kMinimumTerm, &cmeta),
                        "Unable to create new ConsensusMeta for tablet " + tablet_id);
  scoped_refptr<TabletPeer> new_peer = CreateAndRegisterTabletPeer(meta);

  // We can run this synchronously since there is nothing to bootstrap.
  RETURN_NOT_OK(open_tablet_pool_->SubmitFunc(boost::bind(&TSTabletManager::OpenTablet,
                                              this,
                                              meta)));

  if (tablet_peer) {
    *tablet_peer = new_peer;
  }
  return Status::OK();
}

// TODO: KUDU-921: Run this procedure on a background thread.
Status TSTabletManager::StartRemoteBootstrap(const std::string& tablet_id,
                                             const std::string& bootstrap_peer_uuid,
                                             const HostPort& bootstrap_peer_addr) {
  const string kLogPrefix = "T " + tablet_id + " P " + fs_manager_->uuid() + ": ";

  {
    boost::lock_guard<rw_spinlock> lock(lock_);
    scoped_refptr<TabletPeer> tablet_peer;
    if (LookupTabletUnlocked(tablet_id, &tablet_peer)) {
      return Status::AlreadyPresent("Remote bootstrap of existing tablet not yet supported",
                                    tablet_id);
    } else if (ContainsKey(creates_in_progress_, tablet_id)) {
      return Status::AlreadyPresent("Tablet already under construction", tablet_id);
    }
    InsertOrDie(&creates_in_progress_, tablet_id);
  }

  CreatesInProgressDeleter deleter(&creates_in_progress_, &lock_, tablet_id);

  string init_msg = kLogPrefix + Substitute("Initiating remote bootstrap from Peer $0 ($1)",
                                            bootstrap_peer_uuid, bootstrap_peer_addr.ToString());
  LOG(INFO) << init_msg;
  TRACE(init_msg);

  gscoped_ptr<RemoteBootstrapClient> rb_client(
      new RemoteBootstrapClient(tablet_id, fs_manager_, server_->messenger(),
                                fs_manager_->uuid()));

  scoped_refptr<TabletMetadata> meta;
  RETURN_NOT_OK(rb_client->Start(bootstrap_peer_uuid, bootstrap_peer_addr, &meta));

  // Registering a non-initialized TabletPeer offers visibility through the Web UI.
  scoped_refptr<TabletPeer> tablet_peer = CreateAndRegisterTabletPeer(meta);

  // Download all of the remote files.
  Status s = rb_client->FetchAll(tablet_peer->status_listener());
  if (!s.ok()) {
    // TODO: If the remote bootstrap fails, attempt to delete the tablet.
    // TODO: Implement tablet delete. See KUDU-967.
    // TODO: We should not leave a broken tablet in tablet_map. See KUDU-922.
    LOG(WARNING) << kLogPrefix << "Error during remote bootstrap: " << s.ToString();
    tablet_peer->SetFailed(s);
    return s;
  }

  // Write out the last files to make the new replica visible. Also update the
  // TabletBootstrapStatePB in the TabletMetadata to REMOTE_BOOTSTRAP_DONE.
  s = rb_client->Finish();
  if (!s.ok()) {
    // TODO: We should not leave a broken tablet in tablet_map. See KUDU-922.
    LOG(WARNING) << kLogPrefix << "Error finishing remote bootstrap: " << s.ToString();
    tablet_peer->SetFailed(s);
    return s;
  }

  OpenTablet(meta);
  return Status::OK();
}

// Create and register a new TabletPeer, given tablet metadata.
scoped_refptr<TabletPeer> TSTabletManager::CreateAndRegisterTabletPeer(
    const scoped_refptr<TabletMetadata>& meta) {
  scoped_refptr<TabletPeer> tablet_peer(
      new TabletPeer(meta,
                     local_peer_pb_,
                     apply_pool_.get(),
                     Bind(&TSTabletManager::MarkTabletDirty, Unretained(this), meta->tablet_id())));
  RegisterTablet(meta->tablet_id(), tablet_peer);
  return tablet_peer;
}

Status TSTabletManager::DeleteTablet(const scoped_refptr<TabletPeer>& tablet_peer) {
  TRACE("Deleting tablet $0 (table=$1 [id=$2])", tablet_peer->tablet()->tablet_id(),
        tablet_peer->tablet()->metadata()->table_name(),
        tablet_peer->tablet()->metadata()->table_id());
  tablet::TabletStatePB prev_state = tablet_peer->Shutdown();
  if (prev_state == tablet::QUIESCING || prev_state == tablet::SHUTDOWN) {
    return Status::ServiceUnavailable("Tablet Peer not in RUNNING state",
                                      tablet::TabletStatePB_Name(prev_state));
  }

  {
    boost::lock_guard<rw_spinlock> lock(lock_);
    CHECK_EQ(1, tablet_map_.erase(tablet_peer->tablet()->tablet_id()));
  }

  WARN_NOT_OK(tablet_peer->DeleteOnDiskData(),
              Substitute("Unable to delete on-disk data from tablet $0",
                         tablet_peer->tablet_id()));

  return Status::OK();
}

Status TSTabletManager::OpenTabletMeta(const string& tablet_id,
                                       scoped_refptr<TabletMetadata>* metadata) {
  LOG(INFO) << "Loading tablet " << tablet_id;
  TRACE("Loading metadata...");
  scoped_refptr<TabletMetadata> meta;
  RETURN_NOT_OK_PREPEND(TabletMetadata::Load(fs_manager_, tablet_id, &meta),
                        strings::Substitute("Failed to load tablet metadata for tablet id $0",
                                            tablet_id));
  TRACE("Metadata loaded");
  metadata->swap(meta);
  return Status::OK();
}

void TSTabletManager::OpenTablet(const scoped_refptr<TabletMetadata>& meta) {
  string tablet_id = meta->tablet_id();
  TRACE_EVENT1("tserver", "TSTabletManager::OpenTablet",
               "tablet_id", tablet_id);

  scoped_refptr<TabletPeer> tablet_peer;
  CHECK(LookupTablet(tablet_id, &tablet_peer))
      << "Tablet not registered prior to OpenTabletAsync call: " << tablet_id;

  shared_ptr<Tablet> tablet;
  scoped_refptr<Log> log;

  LOG(INFO) << "Bootstrapping tablet: " << tablet_id;
  TRACE("Bootstrapping tablet");

  consensus::ConsensusBootstrapInfo bootstrap_info;
  Status s;
  LOG_TIMING(INFO, Substitute("bootstrapping tablet $0", tablet_id)) {
    // TODO: handle crash mid-creation of tablet? do we ever end up with a
    // partially created tablet here?
    s = BootstrapTablet(meta,
                        scoped_refptr<server::Clock>(server_->clock()),
                        server_->mem_tracker(),
                        metric_registry_,
                        tablet_peer->status_listener(),
                        &tablet,
                        &log,
                        tablet_peer->log_anchor_registry(),
                        &bootstrap_info);
    if (!s.ok()) {
      LOG(ERROR) << "Tablet failed to bootstrap: "
          << tablet_id << " Status: " << s.ToString();
      tablet_peer->SetFailed(s);
      return;
    }
  }

  MonoTime start(MonoTime::Now(MonoTime::FINE));
  LOG_TIMING(INFO, Substitute("starting tablet $0", tablet_id)) {
    TRACE("Initializing tablet peer");
    s =  tablet_peer->Init(tablet,
                           scoped_refptr<server::Clock>(server_->clock()),
                           server_->messenger(),
                           log,
                           tablet->GetMetricEntity(),
                           server_->mem_tracker());

    if (!s.ok()) {
      LOG(ERROR) << "Tablet failed to init: "
          << tablet_id << " Status: " << s.ToString();
      tablet_peer->SetFailed(s);
      return;
    }

    TRACE("Starting tablet peer");
    s = tablet_peer->Start(bootstrap_info);
    if (!s.ok()) {
      LOG(ERROR) << "Tablet failed to start: "
          << tablet_id << " Status: " << s.ToString();
      tablet_peer->SetFailed(s);
      return;
    }

    tablet_peer->RegisterMaintenanceOps(server_->maintenance_manager());
  }

  int elapsed_ms = MonoTime::Now(MonoTime::FINE).GetDeltaSince(start).ToMilliseconds();
  if (elapsed_ms > FLAGS_tablet_start_warn_threshold_ms) {
    LOG(WARNING) << "Tablet startup for " << tablet_id << " took " << elapsed_ms << "ms";
    if (Trace::CurrentTrace()) {
      LOG(WARNING) << "Trace:\n" << Trace::CurrentTrace()->DumpToString(true);
    }
  }
}

void TSTabletManager::Shutdown() {
  {
    boost::lock_guard<rw_spinlock> lock(lock_);
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

  // Shut down the bootstrap pool, so new tablets are registered after this point.
  open_tablet_pool_->Shutdown();

  // Take a snapshot of the peers list -- that way we don't have to hold
  // on to the lock while shutting them down, which might cause a lock
  // inversion. (see KUDU-308 for example).
  vector<scoped_refptr<TabletPeer> > peers_to_shutdown;
  GetTabletPeers(&peers_to_shutdown);

  BOOST_FOREACH(const scoped_refptr<TabletPeer>& peer, peers_to_shutdown) {
    peer->Shutdown();
  }

  // Shut down the apply pool.
  apply_pool_->Shutdown();

  {
    boost::lock_guard<rw_spinlock> l(lock_);
    // We don't expect anyone else to be modifying the map after we start the
    // shut down process.
    CHECK_EQ(tablet_map_.size(), peers_to_shutdown.size())
      << "Map contents changed during shutdown!";
    tablet_map_.clear();

    state_ = MANAGER_SHUTDOWN;
  }
}

void TSTabletManager::RegisterTablet(const std::string& tablet_id,
                                     const scoped_refptr<TabletPeer>& tablet_peer) {
  boost::lock_guard<rw_spinlock> lock(lock_);
  if (!InsertIfNotPresent(&tablet_map_, tablet_id, tablet_peer)) {
    LOG(FATAL) << "Unable to register tablet peer " << tablet_id << ": already registered!";
  }

  LOG(INFO) << "Registered tablet " << tablet_id;
}

bool TSTabletManager::LookupTablet(const string& tablet_id,
                                   scoped_refptr<TabletPeer>* tablet_peer) const {
  boost::shared_lock<rw_spinlock> shared_lock(lock_);
  return LookupTabletUnlocked(tablet_id, tablet_peer);
}

bool TSTabletManager::LookupTabletUnlocked(const string& tablet_id,
                                           scoped_refptr<TabletPeer>* tablet_peer) const {
  const scoped_refptr<TabletPeer>* found = FindOrNull(tablet_map_, tablet_id);
  if (!found) {
    return false;
  }
  *tablet_peer = *found;
  return true;
}

Status TSTabletManager::GetTabletPeer(const string& tablet_id,
                                      scoped_refptr<tablet::TabletPeer>* tablet_peer) const {
  if (LookupTablet(tablet_id, tablet_peer)) {
    return Status::OK();
  } else {
    return Status::NotFound("Tablet not found", tablet_id);
  }
}

const NodeInstancePB& TSTabletManager::NodeInstance() const {
  return server_->instance_pb();
}

void TSTabletManager::GetTabletPeers(vector<scoped_refptr<TabletPeer> >* tablet_peers) const {
  boost::shared_lock<rw_spinlock> shared_lock(lock_);
  AppendValuesFromMap(tablet_map_, tablet_peers);
}

void TSTabletManager::MarkTabletDirty(const std::string& tablet_id) {
  boost::lock_guard<rw_spinlock> lock(lock_);
  MarkDirtyUnlocked(tablet_id);
}

int TSTabletManager::GetNumDirtyTabletsForTests() const {
  boost::lock_guard<rw_spinlock> lock(lock_);
  return dirty_tablets_.size();
}

void TSTabletManager::MarkDirtyUnlocked(const std::string& tablet_id) {
  TabletReportState* state = FindOrNull(dirty_tablets_, tablet_id);
  if (state != NULL) {
    CHECK_GE(next_report_seq_, state->change_seq);
    state->change_seq = next_report_seq_;
  } else {
    TabletReportState state;
    state.change_seq = next_report_seq_;

    InsertOrDie(&dirty_tablets_, tablet_id, state);
  }
  VLOG(2) << "Will report tablet " << tablet_id << " in report #" << next_report_seq_;
  server_->heartbeater()->TriggerASAP();
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
                                             const scoped_refptr<TabletPeer>& tablet_peer,
                                             ReportedTabletPB* reported_tablet) {
  reported_tablet->set_tablet_id(tablet_id);
  reported_tablet->set_state(tablet_peer->state());
  if (tablet_peer->state() == tablet::FAILED) {
    AppStatusPB* error_status = reported_tablet->mutable_error();
    StatusToPB(tablet_peer->error(), error_status);
  }

  // We cannot get consensus state information until after Consensus is initialized.
  if (tablet_peer->consensus()) {
    *reported_tablet->mutable_committed_consensus_state() =
        tablet_peer->consensus()->CommittedConsensusState();
  }

  if (tablet_peer->tablet() != NULL) {
    reported_tablet->set_schema_version(tablet_peer->tablet()->metadata()->schema_version());
  }
}

void TSTabletManager::GenerateIncrementalTabletReport(TabletReportPB* report) {
  boost::shared_lock<rw_spinlock> shared_lock(lock_);
  report->Clear();
  report->set_sequence_number(next_report_seq_++);
  report->set_is_incremental(true);
  BOOST_FOREACH(const DirtyMap::value_type& dirty_entry, dirty_tablets_) {
    const string& tablet_id = dirty_entry.first;
    scoped_refptr<tablet::TabletPeer>* tablet_peer = FindOrNull(tablet_map_, tablet_id);
    if (tablet_peer) {
      // Dirty entry, report on it.
      CreateReportedTabletPB(tablet_id, *tablet_peer, report->add_updated_tablets());
    } else {
      // Removed.
      report->add_removed_tablet_ids(tablet_id);
    }
  }
}

void TSTabletManager::GenerateFullTabletReport(TabletReportPB* report) {
  boost::shared_lock<rw_spinlock> shared_lock(lock_);
  report->Clear();
  report->set_is_incremental(false);
  report->set_sequence_number(next_report_seq_++);
  BOOST_FOREACH(const TabletMap::value_type& entry, tablet_map_) {
    CreateReportedTabletPB(entry.first, entry.second, report->add_updated_tablets());
  }
  dirty_tablets_.clear();
}

void TSTabletManager::MarkTabletReportAcknowledged(const TabletReportPB& report) {
  boost::lock_guard<rw_spinlock> l(lock_);

  int32_t acked_seq = report.sequence_number();
  CHECK_LT(acked_seq, next_report_seq_);

  // Clear the "dirty" state for any tablets which have not changed since
  // this report.
  DirtyMap::iterator it = dirty_tablets_.begin();
  while (it != dirty_tablets_.end()) {
    const TabletReportState& state = it->second;
    if (state.change_seq <= acked_seq) {
      // This entry has not changed since this tablet report, we no longer need
      // to track it as dirty. If it becomes dirty again, it will be re-added
      // with a higher sequence number.
      it = dirty_tablets_.erase(it);
    } else {
      ++it;
    }
  }
}
} // namespace tserver
} // namespace kudu
