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
#include "kudu/consensus/opid_anchor_registry.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/master/master.pb.h"
#include "kudu/server/metadata.pb.h"
#include "kudu/tablet/maintenance_manager.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_bootstrap.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_peer.h"
#include "kudu/tserver/tablet_server.h"
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
DEFINE_int32(log_gc_sleep_delay_ms, 10000,
    "Minimum number of milliseconds that the maintenance manager will wait between log GC runs.");

METRIC_DEFINE_gauge_uint32(log_gc_running, kudu::MetricUnit::kMaintenanceOperations,
                           "Number of log GC operations currently running.");
METRIC_DEFINE_histogram(log_gc_duration, kudu::MetricUnit::kSeconds,
                        "Seconds spent garbage collecting the logs.", 60000000LU, 2);

namespace kudu {
namespace tserver {

using consensus::ConsensusMetadata;
using log::Log;
using log::OpIdAnchorRegistry;
using master::ReportedTabletPB;
using master::TabletReportPB;
using metadata::QuorumPB;
using metadata::QuorumPeerPB;
using std::string;
using std::tr1::shared_ptr;
using std::vector;
using strings::Substitute;
using tablet::Tablet;
using tablet::TabletMasterBlockPB;
using tablet::TabletMetadata;
using tablet::TabletPeer;
using tablet::TabletStatusListener;
using tablet::TabletStatusPB;

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
                                 const MetricContext& metric_ctx)
  : fs_manager_(fs_manager),
    server_(server),
    next_report_seq_(0),
    metric_ctx_(metric_ctx),
    state_(MANAGER_INITIALIZING) {
  // TODO base the number of parallel tablet bootstraps on something related
  // to the number of physical devices.
  // Right now it's set to be the same as the number of RPC handlers so that we
  // can process as many of them in parallel as we can.
  CHECK_OK(ThreadPoolBuilder("tablet-bootstrap")
      .set_max_threads(FLAGS_num_tablets_to_open_simultaneously)
      .Build(&open_tablet_pool_));
  // TODO currently these are initialized to default values: no
  // minimum number of threads, 500 ms idle timeout, and maximum
  // number of threads equal to number of CPU cores. Instead, it
  // likewise makes more sense to set this equal to the number of
  // physical storage devices available to us.
  CHECK_OK(ThreadPoolBuilder("ldr-apply").Build(&leader_apply_pool_));
  CHECK_OK(ThreadPoolBuilder("repl-apply").Build(&replica_apply_pool_));
}

TSTabletManager::~TSTabletManager() {
}

Status TSTabletManager::Init() {
  CHECK_EQ(state(), MANAGER_INITIALIZING);

  vector<string> children;
  RETURN_NOT_OK_PREPEND(fs_manager_->ListDir(fs_manager_->GetMasterBlockDir(), &children),
                        "Couldn't list master blocks");

  vector<string> tablets;

  // Search for tablets in the master block dir
  BOOST_FOREACH(const string& child, children) {
    if (!Tablet::IsTabletFileName(child)) {
      continue;
    }
    tablets.push_back(child);
  }

  // Register the tablets and trigger the asynchronous bootstrap
  BOOST_FOREACH(const string& tablet, tablets) {
    scoped_refptr<TabletMetadata> meta;
    RETURN_NOT_OK_PREPEND(OpenTabletMeta(tablet, &meta),
                          "Failed to open tablet metadata for tablet: " + tablet);
    scoped_refptr<TabletPeer> tablet_peer(
        new TabletPeer(meta,
                       leader_apply_pool_.get(),
                       replica_apply_pool_.get(),
                       boost::bind(&TSTabletManager::MarkTabletDirty, this, _1)));
    RegisterTablet(meta->oid(), tablet_peer);

    RETURN_NOT_OK(open_tablet_pool_->SubmitFunc(boost::bind(&TSTabletManager::OpenTablet,
                                                this,
                                                meta)));
  }

  RegisterMaintenanceOp();

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
                                        QuorumPB quorum,
                                        scoped_refptr<TabletPeer>* tablet_peer) {
  CHECK_EQ(state(), MANAGER_RUNNING);

  // If the quorum is specified to use local consensus, verify that the peer
  // matches up with our local info.
  if (quorum.local()) {
    CHECK_EQ(1, quorum.peers_size());
    CHECK_EQ(server_->instance_pb().permanent_uuid(),
             quorum.peers(0).permanent_uuid());
    CHECK_EQ(QuorumPeerPB::CANDIDATE, quorum.peers(0).role());
  }

  // Set the initial sequence number to -1, disregarding the passed sequence
  // number, if any.
  quorum.set_seqno(-1);

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

  // Create a new master block
  TabletMasterBlockPB master_block;
  master_block.set_table_id(table_id);
  master_block.set_tablet_id(tablet_id);
  master_block.set_block_a(fs_manager_->GenerateBlockId().ToString());
  master_block.set_block_b(fs_manager_->GenerateBlockId().ToString());

  TRACE("Creating new master block...");
  scoped_refptr<TabletMetadata> meta;
  RETURN_NOT_OK_PREPEND(
    TabletMetadata::CreateNew(fs_manager_,
                              master_block,
                              table_name,
                              schema,
                              start_key,
                              end_key,
                              tablet::REMOTE_BOOTSTRAP_DONE,
                              &meta),
    "Couldn't create tablet metadata");

  TRACE("Persisting new master block...");
  RETURN_NOT_OK_PREPEND(PersistMasterBlock(master_block),
                        "Couldn't persist master block for new tablet");

  // We must persist the consensus metadata to disk before starting a new
  // tablet's TabletPeer and Consensus implementation.
  gscoped_ptr<ConsensusMetadata> cmeta;
  RETURN_NOT_OK_PREPEND(ConsensusMetadata::Create(fs_manager_, tablet_id, quorum,
                                                  consensus::kMinimumTerm, &cmeta),
                        "Unable to create new ConsensusMeta for tablet " + tablet_id);

  scoped_refptr<TabletPeer> new_peer(
      new TabletPeer(meta,
                     leader_apply_pool_.get(),
                     replica_apply_pool_.get(),
                     boost::bind(&TSTabletManager::MarkTabletDirty, this, _1)));
  RegisterTablet(meta->oid(), new_peer);
  // We can run this synchronously since there is nothing to bootstrap.
  RETURN_NOT_OK(open_tablet_pool_->SubmitFunc(boost::bind(&TSTabletManager::OpenTablet,
                                              this,
                                              meta)));

  if (tablet_peer) {
    *tablet_peer = new_peer;
  }
  return Status::OK();
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
  boost::lock_guard<rw_spinlock> lock(lock_);
  CHECK_EQ(1, tablet_map_.erase(tablet_peer->tablet()->tablet_id()));
  // TODO: Trash the data
  return Status::OK();
}

Status TSTabletManager::OpenTabletMeta(const string& tablet_id,
                                       scoped_refptr<TabletMetadata>* metadata) {
  LOG(INFO) << "Loading master block " << tablet_id;
  TRACE("Loading master block");

  TabletMasterBlockPB master_block;
  RETURN_NOT_OK(LoadMasterBlock(tablet_id, &master_block));
  VLOG(1) << "Loaded master block: " << master_block.ShortDebugString();


  TRACE("Loading metadata...");
  scoped_refptr<TabletMetadata> meta;
  RETURN_NOT_OK_PREPEND(TabletMetadata::Load(fs_manager_, master_block, &meta),
                        strings::Substitute("Failed to load tablet metadata. Master block: $0",
                                            master_block.ShortDebugString()));
  TRACE("Metadata loaded");
  metadata->swap(meta);
  return Status::OK();
}

void TSTabletManager::OpenTablet(const scoped_refptr<TabletMetadata>& meta) {
  string tablet_id = meta->oid();

  scoped_refptr<TabletPeer> tablet_peer;
  CHECK(LookupTablet(tablet_id, &tablet_peer))
      << "Tablet not registered prior to OpenTabletAsync call: " << tablet_id;

  shared_ptr<Tablet> tablet;
  gscoped_ptr<Log> log;
  scoped_refptr<OpIdAnchorRegistry> opid_anchor_registry;

  LOG(INFO) << "Bootstrapping tablet: " << tablet_id;
  TRACE("Bootstrapping tablet");

  consensus::ConsensusBootstrapInfo bootstrap_info;
  Status s;
  LOG_TIMING(INFO, Substitute("tablet $0 bootstrap", tablet_id)) {
    // TODO: handle crash mid-creation of tablet? do we ever end up with a
    // partially created tablet here?
    s = BootstrapTablet(meta,
                        scoped_refptr<server::Clock>(server_->clock()),
                        &metric_ctx_,
                        tablet_peer->status_listener(),
                        &tablet,
                        &log,
                        &opid_anchor_registry,
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
                           log.Pass(),
                           *tablet->GetMetricContext());

    if (!s.ok()) {
      tablet_peer->SetFailed(s);
      return;
    }

    TRACE("Starting tablet peer");
    s = tablet_peer->Start(bootstrap_info);
    if (!s.ok()) {
      tablet_peer->SetFailed(s);
      return;
    }

    tablet_peer->tablet()->RegisterMaintenanceOps(
                server_->maintenance_manager());
    // tablet_peer state changed to RUNNING, mark the tablet dirty
    {
      boost::lock_guard<rw_spinlock> lock(lock_);
      MarkDirtyUnlocked(tablet_peer.get());
    }
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

  // Stop the log GC maintenance operation.
  log_gc_op_->Unregister();

  // Take a snapshot of the peers list -- that way we don't have to hold
  // on to the lock while shutting them down, which might cause a lock
  // inversion. (see KUDU-308 for example).
  vector<scoped_refptr<TabletPeer> > peers_to_shutdown;
  GetTabletPeers(&peers_to_shutdown);

  BOOST_FOREACH(const scoped_refptr<TabletPeer>& peer, peers_to_shutdown) {
    peer->Shutdown();
  }

  // Shut down the apply executors.
  leader_apply_pool_->Shutdown();
  replica_apply_pool_->Shutdown();

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

Status TSTabletManager::PersistMasterBlock(const TabletMasterBlockPB& pb) {
  return TabletMetadata::PersistMasterBlock(fs_manager_, pb);
}

Status TSTabletManager::LoadMasterBlock(const string& tablet_id, TabletMasterBlockPB* block) {
  string path = fs_manager_->GetMasterBlockPath(tablet_id);
  return TabletMetadata::OpenMasterBlock(fs_manager_->env(), path, tablet_id, block);
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

void TSTabletManager::MarkTabletDirty(TabletPeer* tablet_peer) {
  boost::lock_guard<rw_spinlock> lock(lock_);
  MarkDirtyUnlocked(tablet_peer);
}


void TSTabletManager::MarkDirtyUnlocked(TabletPeer* tablet_peer) {
  TabletReportState* state = FindOrNull(dirty_tablets_, tablet_peer->tablet_id());
  if (state != NULL) {
    CHECK_GE(next_report_seq_, state->change_seq);
    state->change_seq = next_report_seq_;
  } else {
    TabletReportState state;
    state.change_seq = next_report_seq_;

    InsertOrDie(&dirty_tablets_, tablet_peer->tablet_id(), state);
  }
  VLOG(2) << "Will report tablet " << tablet_peer->tablet_id()
      << " in report #" << next_report_seq_;
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

  // We cannot call role() until after consensus is initialized.
  if (tablet_peer->consensus()) {
    QuorumPB quorum = tablet_peer->Quorum();
    reported_tablet->set_role(consensus::GetRoleInQuorum(server_->instance_pb().permanent_uuid(),
                                                         quorum));
    reported_tablet->mutable_quorum()->CopyFrom(quorum);
  } else {
    reported_tablet->set_role(QuorumPeerPB::NON_PARTICIPANT);
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
  boost::shared_lock<rw_spinlock> shared_lock(lock_);

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

Status TSTabletManager::RunAllLogGC() {
  TabletMap tablet_map_copy;
  {
    boost::shared_lock<rw_spinlock> shared_lock(lock_);
    tablet_map_copy.insert(tablet_map_.begin(), tablet_map_.end());
  }
  BOOST_FOREACH(const TabletMap::value_type& entry, tablet_map_copy) {
    tablet::TabletStatePB tablet_state = entry.second->state();
    // Since we took a copy, the tablet might have been erased from the map. The scoped_refptr will
    // still be good but the tablet might just be waiting to be destroyed.
    if (tablet_state != tablet::QUIESCING && tablet_state != tablet::SHUTDOWN) {
      WARN_NOT_OK(entry.second->RunLogGC(),
                  Substitute("Failed to run log GC on tablet $0", entry.first));
    }
  }
  return Status::OK();
}

// Maintenance task that runs log GC in all tablets. Will wait at least log_gc_sleep_delay_ms
// between each run and after that the performance improvement is of 1 for each second elapsed after
// the original delay.
//
// Only one LogGC op can run at a time.
class LogGCOp : public MaintenanceOp {
 public:
  LogGCOp(TSTabletManager* tablet_manager, const MetricContext& metric_ctx)
    : MaintenanceOp("LogGCOp"),
      tablet_manager_(tablet_manager),
      log_gc_duration_(METRIC_log_gc_duration.Instantiate(metric_ctx)),
      log_gc_running_(AtomicGauge<uint32_t>::Instantiate(METRIC_log_gc_running, metric_ctx)),
      sem_(1) {
    time_since_last_run_.start();
  }

  virtual void UpdateStats(MaintenanceOpStats* stats) OVERRIDE {
    stats->ram_anchored = 0;
    stats->ts_anchored_secs = 0;
    stats->runnable = sem_.GetValue() == 1;
    double elapsed_ms = time_since_last_run_.elapsed().wall_millis();
    if (elapsed_ms > FLAGS_log_gc_sleep_delay_ms) {
      double extra_millis = elapsed_ms - FLAGS_log_gc_sleep_delay_ms;
      stats->perf_improvement = std::max(extra_millis / 1000, 0.05);
    }
  }

  virtual bool Prepare() OVERRIDE {
    return sem_.try_lock();
  }

  virtual void Perform() OVERRIDE {
    CHECK(!sem_.try_lock());

    tablet_manager_->RunAllLogGC();
    time_since_last_run_.start();

    sem_.unlock();
  }

  virtual Histogram* DurationHistogram() OVERRIDE {
    return log_gc_duration_;
  }

  virtual AtomicGauge<uint32_t>* RunningGauge() OVERRIDE {
    return log_gc_running_;
  }

 private:
  TSTabletManager* tablet_manager_;
  Stopwatch time_since_last_run_;
  Histogram* log_gc_duration_;
  AtomicGauge<uint32_t>* log_gc_running_;
  mutable Semaphore sem_;
};

void TSTabletManager::RegisterMaintenanceOp() {
  log_gc_op_.reset(new LogGCOp(this, metric_ctx_));
  server_->maintenance_manager()->RegisterOp(log_gc_op_.get());
}

} // namespace tserver
} // namespace kudu
