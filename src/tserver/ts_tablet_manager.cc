// Copyright (c) 2013, Cloudera, inc.

#include "tserver/ts_tablet_manager.h"

#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <glog/logging.h>
#include <string>
#include <tr1/memory>
#include <vector>

#include "common/maintenance_manager.h"
#include "common/wire_protocol.h"
#include "consensus/log.h"
#include "consensus/opid_anchor_registry.h"
#include "gutil/strings/substitute.h"
#include "gutil/strings/util.h"
#include "master/master.pb.h"
#include "server/fsmanager.h"
#include "server/metadata.pb.h"
#include "tablet/tablet.pb.h"
#include "tablet/tablet.h"
#include "tablet/tablet_bootstrap.h"
#include "tablet/tablet_peer.h"
#include "tserver/tablet_server.h"
#include "util/env.h"
#include "util/env_util.h"
#include "util/metrics.h"
#include "util/pb_util.h"
#include "util/stopwatch.h"
#include "util/trace.h"

using std::string;
using std::tr1::shared_ptr;
using std::vector;
using strings::Substitute;
using kudu::log::Log;
using kudu::log::OpIdAnchorRegistry;
using kudu::master::ReportedTabletPB;
using kudu::tablet::TabletPeer;
using kudu::tablet::TabletStatusListener;
using kudu::tablet::TabletStatusPB;
using kudu::master::TabletReportPB;
using kudu::metadata::QuorumPB;
using kudu::metadata::QuorumPeerPB;
using kudu::metadata::TabletMasterBlockPB;
using kudu::metadata::TabletMetadata;
using kudu::tablet::Tablet;

static const int kNumTabletsToBoostrapSimultaneously = 4;

namespace kudu {
namespace tserver {

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
    bootstrap_pool_("tablet-bootstrap",
                    0, kNumTabletsToBoostrapSimultaneously, ThreadPool::DEFAULT_TIMEOUT) {
}

TSTabletManager::~TSTabletManager() {
}

Status TSTabletManager::Init() {
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

  // TODO base the number of parallel tablet bootstraps on something related
  // to the number of physical devices.
  RETURN_NOT_OK(bootstrap_pool_.Init());

  // Register the tablets and trigger the asynchronous bootstrap
  BOOST_FOREACH(const string& tablet, tablets) {
    scoped_refptr<TabletMetadata> meta;
    RETURN_NOT_OK_PREPEND(OpenTabletMeta(tablet, &meta),
                          "Failed to open tablet metadata for tablet: " + tablet);
    QuorumPeerPB quorum_peer;
    quorum_peer.set_permanent_uuid(server_->instance_pb().permanent_uuid());
    shared_ptr<TabletPeer> tablet_peer(
        new TabletPeer(meta,
                       quorum_peer,
                       metric_ctx_,
                       boost::bind(&TSTabletManager::MarkTabletDirty, this, _1)));
    RegisterTablet(meta->oid(), tablet_peer);

    RETURN_NOT_OK(bootstrap_pool_.SubmitFunc(boost::bind(&TSTabletManager::OpenTablet,
                                                         this,
                                                         meta)));
  }

  return Status::OK();
}

Status TSTabletManager::WaitForAllBootstrapsToFinish() {
  bootstrap_pool_.Wait();

  Status s = Status::OK();

  boost::shared_lock<rw_spinlock> shared_lock(lock_);
  BOOST_FOREACH(const TabletMap::value_type& entry, tablet_map_) {
    if (entry.second->state() == metadata::FAILED) {
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
                                        shared_ptr<TabletPeer>* tablet_peer) {

  // If the quorum is specified to use local consensus, verify that the peer
  // matches up with our local info.
  if (quorum.local()) {
    CHECK_EQ(1, quorum.peers_size());
    CHECK_EQ(server_->instance_pb().permanent_uuid(),
             quorum.peers(0).permanent_uuid());
    CHECK_EQ(QuorumPeerPB::LEADER, quorum.peers(0).role());
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
    shared_ptr<TabletPeer> junk;
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
  master_block.set_block_a(fs_manager_->GenerateName());
  master_block.set_block_b(fs_manager_->GenerateName());

  TRACE("Creating new master block...");
  scoped_refptr<TabletMetadata> meta;
  RETURN_NOT_OK_PREPEND(
    TabletMetadata::CreateNew(fs_manager_,
                              master_block,
                              table_name,
                              schema,
                              quorum,
                              start_key,
                              end_key,
                              &meta),
    "Couldn't create tablet metadata");

  TRACE("Persisting new master block...");
  RETURN_NOT_OK_PREPEND(PersistMasterBlock(master_block),
                        "Couldn't persist master block for new tablet");

  QuorumPeerPB quorum_peer;
  quorum_peer.set_permanent_uuid(server_->instance_pb().permanent_uuid());
  shared_ptr<TabletPeer> new_peer(
      new TabletPeer(meta,
                     quorum_peer,
                     metric_ctx_,
                     boost::bind(&TSTabletManager::MarkTabletDirty, this, _1)));
  RegisterTablet(meta->oid(), new_peer);
  // We can run this synchronously since there is nothing to bootstrap.
  OpenTablet(meta);

  if (tablet_peer) {
    *tablet_peer = new_peer;
  }
  return Status::OK();
}

Status TSTabletManager::DeleteTablet(const shared_ptr<TabletPeer>& tablet_peer) {
  TRACE("Deleting tablet $0 (table=$1 [id=$2])", tablet_peer->tablet()->tablet_id(),
        tablet_peer->tablet()->metadata()->table_name(),
        tablet_peer->tablet()->metadata()->table_id());
  tablet_peer->Shutdown();
  boost::lock_guard<rw_spinlock> lock(lock_);
  tablet_map_.erase(tablet_peer->tablet()->tablet_id());
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

  shared_ptr<TabletPeer> tablet_peer;
  CHECK(LookupTablet(tablet_id, &tablet_peer))
      << "Tablet not registered prior to OpenTabletAsync call: " << tablet_id;

  shared_ptr<Tablet> tablet;
  gscoped_ptr<Log> log;
  scoped_refptr<OpIdAnchorRegistry> opid_anchor_registry;

  LOG(INFO) << "Bootstrapping tablet: " << tablet_id;
  TRACE("Bootstrapping tablet");

  consensus::ConsensusBootstrapInfo bootstrap_info;
  Status s;
  LOG_TIMING(INFO, Substitute("Tablet $0 bootstrap complete.", tablet_id)) {
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


  LOG_TIMING(INFO, Substitute("Tablet $0 Started.", tablet_id)) {
    TRACE("Initializing tablet peer");

    // Check the tablet metadata for the quorum
    CHECK(tablet->metadata()->Quorum().IsInitialized());

    s =  tablet_peer->Init(tablet,
                           scoped_refptr<server::Clock>(server_->clock()),
                           server_->messenger(),
                           log.Pass(),
                           opid_anchor_registry.get());

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
}

void TSTabletManager::Shutdown() {
  LOG(INFO) << "Shutting down tablet manager...";

  // Shut down the bootstrap pool, so new tablets are registered after this point.
  bootstrap_pool_.Shutdown();

  // Take a snapshot of the peers list -- that way we don't have to hold
  // on to the lock while shutting them down, which might cause a lock
  // inversion. (see KUDU-308 for example).
  vector<shared_ptr<TabletPeer> > peers_to_shutdown;
  GetTabletPeers(&peers_to_shutdown);

  BOOST_FOREACH(const shared_ptr<TabletPeer>& peer, peers_to_shutdown) {
    peer->Shutdown();
  }

  {
    boost::lock_guard<rw_spinlock> l(lock_);
    // We don't expect anyone else to be modifying the map after we start the
    // shut down process.
    CHECK_EQ(tablet_map_.size(), peers_to_shutdown.size())
      << "Map contents changed during shutdown!";
    tablet_map_.clear();
  }
  // TODO: add a state variable?
}

Status TSTabletManager::PersistMasterBlock(const TabletMasterBlockPB& pb) {
  string path = fs_manager_->GetMasterBlockPath(pb.tablet_id());
  return pb_util::WritePBToPath(fs_manager_->env(), path, pb);
}

Status TSTabletManager::LoadMasterBlock(const string& tablet_id, TabletMasterBlockPB* block) {
  string path = fs_manager_->GetMasterBlockPath(tablet_id);
  return TabletMetadata::OpenMasterBlock(fs_manager_->env(), path, tablet_id, block);
}

void TSTabletManager::RegisterTablet(const std::string& tablet_id,
                                     const std::tr1::shared_ptr<TabletPeer>& tablet_peer) {
  boost::lock_guard<rw_spinlock> lock(lock_);
  if (!InsertIfNotPresent(&tablet_map_, tablet_id, tablet_peer)) {
    LOG(FATAL) << "Unable to register tablet peer " << tablet_id << ": already registered!";
  }

  MarkDirtyUnlocked(tablet_peer.get());

  LOG(INFO) << "Registered tablet " << tablet_id;
}

bool TSTabletManager::LookupTablet(const string& tablet_id,
                                   std::tr1::shared_ptr<TabletPeer>* tablet_peer) const {
  boost::shared_lock<rw_spinlock> shared_lock(lock_);
  return LookupTabletUnlocked(tablet_id, tablet_peer);
}

bool TSTabletManager::LookupTabletUnlocked(const string& tablet_id,
                                           std::tr1::shared_ptr<TabletPeer>* tablet_peer) const {
  const std::tr1::shared_ptr<TabletPeer>* found = FindOrNull(tablet_map_, tablet_id);
  if (!found) {
    return false;
  }
  *tablet_peer = *found;
  return true;
}

void TSTabletManager::GetTabletPeers(vector<shared_ptr<TabletPeer> >* tablet_peers) const {
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
    CHECK_GE(next_report_seq_, state->change_seq_);
    state->change_seq_ = next_report_seq_;
  } else {
    TabletReportState state;
    state.change_seq_ = next_report_seq_;

    InsertOrDie(&dirty_tablets_, tablet_peer->tablet_id(), state);
  }
  VLOG(2) << "Will report tablet " << tablet_peer->tablet_id()
      << " in report #" << next_report_seq_;
}

void TSTabletManager::AcknowledgeTabletReport(const TabletReportPB& report) {
  boost::shared_lock<rw_spinlock> shared_lock(lock_);

  int32_t acked_seq = report.sequence_number();
  CHECK_LT(acked_seq, next_report_seq_);

  // Clear the "dirty" state for any tablets which have not changed since
  // this report.
  for (DirtyMap::iterator it = dirty_tablets_.begin();
       it != dirty_tablets_.end();) {
    const TabletReportState& state = (*it).second;

    if (state.change_seq_ <= acked_seq) {
      // This entry has not changed since this tablet report, we no longer need
      // to track it as dirty. If it becomes dirty again, it will be re-added
      // with a higher sequence number.
      it = dirty_tablets_.erase(it);
      continue;
    } else {
      ++it;
    }
  }
}

void TSTabletManager::CreateReportedTabletPB(const string& tablet_id,
                                             const shared_ptr<TabletPeer>& tablet_peer,
                                             ReportedTabletPB* reported_tablet) {
  reported_tablet->set_tablet_id(tablet_id);
  reported_tablet->set_state(tablet_peer->state());
  if (tablet_peer->state() == metadata::FAILED) {
    AppStatusPB* error_status = reported_tablet->mutable_error();
    StatusToPB(tablet_peer->error(), error_status);
  }
  reported_tablet->set_role(tablet_peer->role());

  if (tablet_peer->tablet() != NULL) {
    reported_tablet->set_schema_version(tablet_peer->tablet()->metadata()->schema_version());
  }
}

void TSTabletManager::GenerateTabletReport(TabletReportPB* report) {
  // Generate an incremental report
  boost::shared_lock<rw_spinlock> shared_lock(lock_);
  report->Clear();
  report->set_sequence_number(next_report_seq_++);
  report->set_is_incremental(true);
  for (DirtyMap::iterator it = dirty_tablets_.begin();
       it != dirty_tablets_.end();) {
    const string& tablet_id = (*it).first;

    // The entry is actually dirty, so report it.
    shared_ptr<kudu::tablet::TabletPeer>* entry = FindOrNull(tablet_map_, tablet_id);
    if (entry != NULL) {
      CreateReportedTabletPB(tablet_id, *entry, report->add_updated_tablets());
    } else {
      report->add_removed_tablet_ids(tablet_id);
    }

    ++it;
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

} // namespace tserver
} // namespace kudu
