// Copyright (c) 2013, Cloudera, inc.

#include "tserver/ts_tablet_manager.h"

#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <glog/logging.h>
#include <string>
#include <tr1/memory>
#include <vector>

#include "common/wire_protocol.h"
#include "gutil/strings/substitute.h"
#include "gutil/strings/util.h"
#include "master/master.pb.h"
#include "server/fsmanager.h"
#include "server/metadata.pb.h"
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
using kudu::master::ReportedTabletPB;
using kudu::tablet::TabletPeer;
using kudu::master::TabletReportPB;
using kudu::metadata::QuorumPB;
using kudu::metadata::QuorumPeerPB;
using kudu::metadata::TabletMasterBlockPB;
using kudu::metadata::TabletMetadata;
using kudu::tablet::Tablet;

static const char* const kTmpSuffix = ".tmp";
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
    bootstrap_pool_("tablet-bootstrap") {
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
    if (HasSuffixString(child, kTmpSuffix)) {
      LOG(WARNING) << "Ignoring tmp file in master block dir: " << child;
      continue;
    }

    if (HasPrefixString(child, ".")) {
      // Hidden file or ./..
      VLOG(1) << "Ignoring hidden file in master block dir: " << child;
      continue;
    }

    tablets.push_back(child);
  }

  // TODO base the number of parallel tablet bootstraps on something related
  // to the number of physical devices.
  RETURN_NOT_OK(bootstrap_pool_.Init(kNumTabletsToBoostrapSimultaneously));

  // Register the tablets and trigger the asynchronous bootstrap
  vector<shared_ptr<boost::thread> > tablet_initializers;
  BOOST_FOREACH(const string& tablet, tablets) {
    gscoped_ptr<TabletMetadata> meta;
    RETURN_NOT_OK_PREPEND(OpenTabletMeta(tablet, &meta),
                          "Failed to open tablet metadata for tablet: " + tablet);

    shared_ptr<TabletPeer> tablet_peer(new TabletPeer());
    RegisterTablet(meta->oid(), tablet_peer);

    RETURN_NOT_OK(bootstrap_pool_.SubmitFunc(boost::bind(&TSTabletManager::OpenTablet,
                                                         this,
                                                         meta.release())));
  }

  return Status::OK();
}

Status TSTabletManager::WaitForAllBootstrapsToFinish() {
  bootstrap_pool_.Wait();

  Status s = Status::OK();

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

  // If the quorum is local, set the local peer
  if (quorum.local()) {
    QuorumPeerPB quorum_peer;
    quorum_peer.set_permanent_uuid(server_->instance_pb().permanent_uuid());
    quorum.clear_peers();
    quorum.add_peers()->CopyFrom(quorum_peer);
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
  gscoped_ptr<TabletMetadata> meta;
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

  shared_ptr<TabletPeer> new_peer(new TabletPeer());
  RegisterTablet(meta->oid(), new_peer);
  // We can run this synchronously since there is nothing to bootstrap.
  OpenTablet(meta.release());

  if (tablet_peer) {
    *tablet_peer = new_peer;
  }
  return Status::OK();
}

Status TSTabletManager::DeleteTablet(const shared_ptr<TabletPeer>& tablet_peer) {
  TRACE("Deleting tablet $0 (table=$1 [id=$2])", tablet_peer->tablet()->tablet_id(),
        tablet_peer->tablet()->metadata()->table_name(),
        tablet_peer->tablet()->metadata()->table_id());
  RETURN_NOT_OK(tablet_peer->Shutdown());
  tablet_map_.erase(tablet_peer->tablet()->tablet_id());
  // TODO: Trash the data
  return Status::OK();
}

Status TSTabletManager::OpenTabletMeta(const string& tablet_id,
                                       gscoped_ptr<TabletMetadata>* metadata) {
  LOG(INFO) << "Loading master block " << tablet_id;
  TRACE("Loading master block");

  TabletMasterBlockPB master_block;
  RETURN_NOT_OK(LoadMasterBlock(tablet_id, &master_block));
  VLOG(1) << "Loaded master block: " << master_block.ShortDebugString();


  TRACE("Loading metadata...");
  gscoped_ptr<TabletMetadata> meta;
  RETURN_NOT_OK_PREPEND(TabletMetadata::Load(fs_manager_, master_block, &meta),
                        strings::Substitute("Failed to load tablet metadata. Master block: $0",
                                            master_block.ShortDebugString()));
  TRACE("Metadata loaded");
  metadata->reset(meta.release());
  return Status::OK();
}

void TSTabletManager::OpenTablet(TabletMetadata* metadata) {


  gscoped_ptr<TabletMetadata> meta(metadata);

  string tablet_id = meta->oid();

  shared_ptr<TabletPeer> tablet_peer;
  CHECK(LookupTablet(tablet_id, &tablet_peer))
      << "Tablet not registered prior to OpenTabletAsync call: " << tablet_id;

  shared_ptr<Tablet> tablet;
  gscoped_ptr<Log> log;

  LOG(INFO) << "Bootstrapping tablet: " << tablet_id;
  TRACE("Bootstrapping tablet");

  Status s;
  LOG_TIMING(INFO, Substitute("Tablet $0 bootstrap complete.", tablet_id)) {
    // TODO: handle crash mid-creation of tablet? do we ever end up with a
    // partially created tablet here?
    s = BootstrapTablet(meta.Pass(), &metric_ctx_, &tablet, &log);
    if (!s.ok()) {
      LOG(ERROR) << "Tablet failed to bootstrap: "
          << tablet_id << " Status: " << s.ToString();
      tablet_peer->SetFailed(s);
      return;
    }
  }

  QuorumPeerPB quorum_peer;
  quorum_peer.set_permanent_uuid(server_->instance_pb().permanent_uuid());

  LOG_TIMING(INFO, Substitute("Tablet $0 Started.", tablet_id)) {
    TRACE("Initializing tablet peer");
    s =  tablet_peer->Init(tablet,
                           quorum_peer,
                           log.Pass());
    if (!s.ok()) {
      tablet_peer->SetFailed(s);
      return;
    }

    // Check the tablet metadata for the quorum and increase the sequence number.
    QuorumPB initial_config = tablet->metadata()->Quorum();
    initial_config.set_seqno(initial_config.seqno() + 1);

    TRACE("Starting tablet peer");
    s = tablet_peer->Start(initial_config);
    if (!s.ok()) {
      tablet_peer->SetFailed(s);
      return;
    }

    // tablet_peer state changed to RUNNING, mark the tablet dirty
    {
      boost::lock_guard<rw_spinlock> lock(lock_);
      MarkDirtyUnlocked(tablet_id);
    }
  }
}

void TSTabletManager::Shutdown() {
  LOG(INFO) << "Shutting down tablet manager...";
  bootstrap_pool_.Shutdown();
  boost::lock_guard<rw_spinlock> l(lock_);
  BOOST_FOREACH(const TabletMap::value_type &pair, tablet_map_) {
    const std::tr1::shared_ptr<TabletPeer>& peer = pair.second;
    WARN_NOT_OK(peer->Shutdown(), "Unable to close tablet " + peer->tablet()->tablet_id());
  }
  tablet_map_.clear();
  // TODO: add a state variable?
}

Status TSTabletManager::PersistMasterBlock(const TabletMasterBlockPB& pb) {
  string path = fs_manager_->GetMasterBlockPath(pb.tablet_id());
  return pb_util::WritePBToPath(fs_manager_->env(), path, pb);
}

Status TSTabletManager::LoadMasterBlock(const string& tablet_id, TabletMasterBlockPB* block) {
  string path = fs_manager_->GetMasterBlockPath(tablet_id);
  RETURN_NOT_OK(pb_util::ReadPBFromPath(fs_manager_->env(), path, block));
  if (tablet_id != block->tablet_id()) {
    LOG_AND_RETURN(ERROR, Status::Corruption(
                     strings::Substitute("Corrupt master block $0: PB has wrong tablet ID",
                                         path),
                     block->ShortDebugString()));
  }

  return Status::OK();
}

void TSTabletManager::RegisterTablet(const std::string& tablet_id,
                                     const std::tr1::shared_ptr<TabletPeer>& tablet_peer) {
  boost::lock_guard<rw_spinlock> lock(lock_);
  if (!InsertIfNotPresent(&tablet_map_, tablet_id, tablet_peer)) {
    LOG(FATAL) << "Unable to register tablet peer " << tablet_id << ": already registered!";
  }

  MarkDirtyUnlocked(tablet_id);

  LOG(INFO) << "Registered tablet " << tablet_id;
}

bool TSTabletManager::LookupTablet(const string& tablet_id,
                                   std::tr1::shared_ptr<TabletPeer>* tablet_peer) const {
  boost::shared_lock<rw_spinlock> lock(lock_);
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
  boost::shared_lock<rw_spinlock> lock(lock_);
  AppendValuesFromMap(tablet_map_, tablet_peers);
}

void TSTabletManager::GetOnlineTabletPeers(
  vector<shared_ptr<TabletPeer> >* online_tablet_peers) const {
  vector<shared_ptr<TabletPeer> > tablet_peers;
  GetTabletPeers(&tablet_peers);
  BOOST_FOREACH(const shared_ptr<TabletPeer>& peer, tablet_peers) {
    if (peer->state() != metadata::BOOTSTRAPPING) {
      online_tablet_peers->push_back(peer);
    }
  }
}

void TSTabletManager::MarkDirtyUnlocked(const std::string& tablet_id) {
  TabletReportState* state = FindOrNull(dirty_tablets_, tablet_id);
  if (state != NULL) {
    CHECK_GE(next_report_seq_, state->change_seq_);
    state->change_seq_ = next_report_seq_;
  } else {
    TabletReportState state;
    state.change_seq_ = next_report_seq_;

    InsertOrDie(&dirty_tablets_, tablet_id, state);
  }
  VLOG(2) << "Will report tablet " << tablet_id << " in report #" << next_report_seq_;
}

void TSTabletManager::AcknowledgeTabletReport(const TabletReportPB& report) {
  boost::shared_lock<rw_spinlock> lock(lock_);

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
}

void TSTabletManager::GenerateTabletReport(TabletReportPB* report) {
  // Generate an incremental report
  boost::shared_lock<rw_spinlock> lock(lock_);
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
  boost::shared_lock<rw_spinlock> lock(lock_);
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
