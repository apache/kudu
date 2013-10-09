// Copyright (c) 2013, Cloudera, inc.

#include "tserver/ts_tablet_manager.h"

#include <boost/thread/mutex.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <glog/logging.h>
#include <string>
#include <tr1/memory>
#include <vector>

#include "gutil/strings/substitute.h"
#include "gutil/strings/util.h"
#include "master/master.pb.h"
#include "server/fsmanager.h"
#include "server/metadata.pb.h"
#include "tablet/tablet.h"
#include "tablet/tablet_peer.h"
#include "util/env.h"
#include "util/env_util.h"
#include "util/metrics.h"
#include "util/pb_util.h"

using std::string;
using std::tr1::shared_ptr;
using std::vector;
using kudu::tablet::TabletPeer;
using kudu::master::TabletReportPB;
using kudu::metadata::TabletMasterBlockPB;
using kudu::metadata::TabletMetadata;
using kudu::tablet::Tablet;

static const char* const kTmpSuffix = ".tmp";

namespace kudu {
namespace tserver {

TSTabletManager::TSTabletManager(FsManager* fs_manager, const MetricContext& metric_ctx)
  : fs_manager_(fs_manager),
    next_report_seq_(0),
    metric_ctx_(metric_ctx) {
}

TSTabletManager::~TSTabletManager() {
}

Status TSTabletManager::Init() {
  vector<string> children;
  RETURN_NOT_OK_PREPEND(fs_manager_->ListDir(fs_manager_->GetMasterBlockDir(), &children),
                        "Couldn't list master blocks");
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

    RETURN_NOT_OK_PREPEND(OpenTablet(child), "Failed to open tablet " + child);
    // TODO: should we still start up even if some fraction of the tablets are unavailable?
    // perhaps create a TabletPeer in a corrupt state? Probably -- so we have a kind of
    // quarantine and a single bad tablet doesn't block the whole server from starting.
  }
  return Status::OK();
}

Status TSTabletManager::CreateNewTablet(const string& tablet_id,
                                        const string& start_key, const string& end_key,
                                        const Schema& schema,
                                        shared_ptr<TabletPeer>* tablet_peer) {
  {
    // Sanity check that the tablet isn't already registered.
    // This fires a FATAL in debug mode, so callers should not depend on the
    // AlreadyPresent result here to check if a tablet needs to be created.
    shared_ptr<TabletPeer> junk;
    if (LookupTablet(tablet_id, &junk)) {
      LOG(DFATAL) << "Trying to create an already-existing tablet " << tablet_id;
      return Status::AlreadyPresent("Tablet already registered", tablet_id);
    }
  }

  // Create a new master block
  TabletMasterBlockPB master_block;
  master_block.set_tablet_id(tablet_id);
  master_block.set_block_a(fs_manager_->GenerateName());
  master_block.set_block_b(fs_manager_->GenerateName());

  gscoped_ptr<TabletMetadata> meta;
  RETURN_NOT_OK_PREPEND(
    TabletMetadata::CreateNew(fs_manager_, master_block, schema, start_key, end_key, &meta),
    "Couldn't create tablet metadata");

  RETURN_NOT_OK_PREPEND(PersistMasterBlock(master_block),
                        "Couldn't persist master block for new tablet");

  return OpenTablet(meta.Pass(), tablet_peer);
}

Status TSTabletManager::OpenTablet(const string& tablet_id) {
  LOG(INFO) << "Loading master block " << tablet_id;

  TabletMasterBlockPB master_block;
  RETURN_NOT_OK(LoadMasterBlock(tablet_id, &master_block));
  VLOG(1) << "Loaded master block: " << master_block.ShortDebugString();

  gscoped_ptr<TabletMetadata> meta;
  RETURN_NOT_OK_PREPEND(TabletMetadata::Load(fs_manager_, master_block, &meta),
                        strings::Substitute("Failed to load tablet metadata. Master block: $0",
                                            master_block.ShortDebugString()));
  return OpenTablet(meta.Pass(), NULL);
}

Status TSTabletManager::OpenTablet(gscoped_ptr<TabletMetadata> meta,
                                   shared_ptr<TabletPeer>* peer) {
  shared_ptr<Tablet> tablet(new Tablet(meta.Pass()));

  RETURN_NOT_OK(tablet->Open());
  // TODO: handle crash mid-creation of tablet? do we ever end up with a partially created tablet here?

  shared_ptr<TabletPeer> tablet_peer(new TabletPeer(tablet, metric_ctx_));
  RETURN_NOT_OK_PREPEND(tablet_peer->Init(), "Failed to Init() TabletPeer");
  RETURN_NOT_OK_PREPEND(tablet_peer->Start(), "Failed to Start() TabletPeer");

  RegisterTablet(tablet_peer);

  if (peer) *peer = tablet_peer;
  return Status::OK();
}

void TSTabletManager::Shutdown() {
  LOG(INFO) << "Shutting down tablet manager...";
  boost::lock_guard<rw_spinlock> l(lock_);
  BOOST_FOREACH(const TabletMap::value_type &pair, tablet_map_) {
    const std::tr1::shared_ptr<TabletPeer>& peer = pair.second;
    WARN_NOT_OK(peer->Shutdown(), "Unable to close tablet " + peer->tablet()->tablet_id());
  }
  tablet_map_.clear();
  // TODO: add a state variable?
}

Status TSTabletManager::PersistMasterBlock(const TabletMasterBlockPB& pb) {
  // TODO: refactor this "atomic write" stuff into a utility function.
  Env* env = fs_manager_->env();
  string path = fs_manager_->GetMasterBlockPath(pb.tablet_id());
  string path_tmp = path + kTmpSuffix;

  shared_ptr<WritableFile> file;
  RETURN_NOT_OK_PREPEND(env_util::OpenFileForWrite(env, path_tmp, &file),
                        "Couldn't open master block file in " + path_tmp);
  env_util::ScopedFileDeleter tmp_deleter(env, path_tmp);

  if (!pb_util::SerializeToWritableFile(pb, file.get())) {
    return Status::IOError("Failed to serialize to file");
  }
  RETURN_NOT_OK_PREPEND(file->Flush(), "Failed to Flush() " + path_tmp);
  RETURN_NOT_OK_PREPEND(file->Sync(), "Failed to Sync() " + path_tmp);
  RETURN_NOT_OK_PREPEND(file->Close(), "Failed to Close() " + path_tmp);
  RETURN_NOT_OK_PREPEND(env->RenameFile(path_tmp, path), "Failed to rename tmp file to " + path);
  tmp_deleter.Cancel();
  return Status::OK();
}

Status TSTabletManager::LoadMasterBlock(const string& tablet_id, TabletMasterBlockPB* block) {
  // TODO: refactor this into some common utility function shared with the metadata block
  // stuff in FsManager
  string path = fs_manager_->GetMasterBlockPath(tablet_id);
  shared_ptr<SequentialFile> rfile;
  RETURN_NOT_OK(env_util::OpenFileForSequential(fs_manager_->env(), path, &rfile));
  if (!pb_util::ParseFromSequentialFile(block, rfile.get())) {
    return Status::IOError("Unable to parse tablet master block from " + path);
  }

  if (tablet_id != block->tablet_id()) {
    LOG_AND_RETURN(ERROR, Status::Corruption(
                     strings::Substitute("Corrupt master block $0: PB has wrong tablet ID",
                                         path),
                     block->ShortDebugString()));
  }

  return Status::OK();
}

void TSTabletManager::RegisterTablet(const std::tr1::shared_ptr<TabletPeer>& tablet_peer) {
  const string& id = tablet_peer->tablet()->tablet_id();
  boost::lock_guard<rw_spinlock> lock(lock_);
  if (!InsertIfNotPresent(&tablet_map_, id, tablet_peer)) {
    LOG(FATAL) << "Unable to register tablet peer " << id << ": already registered!";
  }

  MarkDirtyUnlocked(id);

  LOG(INFO) << "Registered tablet " << id;
}

bool TSTabletManager::LookupTablet(const string& tablet_id,
                                   std::tr1::shared_ptr<TabletPeer>* tablet_peer) const {
  boost::shared_lock<rw_spinlock> lock(lock_);
  const std::tr1::shared_ptr<TabletPeer>* found = FindOrNull(tablet_map_, tablet_id);
  if (!found) {
    return false;
  }
  *tablet_peer = *found;
  return true;
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
    if (ContainsKey(tablet_map_, tablet_id)) {
      report->add_updated_tablets()->set_tablet_id(tablet_id);
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
    const string& tablet_id = entry.first;
    report->add_updated_tablets()->set_tablet_id(tablet_id);
  }
  dirty_tablets_.clear();
}

} // namespace tserver
} // namespace kudu
