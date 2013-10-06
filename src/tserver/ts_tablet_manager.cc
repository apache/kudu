// Copyright (c) 2013, Cloudera, inc.

#include "tserver/ts_tablet_manager.h"

#include <boost/thread/mutex.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <glog/logging.h>
#include <string>
#include <tr1/memory>

#include "server/fsmanager.h"
#include "server/metadata.pb.h"
#include "tablet/tablet_peer.h"

using std::tr1::shared_ptr;
using kudu::tablet::TabletPeer;
using kudu::metadata::TabletMasterBlockPB;

namespace kudu {
namespace tserver {

TSTabletManager::TSTabletManager(FsManager* fs_manager)
  : fs_manager_(fs_manager) {
}

TSTabletManager::~TSTabletManager() {
}

Status TSTabletManager::Init() {
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

void TSTabletManager::RegisterTablet(const std::tr1::shared_ptr<TabletPeer>& tablet_peer) {
  const string& id = tablet_peer->tablet()->tablet_id();
  boost::lock_guard<rw_spinlock> lock(lock_);
  if (!InsertIfNotPresent(&tablet_map_, id, tablet_peer)) {
    LOG(FATAL) << "Unable to register tablet peer " << id << ": already registered!";
  }
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

} // namespace tserver
} // namespace kudu
