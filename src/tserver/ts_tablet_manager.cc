// Copyright (c) 2013, Cloudera, inc.

#include "tserver/ts_tablet_manager.h"

#include <glog/logging.h>
#include <string>
#include <tr1/memory>

#include "server/fsmanager.h"
#include "tablet/tablet_peer.h"

using std::tr1::shared_ptr;
using kudu::tablet::TabletPeer;

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
  if (tablet_peer_) {
    WARN_NOT_OK(tablet_peer_->Shutdown(), "Unable to close tablet");
  }
}

void TSTabletManager::RegisterTablet(const std::tr1::shared_ptr<TabletPeer>& tablet_peer) {
  CHECK(!tablet_peer_) << "Already have a tablet. Currently only supports one tablet per server";
  // TODO: will eventually need a mutex here when tablets get added/removed at
  // runtime.
  tablet_peer_ = tablet_peer;
}

bool TSTabletManager::LookupTablet(const string& tablet_id,
                                   std::tr1::shared_ptr<TabletPeer> *tablet_peer) const {
  // TODO: when the tablet server hosts multiple tablets,
  // lookup the correct one.
  // TODO: will eventually need a mutex here when tablets get added/removed at
  // runtime.
  *tablet_peer = tablet_peer_;
  return true;
}

} // namespace tserver
} // namespace kudu
