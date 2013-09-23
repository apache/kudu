// Copyright (c) 2013, Cloudera, inc.

#include "tserver/tablet_server.h"

#include <boost/foreach.hpp>
#include <list>
#include <vector>

#include "rpc/service_if.h"
#include "server/rpc_server.h"
#include "server/webserver.h"
#include "tablet/tablet_peer.h"
#include "tserver/scanners.h"
#include "tserver/tablet_service.h"
#include "util/net/net_util.h"
#include "util/net/sockaddr.h"
#include "util/status.h"

using std::tr1::shared_ptr;
using std::vector;
using kudu::rpc::ServiceIf;
using kudu::metadata::TabletServerPB;
using kudu::tablet::TabletPeer;

namespace kudu {
namespace tserver {

TabletServer::TabletServer(const TabletServerOptions& opts)
  : ServerBase(opts.rpc_opts, opts.webserver_opts),
    initted_(false),
    opts_(opts),
    fs_manager_(new FsManager(opts.env, opts.base_dir)) {
}

TabletServer::~TabletServer() {
  Shutdown();
}

string TabletServer::ToString() const {
  // TODO: include port numbers, etc.
  return "TabletServer";
}

Status TabletServer::Init() {
  CHECK(!initted_);

  RETURN_NOT_OK(fs_manager_->CreateInitialFileSystemLayout());

  RETURN_NOT_OK(ServerBase::Init());

  scanner_manager_.reset(new ScannerManager);

  // TODO replace this with a 'real' address for dist execution.
  tablet_server_pb_.set_hostname("TODO");
  tablet_server_pb_.set_port(0);

  initted_ = true;
  return Status::OK();
}

Status TabletServer::Start() {
  CHECK(initted_);

  RETURN_NOT_OK(ServerBase::Start(gscoped_ptr<ServiceIf>(new TabletServiceImpl(this))));
  return Status::OK();
}

Status TabletServer::Shutdown() {
  CHECK(initted_);
  LOG(INFO) << "TabletServer shutting down...";
  RETURN_NOT_OK(ServerBase::Shutdown());
  RETURN_NOT_OK(tablet_peer_->Shutdown());
  LOG(INFO) << "TabletServer shut down complete. Bye!";
  return Status::OK();
}

void TabletServer::RegisterTablet(const std::tr1::shared_ptr<TabletPeer>& tablet_peer) {
  CHECK(!tablet_peer_) << "Already have a tablet. Currently only supports one tablet per server";
  // TODO: will eventually need a mutex here when tablets get added/removed at
  // runtime.
  tablet_peer_ = tablet_peer;
}

bool TabletServer::LookupTablet(const string& tablet_id,
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
