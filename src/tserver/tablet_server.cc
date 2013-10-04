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
#include "tserver/ts_tablet_manager.h"
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
    fs_manager_(new FsManager(opts.env, opts.base_dir)),
    tablet_manager_(new TSTabletManager(fs_manager_.get())),
    scanner_manager_(new ScannerManager()) {
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

  RETURN_NOT_OK_PREPEND(fs_manager_->CreateInitialFileSystemLayout(),
                        "Could not init FS layout");
  RETURN_NOT_OK_PREPEND(tablet_manager_->Init(),
                        "Could not init Tablet Manager");

  RETURN_NOT_OK(ServerBase::Init());

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
  tablet_manager_->Shutdown();
  LOG(INFO) << "TabletServer shut down complete. Bye!";
  return Status::OK();
}

} // namespace tserver
} // namespace kudu
