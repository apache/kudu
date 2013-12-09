// Copyright (c) 2013, Cloudera, inc.

#include "tserver/tablet_server.h"

#include <boost/foreach.hpp>
#include <list>
#include <vector>

#include "gutil/strings/substitute.h"
#include "rpc/service_if.h"
#include "server/fsmanager.h"
#include "server/rpc_server.h"
#include "server/webserver.h"
#include "tserver/heartbeater.h"
#include "tserver/scanners.h"
#include "tserver/tablet_service.h"
#include "tserver/ts_tablet_manager.h"
#include "tserver/tserver-path-handlers.h"
#include "util/net/net_util.h"
#include "util/net/sockaddr.h"
#include "util/status.h"

using std::tr1::shared_ptr;
using std::vector;
using kudu::rpc::ServiceIf;
using kudu::tablet::TabletPeer;

namespace kudu {
namespace tserver {

TabletServer::TabletServer(const TabletServerOptions& opts)
  : ServerBase(opts.env, opts.base_dir, opts.rpc_opts, opts.webserver_opts, "kudu.tabletserver"),
    initted_(false),
    opts_(opts),
    tablet_manager_(new TSTabletManager(fs_manager_.get(), metric_context())),
    scanner_manager_(new ScannerManager()),
    path_handlers_(new TabletServerPathHandlers(this)) {
}

TabletServer::~TabletServer() {
  Shutdown();
}

string TabletServer::ToString() const {
  // TODO: include port numbers, etc.
  return "TabletServer";
}

Status TabletServer::ValidateMasterAddressResolution() const {
  Status s = opts_.master_hostport.ResolveAddresses(NULL);
  if (!s.ok()) {
    return s.CloneAndPrepend(strings::Substitute(
                               "Couldn't resolve master service address '$0'",
                               opts_.master_hostport.ToString()));
  }
  return s;
}

Status TabletServer::Init() {
  CHECK(!initted_);

  // Validate that the passed master address actually resolves.
  // We don't validate that we can connect at this point -- it should
  // be allowed to start the TS and the master in whichever order --
  // our heartbeat thread will loop until successfully connecting.
  RETURN_NOT_OK(ValidateMasterAddressResolution());

  RETURN_NOT_OK(ServerBase::Init());
  RETURN_NOT_OK(path_handlers_->Register(web_server_.get()));

  RETURN_NOT_OK_PREPEND(tablet_manager_->Init(),
                        "Could not init Tablet Manager");

  heartbeater_.reset(new Heartbeater(opts_, this));

  initted_ = true;
  return Status::OK();
}

Status TabletServer::Start() {
  CHECK(initted_);

  RETURN_NOT_OK(ServerBase::Start(gscoped_ptr<ServiceIf>(new TabletServiceImpl(this))));
  RETURN_NOT_OK(heartbeater_->Start());
  return Status::OK();
}

Status TabletServer::Shutdown() {
  LOG(INFO) << "TabletServer shutting down...";

  if (initted_) {
    WARN_NOT_OK(heartbeater_->Stop(), "Failed to stop TS Heartbeat thread");
    WARN_NOT_OK(ServerBase::Shutdown(), "Failed to shutdown server base components");
    tablet_manager_->Shutdown();
  }

  LOG(INFO) << "TabletServer shut down complete. Bye!";

  return Status::OK();
}

} // namespace tserver
} // namespace kudu
