// Copyright (c) 2013, Cloudera, inc.

#include "tserver/tablet_server.h"

#include <boost/foreach.hpp>
#include <list>
#include <vector>

#include "rpc/service_if.h"
#include "server/default-path-handlers.h"
#include "server/rpc_server.h"
#include "server/webserver.h"
#include "tserver/scanners.h"
#include "tserver/tablet_service.h"
#include "util/net/net_util.h"
#include "util/net/sockaddr.h"
#include "util/status.h"

using std::vector;
using kudu::rpc::ServiceIf;
using kudu::tablet::Tablet;

namespace kudu {
namespace tserver {

TabletServerOptions::TabletServerOptions()
  : webserver_port(TabletServer::kDefaultWebPort) {
}

TabletServer::TabletServer(const TabletServerOptions& opts)
  : initted_(false),
    rpc_server_(new RpcServer(opts.rpc_opts)),
    web_server_(new Webserver(opts.webserver_port)) {
}

TabletServer::~TabletServer() {
  web_server_->Stop();
  rpc_server_->Shutdown();
}

string TabletServer::ToString() const {
  // TODO: include port numbers, etc.
  return "TabletServer";
}

Status TabletServer::Init() {
  CHECK(!initted_);
  RETURN_NOT_OK(rpc_server_->Init(kDefaultPort));

  scanner_manager_.reset(new ScannerManager);

  initted_ = true;
  return Status::OK();
}

Status TabletServer::Start() {
  CHECK(initted_);

  gscoped_ptr<ServiceIf> impl(new TabletServiceImpl(this));
  RETURN_NOT_OK(rpc_server_->Start(impl.Pass()));

  AddDefaultPathHandlers(web_server_.get());
  RETURN_NOT_OK(web_server_->Start());
  return Status::OK();
}

void TabletServer::RegisterTablet(const std::tr1::shared_ptr<Tablet>& tablet) {
  CHECK(!tablet_) << "Already have a tablet. Currently only supports one tablet per server";
  // TODO: will eventually need a mutex here when tablets get added/removed at
  // runtime.
  tablet_ = tablet;
}

bool TabletServer::LookupTablet(const string& tablet_id,
                                std::tr1::shared_ptr<Tablet>* tablet) const {
  // TODO: when the tablet server hosts multiple tablets,
  // lookup the correct one.
  // TODO: will eventually need a mutex here when tablets get added/removed at
  // runtime.
  *tablet = tablet_;
  return true;
}

} // namespace tserver
} // namespace kudu
