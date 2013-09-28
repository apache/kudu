// Copyright (c) 2013, Cloudera, inc.

#include "tserver/tablet_server.h"

#include <boost/foreach.hpp>
#include <list>
#include <vector>

#include "rpc/service_if.h"
#include "server/rpc_server.h"
#include "tserver/scanners.h"
#include "tserver/tablet_service.h"
#include "util/net/net_util.h"
#include "util/net/sockaddr.h"
#include "util/status.h"

using std::tr1::shared_ptr;
using std::vector;
using kudu::rpc::ServiceIf;
using kudu::tablet::Tablet;

namespace kudu {
namespace tserver {

TabletServer::TabletServer(const TabletServerOptions& opts)
  : ServerBase(opts.rpc_opts, opts.webserver_opts),
    initted_(false),
    opts_(opts),
    fs_manager_(new FsManager(opts.env, opts.base_dir)) {
}

TabletServer::~TabletServer() {
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

  initted_ = true;
  return Status::OK();
}

Status TabletServer::Start() {
  CHECK(initted_);

  ServerBase::Start(gscoped_ptr<ServiceIf>(new TabletServiceImpl(this)));
  return Status::OK();
}

void TabletServer::RegisterTablet(const shared_ptr<Tablet>& tablet) {
  CHECK(!tablet_) << "Already have a tablet. Currently only supports one tablet per server";
  // TODO: will eventually need a mutex here when tablets get added/removed at
  // runtime.
  tablet_ = tablet;
}

bool TabletServer::LookupTablet(const string& tablet_id,
                                shared_ptr<Tablet>* tablet) const {
  // TODO: when the tablet server hosts multiple tablets,
  // lookup the correct one.
  // TODO: will eventually need a mutex here when tablets get added/removed at
  // runtime.
  *tablet = tablet_;
  return true;
}

} // namespace tserver
} // namespace kudu
