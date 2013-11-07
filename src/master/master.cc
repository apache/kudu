// Copyright (c) 2013, Cloudera, inc.

#include "master/master.h"

#include <boost/foreach.hpp>
#include <list>
#include <vector>

#include "gutil/strings/substitute.h"
#include "rpc/messenger.h"
#include "rpc/service_if.h"
#include "rpc/service_pool.h"
#include "server/rpc_server.h"
#include "master/master_service.h"
#include "master/master-path-handlers.h"
#include "master/m_tablet_manager.h"
#include "master/ts_manager.h"
#include "util/net/net_util.h"
#include "util/net/sockaddr.h"
#include "util/status.h"

using std::vector;
using kudu::rpc::ServiceIf;

namespace kudu {
namespace master {

Master::Master(const MasterOptions& opts)
  : ServerBase(opts.env, opts.base_dir, opts.rpc_opts, opts.webserver_opts),
    initted_(false),
    ts_manager_(new TSManager()),
    tablet_manager_(new MTabletManager()),
    path_handlers_(new MasterPathHandlers(this)) {
}

Master::~Master() {
}

string Master::ToString() const {
  if (!initted_) {
    return "Uninitialized master";
  }
  return strings::Substitute("Master@$0", first_rpc_address().ToString());
}

Status Master::Init() {
  CHECK(!initted_);
  RETURN_NOT_OK(ServerBase::Init());

  RETURN_NOT_OK(path_handlers_->Register(web_server_.get()));

  initted_ = true;
  return Status::OK();
}

Status Master::Start() {
  CHECK(initted_);

  gscoped_ptr<ServiceIf> impl(new MasterServiceImpl(this));
  RETURN_NOT_OK(ServerBase::Start(impl.Pass()));

  return Status::OK();
}

Status Master::Shutdown() {
  string name = ToString();
  LOG(INFO) << name << " shutting down...";
  WARN_NOT_OK(ServerBase::Shutdown(),
              "Unable to shutdown base server components");
  LOG(INFO) << name << " shutdown complete.";
  return Status::OK();
}

} // namespace master
} // namespace kudu
