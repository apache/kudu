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
#include "master/catalog_manager.h"
#include "master/master_service.h"
#include "master/master-path-handlers.h"
#include "master/sys_tables.h"
#include "master/ts_manager.h"
#include "util/net/net_util.h"
#include "util/net/sockaddr.h"
#include "util/status.h"

using std::vector;
using kudu::rpc::ServiceIf;

namespace kudu {
namespace master {

Master::Master(const MasterOptions& opts)
  : ServerBase(opts.env, opts.base_dir, opts.rpc_opts, opts.webserver_opts, "kudu.master"),
    initted_(false),
    ts_manager_(new TSManager()),
    catalog_manager_(new CatalogManager(this)),
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

  RETURN_NOT_OK(catalog_manager_->Init(is_first_run_));

  initted_ = true;
  return Status::OK();
}

Status Master::Start() {
  CHECK(initted_);

  gscoped_ptr<ServiceIf> impl(new MasterServiceImpl(this));
  RETURN_NOT_OK(ServerBase::Start(impl.Pass()));

  return Status::OK();
}

void Master::Shutdown() {
  string name = ToString();
  LOG(INFO) << name << " shutting down...";
  catalog_manager_->Shutdown();
  ServerBase::Shutdown();
  LOG(INFO) << name << " shutdown complete.";
}

} // namespace master
} // namespace kudu
