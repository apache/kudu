// Copyright (c) 2013, Cloudera, inc.

#include "kudu/master/master.h"

#include <boost/foreach.hpp>
#include <list>
#include <vector>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/service_if.h"
#include "kudu/rpc/service_pool.h"
#include "kudu/server/rpc_server.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master_service.h"
#include "kudu/master/master-path-handlers.h"
#include "kudu/master/sys_tables.h"
#include "kudu/master/ts_manager.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"

using std::vector;
using kudu::rpc::ServiceIf;

namespace kudu {
namespace master {

Master::Master(const MasterOptions& opts)
  : ServerBase(opts, "kudu.master"),
    state_(kStopped),
    ts_manager_(new TSManager()),
    catalog_manager_(new CatalogManager(this)),
    path_handlers_(new MasterPathHandlers(this)) {
}

Master::~Master() {
  CHECK_NE(kRunning, state_);
}

string Master::ToString() const {
  if (state_ != kRunning) {
    return "Master (stopped)";
  }
  return strings::Substitute("Master@$0", first_rpc_address().ToString());
}

Status Master::Init() {
  CHECK_EQ(kStopped, state_);

  RETURN_NOT_OK(ServerBase::Init());

  RETURN_NOT_OK(path_handlers_->Register(web_server_.get()));

  RETURN_NOT_OK(catalog_manager_->Init(is_first_run_));

  state_ = kInitialized;
  return Status::OK();
}

Status Master::Start() {
  CHECK_EQ(kInitialized, state_);

  gscoped_ptr<ServiceIf> impl(new MasterServiceImpl(this));
  RETURN_NOT_OK(ServerBase::Start(impl.Pass()));

  state_ = kRunning;
  return Status::OK();
}

void Master::Shutdown() {
  if (state_ == kRunning) {
    string name = ToString();
    LOG(INFO) << name << " shutting down...";
    ServerBase::Shutdown();
    catalog_manager_->Shutdown();
    LOG(INFO) << name << " shutdown complete.";
  }
  state_ = kStopped;
}

} // namespace master
} // namespace kudu
