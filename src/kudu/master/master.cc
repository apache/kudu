// Copyright (c) 2013, Cloudera, inc.

#include "kudu/master/master.h"

#include <boost/bind.hpp>
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
#include "kudu/tserver/tablet_service.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/task_executor.h"
#include "kudu/util/status.h"

using std::vector;
using kudu::rpc::ServiceIf;
using kudu::tserver::ConsensusServiceImpl;

namespace kudu {
namespace master {

Master::Master(const MasterOptions& opts)
  : ServerBase(opts, "kudu.master"),
    state_(kStopped),
    ts_manager_(new TSManager()),
    catalog_manager_(new CatalogManager(this)),
    path_handlers_(new MasterPathHandlers(this)),
    opts_(opts) {
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

  RETURN_NOT_OK(TaskExecutorBuilder("init").set_max_threads(1).Build(&init_executor_));

  RETURN_NOT_OK(ServerBase::Init());

  RETURN_NOT_OK(path_handlers_->Register(web_server_.get()));

  state_ = kInitialized;
  return Status::OK();
}

Status Master::Start() {
  RETURN_NOT_OK(StartAsync());
  return WaitForCatalogManagerInit();
}

Status Master::StartAsync() {
  CHECK_EQ(kInitialized, state_);

  gscoped_ptr<ServiceIf> impl(new MasterServiceImpl(this));
  gscoped_ptr<ServiceIf> consensus_service(new ConsensusServiceImpl(metric_context(),
                                                                    catalog_manager_.get()));

  RETURN_NOT_OK(ServerBase::RegisterService(impl.Pass()));
  RETURN_NOT_OK(ServerBase::RegisterService(consensus_service.Pass()));
  RETURN_NOT_OK(ServerBase::Start());

  // Start initializing the catalog manager.
  RETURN_NOT_OK(init_executor_->Submit(boost::bind(&Master::InitCatalogManager, this),
                                       &init_future_));

  state_ = kRunning;

  return Status::OK();
}

Status Master::InitCatalogManager() {
  if (catalog_manager_->IsInitialized()) {
    return Status::IllegalState("catalog manager is already initialized");
  }
  RETURN_NOT_OK_PREPEND(catalog_manager_->Init(is_first_run_),
                        "unable to initialize catalog manager");
  return Status::OK();
}

Status Master::WaitForCatalogManagerInit() {
  CHECK_EQ(state_, kRunning);

  init_future_->Wait();
  return init_future_->status();
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
