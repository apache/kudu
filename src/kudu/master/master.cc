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
#include "kudu/master/master.proxy.h"
#include "kudu/master/master-path-handlers.h"
#include "kudu/master/sys_tables.h"
#include "kudu/master/ts_manager.h"
#include "kudu/tserver/tablet_service.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/task_executor.h"
#include "kudu/util/status.h"

DEFINE_int32(master_registration_rpc_timeout_ms, 1500,
             "Timeout for retrieving master registration over RPC.");

using std::vector;

using kudu::metadata::QuorumPeerPB;
using kudu::rpc::ServiceIf;
using kudu::tserver::ConsensusServiceImpl;
using strings::Substitute;

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
    return Status::IllegalState("Catalog manager is already initialized");
  }
  RETURN_NOT_OK_PREPEND(catalog_manager_->Init(is_first_run_),
                        "Unable to initialize catalog manager");
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

Status Master::GetMasterRegistration(MasterRegistrationPB* reg) const {
  vector<Sockaddr> rpc_addrs;
  rpc_server()->GetBoundAddresses(&rpc_addrs);
  BOOST_FOREACH(const Sockaddr& rpc_addr, rpc_addrs) {
    HostPortPB* rpc_host_port = reg->add_rpc_addresses();
    RETURN_NOT_OK(HostPortToPB(HostPort(rpc_addr), rpc_host_port));
  }
  vector<Sockaddr> http_addrs;
  web_server()->GetBoundAddresses(&http_addrs);
  BOOST_FOREACH(const Sockaddr& http_addr, http_addrs) {
    HostPortPB* http_host_port = reg->add_http_addresses();
    RETURN_NOT_OK(HostPortToPB(HostPort(http_addr), http_host_port));
  }
  return Status::OK();
}

namespace {

// TODO this method should be moved to a separate class (along with
// ListMasters), so that it can also be used in TS and client when
// bootstrapping.
Status GetMasterEntryForHost(const shared_ptr<rpc::Messenger>& messenger,
                             const HostPort& hostport,
                             ListMastersResponsePB::Entry* e) {
  Sockaddr sockaddr;
  RETURN_NOT_OK(SockaddrFromHostPort(hostport, &sockaddr));
  MasterServiceProxy proxy(messenger, sockaddr);
  GetMasterRegistrationRequestPB req;
  GetMasterRegistrationResponsePB resp;
  rpc::RpcController controller;
  controller.set_timeout(MonoDelta::FromMilliseconds(FLAGS_master_registration_rpc_timeout_ms));
  RETURN_NOT_OK(proxy.GetMasterRegistration(req, &resp, &controller));
  e->mutable_instance_id()->CopyFrom(resp.instance_id());
  if (resp.has_error()) {
    return StatusFromPB(resp.error());
  }
  e->mutable_registration()->CopyFrom(resp.registration());
  e->set_local(false);
  return Status::OK();
}

} // anonymous namespace

Status Master::ListMasters(std::vector<ListMastersResponsePB::Entry>* masters) const {
  ListMastersResponsePB::Entry local_entry;
  local_entry.set_local(true);
  local_entry.mutable_instance_id()->CopyFrom(catalog_manager_->NodeInstance());
  RETURN_NOT_OK(GetMasterRegistration(local_entry.mutable_registration()));

  if (opts_.IsDistributed()) {
    if (opts_.leader) {
      local_entry.set_role(QuorumPeerPB::LEADER);
    } else {
      local_entry.set_role(QuorumPeerPB::FOLLOWER);
      ListMastersResponsePB::Entry leader_entry;
      Status s = GetMasterEntryForHost(messenger_, opts_.leader_address, &leader_entry);
      if (!s.ok()) {
        s = s.CloneAndPrepend(Substitute("Unable to get registration information for leader ($0)",
                                         opts_.leader_address.ToString()));
        LOG(WARNING) << s.ToString();
        StatusToPB(s, leader_entry.mutable_error());
      } else {
        leader_entry.set_role(QuorumPeerPB::LEADER);
      }
      masters->push_back(leader_entry);
    }
    BOOST_FOREACH(const HostPort& follower_addr, opts_.follower_addresses) {
      ListMastersResponsePB::Entry follower_entry;
      Status s = GetMasterEntryForHost(messenger_, follower_addr, &follower_entry);
      if (!s.ok()) {
        s = s.CloneAndPrepend(
            Substitute("Unable to get registration information for follower ($0)",
                       follower_addr.ToString()));
        LOG(WARNING) << s.ToString();
        StatusToPB(s, follower_entry.mutable_error());
      } else {
        follower_entry.set_role(QuorumPeerPB::FOLLOWER);
      }
      masters->push_back(follower_entry);
    }
  }

  masters->push_back(local_entry);
  return Status::OK();
}

} // namespace master
} // namespace kudu
