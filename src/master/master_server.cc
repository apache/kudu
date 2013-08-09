// Copyright (c) 2013, Cloudera, inc.

#include "master/master_server.h"

#include <boost/foreach.hpp>
#include <list>
#include <vector>

#include "rpc/messenger.h"
#include "rpc/service_if.h"
#include "rpc/service_pool.h"
#include "master/master_service.h"
#include "util/net/net_util.h"
#include "util/net/sockaddr.h"
#include "util/status.h"

using std::vector;
using kudu::rpc::ServiceIf;

namespace kudu {
namespace master {

MasterServerOptions::MasterServerOptions()
  : rpc_bind_addresses("0.0.0.0:7150"),
    num_rpc_reactors(1),
    num_acceptors_per_address(1),
    num_service_threads(10) {
}

MasterServer::MasterServer(const MasterServerOptions& opts)
  : options_(opts),
    initted_(false) {
}

MasterServer::~MasterServer() {
  if (rpc_messenger_) {
    rpc_messenger_->Shutdown();
  }
}

string MasterServer::ToString() const {
  // TODO: include port numbers, etc.
  return "MasterServer";
}

Status MasterServer::Init() {
  CHECK(!initted_);
  RETURN_NOT_OK(ParseAddressList(options_.rpc_bind_addresses, kDefaultPort,
                                 &rpc_bind_addresses_));
  BOOST_FOREACH(const Sockaddr& addr, rpc_bind_addresses_) {
    if (IsPrivilegedPort(addr.port())) {
      LOG(WARNING) << "May be unable to bind to privileged port for address "
                   << addr.ToString();
    }
  }

  initted_ = true;
  return Status::OK();
}

Status MasterServer::Start() {
  CHECK(initted_);
  RETURN_NOT_OK(StartRpcServer());
  return Status::OK();
}

Status MasterServer::StartRpcServer() {
  CHECK(initted_);
  CHECK(!rpc_messenger_);

  // Create the Messenger.
  rpc::MessengerBuilder builder(ToString());
  builder.set_num_reactors(options_.num_rpc_reactors);
  RETURN_NOT_OK(builder.Build(&rpc_messenger_));

  // Create the AcceptorPool for each bind address.
  BOOST_FOREACH(const Sockaddr& bind_addr, rpc_bind_addresses_) {
    RETURN_NOT_OK(rpc_messenger_->AddAcceptorPool(
                    bind_addr, options_.num_acceptors_per_address));
  }

  // Create the Service pool
  gscoped_ptr<ServiceIf> impl(new MasterServiceImpl(this));
  gscoped_ptr<rpc::ServicePool> pool(new rpc::ServicePool(rpc_messenger_, impl.Pass()));
  RETURN_NOT_OK(pool->Init(options_.num_service_threads));

  rpc_service_pool_.swap(pool);

  return Status::OK();
}

void MasterServer::GetBoundAddresses(vector<Sockaddr>* addresses) {
  using rpc::AcceptorPoolInfo;

  CHECK(initted_);
  std::list<AcceptorPoolInfo> acceptors;
  rpc_messenger_->GetAcceptorInfo(&acceptors);
  BOOST_FOREACH(const AcceptorPoolInfo& info, acceptors) {
    addresses->push_back(info.bind_address());
  }
}

} // namespace master
} // namespace kudu
