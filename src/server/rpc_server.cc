// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <list>
#include <vector>

#include "rpc/messenger.h"
#include "rpc/service_if.h"
#include "rpc/service_pool.h"
#include "server/rpc_server.h"
#include "util/net/net_util.h"
#include "util/net/sockaddr.h"
#include "util/status.h"

using std::vector;
using kudu::rpc::ServiceIf;

namespace kudu {

RpcServerOptions::RpcServerOptions()
  : rpc_bind_addresses("0.0.0.0:0"),
    num_rpc_reactors(1),
    num_acceptors_per_address(1),
    num_service_threads(10) {
}

RpcServer::RpcServer(const RpcServerOptions& opts)
  : options_(opts),
    initted_(false) {
}

RpcServer::~RpcServer() {
  Shutdown();
}

string RpcServer::ToString() const {
  // TODO: include port numbers, etc.
  return "RpcServer";
}

Status RpcServer::Init(uint16_t default_port) {
  CHECK(!initted_);
  RETURN_NOT_OK(ParseAddressList(options_.rpc_bind_addresses, default_port,
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

Status RpcServer::Start(gscoped_ptr<rpc::ServiceIf> service) {
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
  gscoped_ptr<rpc::ServicePool> pool(new rpc::ServicePool(rpc_messenger_, service.Pass()));
  RETURN_NOT_OK(pool->Init(options_.num_service_threads));

  rpc_service_pool_.swap(pool);

  return Status::OK();
}

void RpcServer::Shutdown() {
  if (rpc_messenger_) {
    rpc_messenger_->Shutdown();
    rpc_messenger_.reset();
  }
}

void RpcServer::GetBoundAddresses(vector<Sockaddr>* addresses) const {
  using rpc::AcceptorPoolInfo;

  CHECK(initted_);
  std::list<AcceptorPoolInfo> acceptors;
  rpc_messenger_->GetAcceptorInfo(&acceptors);
  BOOST_FOREACH(const AcceptorPoolInfo& info, acceptors) {
    addresses->push_back(info.bind_address());
  }
}

} // namespace kudu
