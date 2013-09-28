// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <list>
#include <string>
#include <vector>

#include "rpc/acceptor_pool.h"
#include "rpc/messenger.h"
#include "rpc/service_if.h"
#include "rpc/service_pool.h"
#include "server/rpc_server.h"
#include "util/net/net_util.h"
#include "util/net/sockaddr.h"
#include "util/status.h"

using std::string;
using std::tr1::shared_ptr;
using std::vector;
using kudu::rpc::AcceptorPool;
using kudu::rpc::Messenger;
using kudu::rpc::ServiceIf;

namespace kudu {

RpcServerOptions::RpcServerOptions()
  : rpc_bind_addresses("0.0.0.0:0"),
    num_acceptors_per_address(1),
    num_service_threads(10),
    default_port(0) {
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

Status RpcServer::Init() {
  CHECK(!initted_);
  RETURN_NOT_OK(ParseAddressList(options_.rpc_bind_addresses,
                                 options_.default_port,
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

Status RpcServer::Start(const std::tr1::shared_ptr<Messenger>& messenger,
                        gscoped_ptr<rpc::ServiceIf> service) {
  CHECK(initted_);

  // Create the Acceptor pools (one per bind address)
  vector<shared_ptr<AcceptorPool> > new_acceptor_pools;
  // Create the AcceptorPool for each bind address.
  BOOST_FOREACH(const Sockaddr& bind_addr, rpc_bind_addresses_) {
    shared_ptr<rpc::AcceptorPool> pool;
    RETURN_NOT_OK(messenger->AddAcceptorPool(
                    bind_addr, options_.num_acceptors_per_address,
                    &pool));
    new_acceptor_pools.push_back(pool);
  }

  // Create the Service pool
  gscoped_ptr<rpc::ServicePool> pool(new rpc::ServicePool(messenger, service.Pass()));
  RETURN_NOT_OK(pool->Init(options_.num_service_threads));

  acceptor_pools_.swap(new_acceptor_pools);
  service_pool_.swap(pool);

  return Status::OK();
}

void RpcServer::Shutdown() {
  BOOST_FOREACH(const shared_ptr<AcceptorPool>& pool, acceptor_pools_) {
    pool->Shutdown();
  }
  acceptor_pools_.clear();

  // TODO: Does closing the service pool properly end up clearing the queue of anything
  // which might have been pending? Would be good to verify this.
  service_pool_.reset();
}

void RpcServer::GetBoundAddresses(vector<Sockaddr>* addresses) const {
  CHECK(initted_);
  BOOST_FOREACH(const shared_ptr<AcceptorPool>& pool, acceptor_pools_) {
    addresses->push_back(pool->bind_address());
  }
}

} // namespace kudu
