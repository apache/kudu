// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/foreach.hpp>
#include <list>
#include <string>
#include <vector>

#include <gflags/gflags.h>

#include "kudu/gutil/casts.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/acceptor_pool.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/service_if.h"
#include "kudu/rpc/service_pool.h"
#include "kudu/server/rpc_server.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"

using std::string;
using std::tr1::shared_ptr;
using std::vector;
using kudu::rpc::AcceptorPool;
using kudu::rpc::Messenger;
using kudu::rpc::ServiceIf;
using strings::Substitute;

DEFINE_int32(rpc_service_queue_length, 50,
             "Default length of queue for incoming RPC requests");

namespace kudu {

RpcServerOptions::RpcServerOptions()
  : rpc_bind_addresses("0.0.0.0:0"),
    num_acceptors_per_address(1),
    num_service_threads(10),
    default_port(0),
    service_queue_length(FLAGS_rpc_service_queue_length) {
}

RpcServer::RpcServer(const RpcServerOptions& opts)
  : server_state_(UNINITIALIZED),
    options_(opts) {
}

RpcServer::~RpcServer() {
  Shutdown();
}

string RpcServer::ToString() const {
  // TODO: include port numbers, etc.
  return "RpcServer";
}

Status RpcServer::Init(const std::tr1::shared_ptr<Messenger>& messenger) {
  CHECK_EQ(server_state_, UNINITIALIZED);
  messenger_ = messenger;

  RETURN_NOT_OK(ParseAddressList(options_.rpc_bind_addresses,
                                 options_.default_port,
                                 &rpc_bind_addresses_));
  BOOST_FOREACH(const Sockaddr& addr, rpc_bind_addresses_) {
    if (IsPrivilegedPort(addr.port())) {
      LOG(WARNING) << "May be unable to bind to privileged port for address "
                   << addr.ToString();
    }
  }

  server_state_ = INITIALIZED;
  return Status::OK();
}

Status RpcServer::RegisterService(gscoped_ptr<rpc::ServiceIf> service) {
  CHECK_EQ(server_state_, INITIALIZED);
  const MetricContext& metric_ctx = *messenger_->metric_context();
  string service_name = service->service_name();
  scoped_refptr<rpc::ServicePool> service_pool =
    new rpc::ServicePool(service.Pass(), metric_ctx, options_.service_queue_length);
  RETURN_NOT_OK(service_pool->Init(options_.num_service_threads));
  RETURN_NOT_OK(messenger_->RegisterService(service_name, service_pool));
  return Status::OK();
}

Status RpcServer::Start() {
  CHECK_EQ(server_state_, INITIALIZED);

  // Create the Acceptor pools (one per bind address)
  vector<shared_ptr<AcceptorPool> > new_acceptor_pools;
  // Create the AcceptorPool for each bind address.
  BOOST_FOREACH(const Sockaddr& bind_addr, rpc_bind_addresses_) {
    shared_ptr<rpc::AcceptorPool> pool;
    RETURN_NOT_OK(messenger_->AddAcceptorPool(
                    bind_addr, options_.num_acceptors_per_address,
                    &pool));
    new_acceptor_pools.push_back(pool);
  }
  acceptor_pools_.swap(new_acceptor_pools);

  server_state_ = STARTED;
  return Status::OK();
}

void RpcServer::Shutdown() {
  BOOST_FOREACH(const shared_ptr<AcceptorPool>& pool, acceptor_pools_) {
    pool->Shutdown();
  }
  acceptor_pools_.clear();

  if (messenger_) {
    WARN_NOT_OK(messenger_->UnregisterAllServices(), "Unable to unregister our services");
  }
}

void RpcServer::GetBoundAddresses(vector<Sockaddr>* addresses) const {
  CHECK_EQ(server_state_, STARTED);
  BOOST_FOREACH(const shared_ptr<AcceptorPool>& pool, acceptor_pools_) {
    addresses->push_back(pool->bind_address());
  }
}

const rpc::ServicePool* RpcServer::service_pool(const string& service_name) const {
  return down_cast<rpc::ServicePool*>(messenger_->rpc_service(service_name).get());
}

} // namespace kudu
