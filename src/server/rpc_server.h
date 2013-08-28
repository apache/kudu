// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_RPC_SERVER_H
#define KUDU_RPC_SERVER_H

#include <boost/foreach.hpp>
#include <string>
#include <vector>

#include "rpc/messenger.h"
#include "rpc/service_if.h"
#include "rpc/service_pool.h"
#include "util/net/net_util.h"
#include "util/net/sockaddr.h"
#include "util/status.h"

namespace kudu {

struct RpcServerOptions {
  RpcServerOptions();

  string rpc_bind_addresses;
  uint32_t num_rpc_reactors;
  uint32_t num_acceptors_per_address;
  uint32_t num_service_threads;
};

class RpcServer {
 public:
  explicit RpcServer(const RpcServerOptions& opts);
  ~RpcServer();

  Status Init(uint16_t default_port);
  Status Start(gscoped_ptr<rpc::ServiceIf> service);
  void Shutdown();

  string ToString() const;

  // Return the addresses that this server has successfully
  // bound to. Requires that the server has been Start()ed.
  void GetBoundAddresses(std::vector<Sockaddr>* addresses) const;

 private:
  const RpcServerOptions options_;
  bool initted_;

  // Parsed addresses to bind RPC to. Set by Init()
  std::vector<Sockaddr> rpc_bind_addresses_;

  // RPC messenger.
  std::tr1::shared_ptr<rpc::Messenger> rpc_messenger_;
  gscoped_ptr<rpc::ServicePool> rpc_service_pool_;

  DISALLOW_COPY_AND_ASSIGN(RpcServer);
};

} // namespace kudu

#endif
