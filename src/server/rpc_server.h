// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_RPC_SERVER_H
#define KUDU_RPC_SERVER_H

#include <string>
#include <tr1/memory>
#include <vector>

#include "util/net/sockaddr.h"
#include "util/status.h"

namespace kudu {

namespace rpc {
class AcceptorPool;
class Messenger;
class ServiceIf;
class ServicePool;
} // namespace rpc

struct RpcServerOptions {
  RpcServerOptions();

  string rpc_bind_addresses;
  uint32_t num_acceptors_per_address;
  uint32_t num_service_threads;
  uint16_t default_port;

  // TODO: add service queue length -- awkward to do right now since the
  // service queue is part of the messenger instead of the RpcServer.
  // This is do for some refactoring.
};

class RpcServer {
 public:
  explicit RpcServer(const RpcServerOptions& opts);
  ~RpcServer();

  Status Init();
  Status Start(const std::tr1::shared_ptr<rpc::Messenger>& messenger,
               gscoped_ptr<rpc::ServiceIf> service);
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

  std::vector<std::tr1::shared_ptr<rpc::AcceptorPool> > acceptor_pools_;
  gscoped_ptr<rpc::ServicePool> service_pool_;
  DISALLOW_COPY_AND_ASSIGN(RpcServer);
};

} // namespace kudu

#endif
