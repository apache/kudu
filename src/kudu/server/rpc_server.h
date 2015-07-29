// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_RPC_SERVER_H
#define KUDU_RPC_SERVER_H

#include <string>
#include <tr1/memory>
#include <vector>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/service_pool.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"

namespace kudu {

namespace rpc {
class AcceptorPool;
class Messenger;
class ServiceIf;
} // namespace rpc

struct RpcServerOptions {
  RpcServerOptions();

  std::string rpc_bind_addresses;
  uint32_t num_acceptors_per_address;
  uint32_t num_service_threads;
  uint16_t default_port;
  size_t service_queue_length;
};

class RpcServer {
 public:
  explicit RpcServer(const RpcServerOptions& opts);
  ~RpcServer();

  Status Init(const std::tr1::shared_ptr<rpc::Messenger>& messenger);
  // Services need to be registered after Init'ing, but before Start'ing.
  // The service's ownership will be given to a ServicePool.
  Status RegisterService(gscoped_ptr<rpc::ServiceIf> service);
  Status Bind();
  Status Start();
  void Shutdown();

  std::string ToString() const;

  // Return the addresses that this server has successfully
  // bound to. Requires that the server has been Start()ed.
  Status GetBoundAddresses(std::vector<Sockaddr>* addresses) const WARN_UNUSED_RESULT;

  const rpc::ServicePool* service_pool(const std::string& service_name) const;

 private:
  enum ServerState {
    // Default state when the rpc server is constructed.
    UNINITIALIZED,
    // State after Init() was called.
    INITIALIZED,
    // State after Bind().
    BOUND,
    // State after Start() was called.
    STARTED
  };
  ServerState server_state_;

  const RpcServerOptions options_;
  std::tr1::shared_ptr<rpc::Messenger> messenger_;

  // Parsed addresses to bind RPC to. Set by Init()
  std::vector<Sockaddr> rpc_bind_addresses_;

  std::vector<std::tr1::shared_ptr<rpc::AcceptorPool> > acceptor_pools_;

  DISALLOW_COPY_AND_ASSIGN(RpcServer);
};

} // namespace kudu

#endif
