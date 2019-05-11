// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#ifndef KUDU_SERVER_SERVER_BASE_H
#define KUDU_SERVER_SERVER_BASE_H

#include <cstdint>
#include <memory>
#include <string>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/messenger.h"
#include "kudu/security/simple_acl.h"
#include "kudu/server/server_base_options.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/status.h"

namespace kudu {

class FsManager;
class MemTracker;
class MetricEntity;
class MetricRegistry;
#ifdef FB_DO_NOT_REMOVE
class MinidumpExceptionHandler;
#endif
class NodeInstancePB;
class RpcServer;
class ScopedGLogMetrics;
class Sockaddr;
class Thread;
#ifdef FB_DO_NOT_REMOVE
class Webserver;
#endif

namespace clock {
class Clock;
} // namespace clock

namespace rpc {
class ResultTracker;
class RpcContext;
class ServiceIf;
class ServicePool;
} // namespace rpc

namespace security {
class TlsContext;
class TokenVerifier;
} // namespace security

namespace server {
class DiagnosticsLog;
class ServerStatusPB;

// Base class for tablet server and master.
// Handles starting and stopping the RPC server and web server,
// and provides a common interface for server-type-agnostic functions.
class ServerBase {
 public:
  const RpcServer *rpc_server() const { return rpc_server_.get(); }

#ifdef FB_DO_NOT_REMOVE
  const Webserver *web_server() const { return web_server_.get(); }
#endif
  const std::shared_ptr<rpc::Messenger>& messenger() const { return messenger_; }

  // Return the first RPC address that this server has bound to.
  // FATALs if the server is not started.
  Sockaddr first_rpc_address() const;

#ifdef FB_DO_NOT_REMOVE
  // Return the first HTTP address that this server has bound to.
  // FATALs if the server is not started.
  Sockaddr first_http_address() const;
#endif

  FsManager* fs_manager() { return fs_manager_.get(); }

  const security::TlsContext& tls_context() const { return messenger_->tls_context(); }
  security::TlsContext* mutable_tls_context() { return messenger_->mutable_tls_context(); }

  const security::TokenVerifier& token_verifier() const { return messenger_->token_verifier(); }
  security::TokenVerifier* mutable_token_verifier() { return messenger_->mutable_token_verifier(); }

  // Return the instance identifier of this server.
  // This may not be called until after the server is Started.
  const NodeInstancePB& instance_pb() const;

  const std::shared_ptr<MemTracker>& mem_tracker() const { return mem_tracker_; }

  const scoped_refptr<MetricEntity>& metric_entity() const { return metric_entity_; }

  MetricRegistry* metric_registry() { return metric_registry_.get(); }

  const scoped_refptr<rpc::ResultTracker>& result_tracker() const { return result_tracker_; }

  // Returns this server's clock.
  clock::Clock* clock() { return clock_.get(); }

  // Return a PB describing the status of the server (version info, bound ports, etc)
  Status GetStatusPB(ServerStatusPB* status) const;

  enum {
    SUPER_USER = 1,
    USER = 1 << 1,
    SERVICE_USER = 1 << 2
  };

  // Authorize an RPC. 'allowed_roles' is a bitset of which roles from the above
  // enum should be allowed to make hthe RPC.
  //
  // If authorization fails, return false and respond to the RPC.
  bool Authorize(rpc::RpcContext* rpc, uint32_t allowed_roles);

 protected:
  ServerBase(std::string name, const ServerBaseOptions& options,
             const std::string& metric_namespace);
  virtual ~ServerBase();

  virtual Status Init();

  // Starts the server, including activating its RPC and HTTP endpoints such
  // that incoming requests get processed.
  virtual Status Start();

  // Shuts down the server.
  virtual void Shutdown();

  // Registers a new RPC service. Once Start() is called, the server will
  // process and dispatch incoming RPCs belonging to this service.
  Status RegisterService(gscoped_ptr<rpc::ServiceIf> rpc_impl);

  // Unregisters all RPC services. After this function returns, any subsequent
  // incoming RPCs will be rejected.
  //
  // When shutting down, this function should be called before shutting down
  // higher-level subsystems. For example:
  // 1. ServerBase::UnregisterAllServices()
  // 2. <shut down other subsystems>
  // 3. ServerBase::Shutdown()
  //
  // TODO(adar): this should also wait on all outstanding RPCs to finish via
  // Messenger::Shutdown, but doing that causes too many other shutdown-related
  // issues. Here are a few that I observed:
  // - tserver heartbeater threads access acceptor pool socket state.
  // - Shutting down TabletReplicas invokes RPC callbacks for aborted
  //   transactions, but Messenger::Shutdown has already destroyed too much
  //   necessary RPC state.
  //
  // TODO(adar): this should also shutdown the webserver, but it isn't safe to
  // do that before before shutting down the tserver heartbeater.
  void UnregisterAllServices();

  void LogUnauthorizedAccess(rpc::RpcContext* rpc) const;

  const std::string name_;

#ifdef FB_DO_NOT_REMOVE
  std::unique_ptr<MinidumpExceptionHandler> minidump_handler_;
#endif

  std::shared_ptr<MemTracker> mem_tracker_;
  gscoped_ptr<MetricRegistry> metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
  gscoped_ptr<FsManager> fs_manager_;
  gscoped_ptr<RpcServer> rpc_server_;

#ifdef FB_DO_NOT_REMOVE
  gscoped_ptr<Webserver> web_server_;
#endif

  std::shared_ptr<rpc::Messenger> messenger_;
  scoped_refptr<rpc::ResultTracker> result_tracker_;
  bool is_first_run_;

  scoped_refptr<clock::Clock> clock_;

  // The instance identifier of this server.
  gscoped_ptr<NodeInstancePB> instance_pb_;

  // The ACL of users who are allowed to act as superusers.
  security::SimpleAcl superuser_acl_;

  // The ACL of users who are allowed to access the cluster.
  security::SimpleAcl user_acl_;

  // The ACL of users who may act as part of the Kudu service.
  security::SimpleAcl service_acl_;
 private:
  Status InitAcls();
  void GenerateInstanceID();
  Status DumpServerInfo(const std::string& path,
                        const std::string& format) const;
  Status StartMetricsLogging();
  void MetricsLoggingThread();

#ifdef FB_DO_NOT_REMOVE
  std::string FooterHtml() const;
#endif

  // Callback from the RPC system when a service queue has overflowed.
  void ServiceQueueOverflowed(rpc::ServicePool* service);

#ifdef FB_DO_NOT_REMOVE
  // Start thread to remove excess glog and minidump files.
  Status StartExcessLogFileDeleterThread();
  void ExcessLogFileDeleterThread();
#endif

  ServerBaseOptions options_;

#ifdef FB_DO_NOT_REMOVE
  std::unique_ptr<DiagnosticsLog> diag_log_;
  scoped_refptr<Thread> excess_log_deleter_thread_;
#endif
  CountDownLatch stop_background_threads_latch_;

#ifdef FB_DO_NOT_REMOVE
  gscoped_ptr<ScopedGLogMetrics> glog_metrics_;
#endif

  DISALLOW_COPY_AND_ASSIGN(ServerBase);
};

} // namespace server
} // namespace kudu
#endif /* KUDU_SERVER_SERVER_BASE_H */
