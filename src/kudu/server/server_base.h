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
#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/messenger.h"
#include "kudu/security/simple_acl.h"
#include "kudu/server/server_base_options.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/status.h"

namespace kudu {

class DnsResolver;
class FileCache;
class FsManager;
class MemTracker;
class MetricEntity;
class MetricRegistry;
class MinidumpExceptionHandler;
class NodeInstancePB;
class RpcServer;
class ScopedGLogMetrics;
class Sockaddr;
class Thread;
class Webserver;

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
  const Webserver *web_server() const { return web_server_.get(); }
  const std::shared_ptr<rpc::Messenger>& messenger() const { return messenger_; }

  // Return the first RPC address that this server has bound to.
  // FATALs if the server is not started.
  Sockaddr first_rpc_address() const;

  // Return the first HTTP address that this server has bound to.
  // FATALs if the server is not started.
  Sockaddr first_http_address() const;

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

  MetricRegistry* metric_registry() const { return metric_registry_.get(); }

  const scoped_refptr<rpc::ResultTracker>& result_tracker() const { return result_tracker_; }

  clock::Clock* clock() const { return clock_.get(); }

  DnsResolver* dns_resolver() const { return dns_resolver_.get(); }

  FileCache* file_cache() const { return file_cache_.get(); }

  // Return a PB describing the status of the server (version info, bound ports, etc)
  Status GetStatusPB(ServerStatusPB* status) const;

  int64_t start_time() const {
    return start_time_;
  }

  enum {
    SUPER_USER = 1,
    USER = 1 << 1,
    SERVICE_USER = 1 << 2
  };

  // Returns whether or not the rpc is from a super-user.
  bool IsFromSuperUser(const rpc::RpcContext* rpc);

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
  virtual void Shutdown() {
    ShutdownImpl();
  }

  // Registers a new RPC service. Once Start() is called, the server will
  // process and dispatch incoming RPCs belonging to this service.
  Status RegisterService(std::unique_ptr<rpc::ServiceIf> rpc_impl);

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
  //   ops, but Messenger::Shutdown has already destroyed too much necessary
  //   RPC state.
  //
  // TODO(adar): this should also shutdown the webserver, but it isn't safe to
  // do that before before shutting down the tserver heartbeater.
  void UnregisterAllServices();

  void LogUnauthorizedAccess(rpc::RpcContext* rpc) const;

  const std::string name_;
  // Seconds since the epoch.
  int64_t start_time_;

  std::unique_ptr<MinidumpExceptionHandler> minidump_handler_;
  std::shared_ptr<MemTracker> mem_tracker_;
  std::unique_ptr<MetricRegistry> metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
  std::unique_ptr<FileCache> file_cache_;
  std::unique_ptr<FsManager> fs_manager_;
  std::unique_ptr<RpcServer> rpc_server_;
  std::unique_ptr<Webserver> web_server_;

  std::shared_ptr<rpc::Messenger> messenger_;
  scoped_refptr<rpc::ResultTracker> result_tracker_;
  bool is_first_run_;

  std::unique_ptr<clock::Clock> clock_;

  // The instance identifier of this server.
  std::unique_ptr<NodeInstancePB> instance_pb_;

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
  std::string FooterHtml() const;

  // Callback from the RPC system when a service queue has overflowed.
  void ServiceQueueOverflowed(rpc::ServicePool* service);

  // Start thread to remove excess glog and minidump files.
  Status StartExcessLogFileDeleterThread();
  void ExcessLogFileDeleterThread();

  // A method for internal use in the destructor. Some static code analyzers
  // issue a warning if calling a virtual function from destructor even if it's
  // safe in a particular case.
  void ShutdownImpl();

#ifdef TCMALLOC_ENABLED
  // Start thread to GC tcmalloc allocated memory.
  Status StartTcmallocMemoryGcThread();
  void TcmallocMemoryGcThread();
#endif

  // Utility object for DNS name resolutions.
  std::unique_ptr<DnsResolver> dns_resolver_;

  ServerBaseOptions options_;

  std::unique_ptr<DiagnosticsLog> diag_log_;
  scoped_refptr<Thread> excess_log_deleter_thread_;
#ifdef TCMALLOC_ENABLED
  scoped_refptr<Thread> tcmalloc_memory_gc_thread_;
#endif
  CountDownLatch stop_background_threads_latch_;

  std::unique_ptr<ScopedGLogMetrics> glog_metrics_;

  DISALLOW_COPY_AND_ASSIGN(ServerBase);
};

} // namespace server
} // namespace kudu
