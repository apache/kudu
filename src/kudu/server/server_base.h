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

#include <memory>
#include <string>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/service_if.h"
#include "kudu/security/simple_acl.h"
#include "kudu/server/server_base_options.h"
#include "kudu/util/status.h"

namespace kudu {

class Env;
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

namespace rpc {
class RpcContext;
} // namespace rpc

namespace security {
class TlsContext;
class TokenVerifier;
} // namespace security

namespace server {
class Clock;

struct ServerBaseOptions;
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

  MetricRegistry* metric_registry() { return metric_registry_.get(); }

  const scoped_refptr<rpc::ResultTracker>& result_tracker() const { return result_tracker_; }

  // Returns this server's clock.
  Clock* clock() { return clock_.get(); }

  // Return a PB describing the status of the server (version info, bound ports, etc)
  void GetStatusPB(ServerStatusPB* status) const;

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
  virtual Status Start();
  virtual void Shutdown();

  Status RegisterService(gscoped_ptr<rpc::ServiceIf> rpc_impl);

  void LogUnauthorizedAccess(rpc::RpcContext* rpc) const;

  const std::string name_;

  std::unique_ptr<MinidumpExceptionHandler> minidump_handler_;
  std::shared_ptr<MemTracker> mem_tracker_;
  gscoped_ptr<MetricRegistry> metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
  gscoped_ptr<FsManager> fs_manager_;
  gscoped_ptr<RpcServer> rpc_server_;
  gscoped_ptr<Webserver> web_server_;

  std::shared_ptr<rpc::Messenger> messenger_;
  scoped_refptr<rpc::ResultTracker> result_tracker_;
  bool is_first_run_;

  scoped_refptr<Clock> clock_;

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
  std::string FooterHtml() const;

  // Start thread to remove excess glog and minidump files.
  Status StartExcessLogFileDeleterThread();
  void ExcessLogFileDeleterThread();

  ServerBaseOptions options_;

  scoped_refptr<Thread> metrics_logging_thread_;
  scoped_refptr<Thread> excess_log_deleter_thread_;
  CountDownLatch stop_background_threads_latch_;

  gscoped_ptr<ScopedGLogMetrics> glog_metrics_;

  DISALLOW_COPY_AND_ASSIGN(ServerBase);
};

} // namespace server
} // namespace kudu
#endif /* KUDU_SERVER_SERVER_BASE_H */
