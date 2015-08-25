// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_SERVER_SERVER_BASE_H
#define KUDU_SERVER_SERVER_BASE_H

#include <string>
#include <tr1/memory>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/service_if.h"
#include "kudu/server/server_base_options.h"
#include "kudu/util/status.h"

namespace kudu {

class Env;
class FsManager;
class MemTracker;
class MetricEntity;
class MetricRegistry;
class NodeInstancePB;
class RpcServer;
class ScopedGLogMetrics;
class Sockaddr;
class Thread;
class Webserver;

namespace rpc {
class Messenger;
class ServiceIf;
} // namespace rpc

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
  const std::tr1::shared_ptr<rpc::Messenger>& messenger() const { return messenger_; }

  // Return the first RPC address that this server has bound to.
  // FATALs if the server is not started.
  Sockaddr first_rpc_address() const;

  // Return the first HTTP address that this server has bound to.
  // FATALs if the server is not started.
  Sockaddr first_http_address() const;

  FsManager* fs_manager() { return fs_manager_.get(); }

  // Return the instance identifier of this server.
  // This may not be called until after the server is Initted.
  const NodeInstancePB& instance_pb() const;

  const std::tr1::shared_ptr<MemTracker>& mem_tracker() const { return mem_tracker_; }

  const scoped_refptr<MetricEntity>& metric_entity() const { return metric_entity_; }

  MetricRegistry* metric_registry() { return metric_registry_.get(); }

  // Returns this server's clock.
  Clock* clock() { return clock_.get(); }

  // Return a PB describing the status of the server (version info, bound ports, etc)
  void GetStatusPB(ServerStatusPB* status) const;

 protected:
  ServerBase(const std::string& name,
             const ServerBaseOptions& options,
             const std::string& metrics_namespace);
  virtual ~ServerBase();

  Status Init();
  Status RegisterService(gscoped_ptr<rpc::ServiceIf> rpc_impl);
  Status Start();
  void Shutdown();

  const std::string name_;

  std::tr1::shared_ptr<MemTracker> mem_tracker_;
  gscoped_ptr<MetricRegistry> metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
  gscoped_ptr<FsManager> fs_manager_;
  gscoped_ptr<RpcServer> rpc_server_;
  gscoped_ptr<Webserver> web_server_;
  std::tr1::shared_ptr<rpc::Messenger> messenger_;
  bool is_first_run_;

  scoped_refptr<Clock> clock_;

  // The instance identifier of this server.
  gscoped_ptr<NodeInstancePB> instance_pb_;

 private:
  void GenerateInstanceID();
  Status DumpServerInfo(const std::string& path,
                        const std::string& format) const;
  Status StartMetricsLogging();
  void MetricsLoggingThread();
  std::string FooterHtml() const;

  ServerBaseOptions options_;

  scoped_refptr<Thread> metrics_logging_thread_;
  CountDownLatch stop_metrics_logging_latch_;

  gscoped_ptr<ScopedGLogMetrics> glog_metrics_;

  DISALLOW_COPY_AND_ASSIGN(ServerBase);
};

} // namespace server
} // namespace kudu
#endif /* KUDU_SERVER_SERVER_BASE_H */
