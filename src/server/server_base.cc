// Copyright (c) 2013, Cloudera, inc.

#include "server/server_base.h"

#include <gflags/gflags.h>
#include <string>
#include <vector>

#include "common/wire_protocol.pb.h"
#include "rpc/messenger.h"
#include "server/default-path-handlers.h"
#include "server/fsmanager.h"
#include "server/rpc_server.h"
#include "server/tcmalloc_metrics.h"
#include "server/webserver.h"
#include "util/env.h"
#include "util/metrics.h"
#include "util/net/sockaddr.h"

DEFINE_int32(num_reactor_threads, 4, "Number of libev reactor threads to start."
             " (Advanced option).");

using std::vector;

namespace kudu {
namespace server {

ServerBase::ServerBase(Env* env, const string& base_dir,
                       const RpcServerOptions& rpc_opts,
                       const WebserverOptions& web_opts,
                       const string& metric_namespace)
  : metric_registry_(new MetricRegistry()),
    metric_ctx_(new MetricContext(metric_registry_.get(), metric_namespace)),
    fs_manager_(new FsManager(env, base_dir)),
    rpc_server_(new RpcServer(rpc_opts)),
    web_server_(new Webserver(web_opts)),
    is_first_run_(false) {
}

ServerBase::~ServerBase() {
  WARN_NOT_OK(Shutdown(), "Failed to shut down ServerBase");
}

Sockaddr ServerBase::first_rpc_address() const {
  vector<Sockaddr> addrs;
  rpc_server_->GetBoundAddresses(&addrs);
  CHECK(!addrs.empty()) << "Not bound";
  return addrs[0];
}

Sockaddr ServerBase::first_http_address() const {
  vector<Sockaddr> addrs;
  WARN_NOT_OK(web_server_->GetBoundAddresses(&addrs),
              "Couldn't get bound webserver addresses");
  CHECK(!addrs.empty()) << "Not bound";
  return addrs[0];
}

const NodeInstancePB& ServerBase::instance_pb() const {
  return *DCHECK_NOTNULL(instance_pb_.get());
}

const MetricContext& ServerBase::metric_context() const {
  return *metric_ctx_;
}

Status ServerBase::GenerateInstanceID() {
  instance_pb_.reset(new NodeInstancePB);
  instance_pb_->set_permanent_uuid(fs_manager_->uuid());
  // TODO: maybe actually bump a sequence number on local disk instead of
  // using time.
  instance_pb_->set_instance_seqno(Env::Default()->NowMicros());
  return Status::OK();
}

Status ServerBase::Init() {
  tcmalloc::RegisterMetrics(metric_registry_.get());

  Status s = fs_manager_->Open();
  if (s.IsNotFound()) {
    is_first_run_ = true;
    RETURN_NOT_OK_PREPEND(fs_manager_->CreateInitialFileSystemLayout(),
                          "Could not create new FS layout");
    s = fs_manager_->Open();
  }
  RETURN_NOT_OK_PREPEND(s, "Failed to load FS layout");

  RETURN_NOT_OK(GenerateInstanceID());

  // Create the Messenger.
  rpc::MessengerBuilder builder("TODO: add a ToString for ServerBase");

  builder.set_num_reactors(FLAGS_num_reactor_threads);
  builder.set_metric_context(metric_context());
  RETURN_NOT_OK(builder.Build(&messenger_));

  RETURN_NOT_OK(rpc_server_->Init());
  return Status::OK();
}

Status ServerBase::Start(gscoped_ptr<rpc::ServiceIf> rpc_impl) {
  RETURN_NOT_OK(rpc_server_->Start(messenger_, rpc_impl.Pass()));

  AddDefaultPathHandlers(web_server_.get());
  RegisterMetricsJsonHandler(web_server_.get(), metric_registry_.get());
  RETURN_NOT_OK(web_server_->Start());
  return Status::OK();
}

Status ServerBase::Shutdown() {
  if (messenger_) {
    messenger_->Shutdown();
    messenger_.reset();
  }
  web_server_->Stop();
  rpc_server_->Shutdown();
  return Status::OK();
}

} // namespace server
} // namespace kudu
