// Copyright (c) 2013, Cloudera, inc.
#include "kudu/server/server_base.h"

#include <boost/foreach.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <gflags/gflags.h>
#include <string>
#include <vector>

#include "kudu/codegen/compilation_manager.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/rpc/messenger.h"
#include "kudu/server/default-path-handlers.h"
#include "kudu/server/hybrid_clock.h"
#include "kudu/server/logical_clock.h"
#include "kudu/server/rpc_server.h"
#include "kudu/server/tcmalloc_metrics.h"
#include "kudu/server/webserver.h"
#include "kudu/server/rpcz-path-handler.h"
#include "kudu/server/server_base_options.h"
#include "kudu/server/server_base.pb.h"
#include "kudu/util/thread.h"
#include "kudu/util/env.h"
#include "kudu/util/metrics.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/spinlock_profiling.h"

DEFINE_int32(num_reactor_threads, 4, "Number of libev reactor threads to start."
             " (Advanced option).");
DECLARE_bool(use_hybrid_clock);

using std::string;
using std::vector;

namespace kudu {
namespace server {

ServerBase::ServerBase(const ServerBaseOptions& options,
                       const string& metric_namespace)
  : metric_registry_(new MetricRegistry()),
    metric_ctx_(new MetricContext(metric_registry_.get(), metric_namespace)),
    fs_manager_(new FsManager(options.env, options.base_dir)),
    rpc_server_(new RpcServer(options.rpc_opts)),
    web_server_(new Webserver(options.webserver_opts)),
    is_first_run_(false),
    options_(options) {
  if (FLAGS_use_hybrid_clock) {
    clock_ = new HybridClock();
  } else {
    clock_ = LogicalClock::CreateStartingAt(Timestamp::kInitialTimestamp);
  }
  CHECK_OK(StartThreadInstrumentation(metric_registry_.get(), web_server_.get()));
  CHECK_OK(codegen::CompilationManager::GetSingleton()->StartInstrumentation(
             metric_registry_.get()));
}

ServerBase::~ServerBase() {
  Shutdown();
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

MetricContext* ServerBase::mutable_metric_context() const {
  return metric_ctx_.get();
}

void ServerBase::GenerateInstanceID() {
  instance_pb_.reset(new NodeInstancePB);
  instance_pb_->set_permanent_uuid(fs_manager_->uuid());
  // TODO: maybe actually bump a sequence number on local disk instead of
  // using time.
  instance_pb_->set_instance_seqno(Env::Default()->NowMicros());
}

Status ServerBase::Init() {
  tcmalloc::RegisterMetrics(metric_registry_.get());
  clock_->RegisterMetrics(metric_registry_.get());

  InitSpinLockContentionProfiling();

  Status s = fs_manager_->Open();
  if (s.IsNotFound()) {
    is_first_run_ = true;
    RETURN_NOT_OK_PREPEND(fs_manager_->CreateInitialFileSystemLayout(),
                          "Could not create new FS layout");
    s = fs_manager_->Open();
  }
  RETURN_NOT_OK_PREPEND(s, "Failed to load FS layout");

  // Create the Messenger.
  rpc::MessengerBuilder builder("TODO: add a ToString for ServerBase");

  builder.set_num_reactors(FLAGS_num_reactor_threads);
  builder.set_metric_context(metric_context());
  RETURN_NOT_OK(builder.Build(&messenger_));

  RETURN_NOT_OK(rpc_server_->Init(messenger_));
  RETURN_NOT_OK_PREPEND(clock_->Init(), "Cannot initialize clock");
  return Status::OK();
}

Status ServerBase::DumpServerInfo(const string& path,
                                  const string& format) const {
  ServerStatusPB status;

  // Node instance
  status.mutable_node_instance()->CopyFrom(*instance_pb_);

  // RPC ports
  {
    vector<Sockaddr> addrs;
    rpc_server_->GetBoundAddresses(&addrs);
    BOOST_FOREACH(const Sockaddr& addr, addrs) {
      HostPortPB* pb = status.add_bound_rpc_addresses();
      pb->set_host(addr.host());
      pb->set_port(addr.port());
    }
  }

  // HTTP ports
  {
    vector<Sockaddr> addrs;
    web_server_->GetBoundAddresses(&addrs);
    BOOST_FOREACH(const Sockaddr& addr, addrs) {
      HostPortPB* pb = status.add_bound_http_addresses();
      pb->set_host(addr.host());
      pb->set_port(addr.port());
    }
  }

  if (boost::iequals(format, "json")) {
    string json = JsonWriter::ToJson(status);
    RETURN_NOT_OK(WriteStringToFile(options_.env, Slice(json), path));
  } else if (boost::iequals(format, "pb")) {
    // TODO: Use PB container format?
    RETURN_NOT_OK(pb_util::WritePBToPath(options_.env, path, status,
                                         pb_util::NO_SYNC)); // durability doesn't matter
  } else {
    return Status::InvalidArgument("bad format", format);
  }

  LOG(INFO) << "Dumped server information to " << path;
  return Status::OK();
}

Status ServerBase::RegisterService(gscoped_ptr<rpc::ServiceIf> rpc_impl) {
  return rpc_server_->RegisterService(rpc_impl.Pass());
}

Status ServerBase::Start() {
  GenerateInstanceID();

  RETURN_NOT_OK(rpc_server_->Start());

  AddDefaultPathHandlers(web_server_.get());
  AddRpczPathHandlers(messenger_, web_server_.get());
  RegisterMetricsJsonHandler(web_server_.get(), metric_registry_.get());
  RETURN_NOT_OK(web_server_->Start());

  if (!options_.dump_info_path.empty()) {
    RETURN_NOT_OK_PREPEND(DumpServerInfo(options_.dump_info_path, options_.dump_info_format),
                          "Failed to dump server info to " + options_.dump_info_path);
  }

  return Status::OK();
}

void ServerBase::Shutdown() {
  web_server_->Stop();
  rpc_server_->Shutdown();
}

} // namespace server
} // namespace kudu
