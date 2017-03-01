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
#include "kudu/server/server_base.h"

#include <sstream>
#include <string>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/optional.hpp>
#include <gflags/gflags.h>

#include "kudu/codegen/compilation_manager.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/remote_user.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/security/kerberos_util.h"
#include "kudu/security/init.h"
#include "kudu/server/default-path-handlers.h"
#include "kudu/server/generic_service.h"
#include "kudu/server/glog_metrics.h"
#include "kudu/server/hybrid_clock.h"
#include "kudu/server/logical_clock.h"
#include "kudu/server/rpc_server.h"
#include "kudu/server/rpcz-path-handler.h"
#include "kudu/server/server_base.pb.h"
#include "kudu/server/server_base_options.h"
#include "kudu/server/tcmalloc_metrics.h"
#include "kudu/server/tracing-path-handlers.h"
#include "kudu/server/webserver.h"
#include "kudu/util/atomic.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/logging.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/minidump.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/rolling_log.h"
#include "kudu/util/spinlock_profiling.h"
#include "kudu/util/thread.h"
#include "kudu/util/user.h"
#include "kudu/util/version_info.h"

DEFINE_int32(num_reactor_threads, 4, "Number of libev reactor threads to start.");
TAG_FLAG(num_reactor_threads, advanced);

DEFINE_int32(min_negotiation_threads, 0, "Minimum number of connection negotiation threads.");
TAG_FLAG(min_negotiation_threads, advanced);

DEFINE_int32(max_negotiation_threads, 50, "Maximum number of connection negotiation threads.");
TAG_FLAG(max_negotiation_threads, advanced);

DEFINE_bool(webserver_enabled, true, "Whether to enable the web server on this daemon. "
            "NOTE: disabling the web server is also likely to prevent monitoring systems "
            "from properly capturing metrics.");
TAG_FLAG(webserver_enabled, advanced);

DEFINE_string(superuser_acl, "",
              "The list of usernames to allow as super users, comma-separated. "
              "A '*' entry indicates that all authenticated users are allowed. "
              "If this is left unset or blank, the default behavior is that the "
              "identity of the daemon itself determines the superuser. If the "
              "daemon is logged in from a Keytab, then the local username from "
              "the Kerberos principal is used; otherwise, the local Unix "
              "username is used.");
TAG_FLAG(superuser_acl, stable);
TAG_FLAG(superuser_acl, sensitive);

DEFINE_string(user_acl, "*",
              "The list of usernames who may access the cluster, comma-separated. "
              "A '*' entry indicates that all authenticated users are allowed.");
TAG_FLAG(user_acl, stable);
TAG_FLAG(user_acl, sensitive);

DECLARE_bool(use_hybrid_clock);

using std::ostringstream;
using std::shared_ptr;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace server {

namespace {

// Disambiguates between servers when in a minicluster.
AtomicInt<int32_t> mem_tracker_id_counter(-1);

shared_ptr<MemTracker> CreateMemTrackerForServer() {
  int32_t id = mem_tracker_id_counter.Increment();
  string id_str = "server";
  if (id != 0) {
    StrAppend(&id_str, " ", id);
  }
  return shared_ptr<MemTracker>(MemTracker::CreateTracker(-1, id_str));
}

} // anonymous namespace

ServerBase::ServerBase(string name, const ServerBaseOptions& options,
                       const string& metric_namespace)
    : name_(std::move(name)),
      minidump_handler_(new MinidumpExceptionHandler()),
      mem_tracker_(CreateMemTrackerForServer()),
      metric_registry_(new MetricRegistry()),
      metric_entity_(METRIC_ENTITY_server.Instantiate(metric_registry_.get(),
                                                      metric_namespace)),
      rpc_server_(new RpcServer(options.rpc_opts)),
      result_tracker_(new rpc::ResultTracker(shared_ptr<MemTracker>(
          MemTracker::CreateTracker(-1, "result-tracker", mem_tracker_)))),
      is_first_run_(false),
      options_(options),
      stop_background_threads_latch_(1) {
  FsManagerOpts fs_opts;
  fs_opts.metric_entity = metric_entity_;
  fs_opts.parent_mem_tracker = mem_tracker_;
  fs_opts.wal_path = options.fs_opts.wal_path;
  fs_opts.data_paths = options.fs_opts.data_paths;
  fs_manager_.reset(new FsManager(options.env, fs_opts));

  if (FLAGS_use_hybrid_clock) {
    clock_ = new HybridClock();
  } else {
    clock_ = LogicalClock::CreateStartingAt(Timestamp::kInitialTimestamp);
  }

  if (FLAGS_webserver_enabled) {
    web_server_.reset(new Webserver(options.webserver_opts));
  }

  CHECK_OK(StartThreadInstrumentation(metric_entity_, web_server_.get()));
  CHECK_OK(codegen::CompilationManager::GetSingleton()->StartInstrumentation(
               metric_entity_));
}

ServerBase::~ServerBase() {
  Shutdown();
}

Sockaddr ServerBase::first_rpc_address() const {
  vector<Sockaddr> addrs;
  WARN_NOT_OK(rpc_server_->GetBoundAddresses(&addrs),
              "Couldn't get bound RPC address");
  CHECK(!addrs.empty()) << "Not bound";
  return addrs[0];
}

Sockaddr ServerBase::first_http_address() const {
  CHECK(web_server_);
  vector<Sockaddr> addrs;
  WARN_NOT_OK(web_server_->GetBoundAddresses(&addrs),
              "Couldn't get bound webserver addresses");
  CHECK(!addrs.empty()) << "Not bound";
  return addrs[0];
}

const NodeInstancePB& ServerBase::instance_pb() const {
  return *DCHECK_NOTNULL(instance_pb_.get());
}

void ServerBase::GenerateInstanceID() {
  instance_pb_.reset(new NodeInstancePB);
  instance_pb_->set_permanent_uuid(fs_manager_->uuid());
  // TODO: maybe actually bump a sequence number on local disk instead of
  // using time.
  instance_pb_->set_instance_seqno(Env::Default()->NowMicros());
}

Status ServerBase::Init() {
  glog_metrics_.reset(new ScopedGLogMetrics(metric_entity_));
  tcmalloc::RegisterMetrics(metric_entity_);
  RegisterSpinLockContentionMetrics(metric_entity_);

  InitSpinLockContentionProfiling();

  // Initialize the clock immediately. This checks that the clock is synchronized
  // so we're less likely to get into a partially initialized state on disk during startup
  // if we're having clock problems.
  RETURN_NOT_OK_PREPEND(clock_->Init(), "Cannot initialize clock");

  RETURN_NOT_OK(security::InitKerberosForServer());

  Status s = fs_manager_->Open();
  if (s.IsNotFound()) {
    LOG(INFO) << "Could not load existing FS layout: " << s.ToString();
    LOG(INFO) << "Creating new FS layout";
    is_first_run_ = true;
    RETURN_NOT_OK_PREPEND(fs_manager_->CreateInitialFileSystemLayout(),
                          "Could not create new FS layout");
    s = fs_manager_->Open();
  }
  RETURN_NOT_OK_PREPEND(s, "Failed to load FS layout");

  RETURN_NOT_OK(InitAcls());

  // Create the Messenger.
  rpc::MessengerBuilder builder(name_);

  builder.set_num_reactors(FLAGS_num_reactor_threads)
         .set_min_negotiation_threads(FLAGS_min_negotiation_threads)
         .set_max_negotiation_threads(FLAGS_max_negotiation_threads)
         .set_metric_entity(metric_entity())
         // TODO(PKI): make built-in PKI enabled/disabled based on a flag.
         .enable_inbound_tls();
  RETURN_NOT_OK(builder.Build(&messenger_));

  RETURN_NOT_OK(rpc_server_->Init(messenger_));
  RETURN_NOT_OK(rpc_server_->Bind());
  clock_->RegisterMetrics(metric_entity_);

  RETURN_NOT_OK_PREPEND(StartMetricsLogging(), "Could not enable metrics logging");

  result_tracker_->StartGCThread();
  RETURN_NOT_OK(StartExcessLogFileDeleterThread());

  return Status::OK();
}

Status ServerBase::InitAcls() {

  string service_user;
  boost::optional<string> keytab_user = security::GetLoggedInUsernameFromKeytab();
  if (keytab_user) {
    // If we're logged in from a keytab, then everyone should be, and we expect them
    // to use the same mapped username.
    service_user = *keytab_user;
  } else {
    // If we aren't logged in from a keytab, then just assume that the services
    // will be running as the same Unix user as we are.
    RETURN_NOT_OK_PREPEND(GetLoggedInUser(&service_user),
                          "could not deterine local username");
  }

  // If the user has specified a superuser acl, use that. Otherwise, assume
  // that the same user running the service acts as superuser.
  if (!FLAGS_superuser_acl.empty()) {
    RETURN_NOT_OK_PREPEND(superuser_acl_.ParseFlag(FLAGS_superuser_acl),
                          "could not parse --superuser_acl flag");
  } else {
    superuser_acl_.Reset({ service_user });
  }

  RETURN_NOT_OK_PREPEND(user_acl_.ParseFlag(FLAGS_user_acl),
                        "could not parse --user_acl flag");

  // For the "service" ACL, we currently don't allow it to be user-configured,
  // but instead assume that all of the services will be running the same
  // way.
  service_acl_.Reset({ service_user });

  return Status::OK();
}

void ServerBase::GetStatusPB(ServerStatusPB* status) const {
  // Node instance
  status->mutable_node_instance()->CopyFrom(*instance_pb_);

  // RPC ports
  {
    vector<Sockaddr> addrs;
    CHECK_OK(rpc_server_->GetBoundAddresses(&addrs));
    for (const Sockaddr& addr : addrs) {
      HostPort hp;
      CHECK_OK(HostPortFromSockaddrReplaceWildcard(addr, &hp));
      HostPortPB* pb = status->add_bound_rpc_addresses();
      CHECK_OK(HostPortToPB(hp, pb));
    }
  }

  // HTTP ports
  if (web_server_) {
    vector<Sockaddr> addrs;
    CHECK_OK(web_server_->GetBoundAddresses(&addrs));
    for (const Sockaddr& addr : addrs) {
      HostPort hp;
      CHECK_OK(HostPortFromSockaddrReplaceWildcard(addr, &hp));
      HostPortPB* pb = status->add_bound_http_addresses();
      CHECK_OK(HostPortToPB(hp, pb));
    }
  }

  VersionInfo::GetVersionInfoPB(status->mutable_version_info());
}

void ServerBase::LogUnauthorizedAccess(rpc::RpcContext* rpc) const {
  LOG(WARNING) << "Unauthorized access attempt to method "
               << rpc->service_name() << "." << rpc->method_name()
               << " from " << rpc->requestor_string();
}

bool ServerBase::Authorize(rpc::RpcContext* rpc, uint32_t allowed_roles) {
  if ((allowed_roles & SUPER_USER) &&
      superuser_acl_.UserAllowed(rpc->remote_user().username())) {
    return true;
  }

  if ((allowed_roles & USER) &&
      user_acl_.UserAllowed(rpc->remote_user().username())) {
    return true;
  }

  if ((allowed_roles & SERVICE_USER) &&
      service_acl_.UserAllowed(rpc->remote_user().username())) {
    return true;
  }

  LogUnauthorizedAccess(rpc);
  rpc->RespondFailure(Status::NotAuthorized("unauthorized access to method",
                                            rpc->method_name()));
  return false;
}

Status ServerBase::DumpServerInfo(const string& path,
                                  const string& format) const {
  ServerStatusPB status;
  GetStatusPB(&status);

  if (boost::iequals(format, "json")) {
    string json = JsonWriter::ToJson(status, JsonWriter::PRETTY);
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
  return rpc_server_->RegisterService(std::move(rpc_impl));
}

Status ServerBase::StartMetricsLogging() {
  if (options_.metrics_log_interval_ms <= 0) {
    return Status::OK();
  }

  return Thread::Create("server", "metrics-logger", &ServerBase::MetricsLoggingThread,
                        this, &metrics_logging_thread_);
}

void ServerBase::MetricsLoggingThread() {
  RollingLog log(Env::Default(), FLAGS_log_dir, "metrics");

  // How long to wait before trying again if we experience a failure
  // logging metrics.
  const MonoDelta kWaitBetweenFailures = MonoDelta::FromSeconds(60);


  MonoTime next_log = MonoTime::Now();
  while (!stop_background_threads_latch_.WaitUntil(next_log)) {
    next_log = MonoTime::Now() +
        MonoDelta::FromMilliseconds(options_.metrics_log_interval_ms);

    std::ostringstream buf;
    buf << "metrics " << GetCurrentTimeMicros() << " ";

    // Collect the metrics JSON string.
    vector<string> metrics;
    metrics.push_back("*");
    MetricJsonOptions opts;
    opts.include_raw_histograms = true;

    JsonWriter writer(&buf, JsonWriter::COMPACT);
    Status s = metric_registry_->WriteAsJson(&writer, metrics, opts);
    if (!s.ok()) {
      WARN_NOT_OK(s, "Unable to collect metrics to log");
      next_log += kWaitBetweenFailures;
      continue;
    }

    buf << "\n";

    s = log.Append(buf.str());
    if (!s.ok()) {
      WARN_NOT_OK(s, "Unable to write metrics to log");
      next_log += kWaitBetweenFailures;
      continue;
    }
  }

  WARN_NOT_OK(log.Close(), "Unable to close metric log");
}

Status ServerBase::StartExcessLogFileDeleterThread() {
  // Try synchronously deleting excess log files once at startup to make sure it
  // works, then start a background thread to continue deleting them in the
  // future. Same with minidumps.
  if (!FLAGS_logtostderr) {
    RETURN_NOT_OK_PREPEND(DeleteExcessLogFiles(options_.env),
                          "Unable to delete excess log files");
  }
  RETURN_NOT_OK_PREPEND(minidump_handler_->DeleteExcessMinidumpFiles(options_.env),
                        "Unable to delete excess minidump files");
  return Thread::Create("server", "excess-log-deleter", &ServerBase::ExcessLogFileDeleterThread,
                        this, &excess_log_deleter_thread_);
}

void ServerBase::ExcessLogFileDeleterThread() {
  // How often to attempt to clean up excess glog and minidump files.
  const MonoDelta kWait = MonoDelta::FromSeconds(60);
  while (!stop_background_threads_latch_.WaitUntil(MonoTime::Now() + kWait)) {
    WARN_NOT_OK(DeleteExcessLogFiles(options_.env), "Unable to delete excess log files");
    WARN_NOT_OK(minidump_handler_->DeleteExcessMinidumpFiles(options_.env),
                "Unable to delete excess minidump files");
  }
}

std::string ServerBase::FooterHtml() const {
  return Substitute("<pre>$0\nserver uuid $1</pre>",
                    VersionInfo::GetShortVersionString(),
                    instance_pb_->permanent_uuid());
}

Status ServerBase::Start() {
  GenerateInstanceID();

  RETURN_NOT_OK(RegisterService(make_gscoped_ptr<rpc::ServiceIf>(
                                  new GenericServiceImpl(this))));

  RETURN_NOT_OK(rpc_server_->Start());

  if (web_server_) {
    AddDefaultPathHandlers(web_server_.get());
    AddRpczPathHandlers(messenger_, web_server_.get());
    RegisterMetricsJsonHandler(web_server_.get(), metric_registry_.get());
    TracingPathHandlers::RegisterHandlers(web_server_.get());
    web_server_->set_footer_html(FooterHtml());
    RETURN_NOT_OK(web_server_->Start());
  }

  if (!options_.dump_info_path.empty()) {
    RETURN_NOT_OK_PREPEND(DumpServerInfo(options_.dump_info_path, options_.dump_info_format),
                          "Failed to dump server info to " + options_.dump_info_path);
  }

  return Status::OK();
}

void ServerBase::Shutdown() {
  stop_background_threads_latch_.CountDown();
  if (metrics_logging_thread_) {
    metrics_logging_thread_->Join();
  }
  if (excess_log_deleter_thread_) {
    excess_log_deleter_thread_->Join();
  }
  if (web_server_) {
    web_server_->Stop();
  }
  rpc_server_->Shutdown();
}

} // namespace server
} // namespace kudu
