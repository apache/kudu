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

#include <algorithm>
#include <cstdint>
#include <functional>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/clock/clock.h"
#include "kudu/clock/hybrid_clock.h"
#include "kudu/clock/logical_clock.h"
#include "kudu/codegen/compilation_manager.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/fs/fs_report.h"
#include "kudu/gutil/integral_types.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/remote_user.h"
#include "kudu/rpc/result_tracker.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/service_if.h"
#include "kudu/rpc/service_pool.h"
#include "kudu/security/init.h"
#include "kudu/security/security_flags.h"
#include "kudu/server/default_path_handlers.h"
#include "kudu/server/diagnostics_log.h"
#include "kudu/server/generic_service.h"
#include "kudu/server/glog_metrics.h"
#include "kudu/server/rpc_server.h"
#include "kudu/server/rpcz-path-handler.h"
#include "kudu/server/server_base.pb.h"
#include "kudu/server/server_base_options.h"
#include "kudu/server/tcmalloc_metrics.h"
#include "kudu/server/tracing_path_handlers.h"
#include "kudu/server/webserver.h"
#include "kudu/util/atomic.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/file_cache.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/flag_validators.h"
#include "kudu/util/flags.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/logging.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/minidump.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/dns_resolver.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#ifdef TCMALLOC_ENABLED
#include "kudu/util/process_memory.h"
#endif
#include "kudu/util/slice.h"
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

DEFINE_int64(rpc_negotiation_timeout_ms, 3000,
             "Timeout for negotiating an RPC connection.");
TAG_FLAG(rpc_negotiation_timeout_ms, advanced);
TAG_FLAG(rpc_negotiation_timeout_ms, runtime);

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

DEFINE_string(principal, "kudu/_HOST",
              "Kerberos principal that this daemon will log in as. The special token "
              "_HOST will be replaced with the FQDN of the local host.");
TAG_FLAG(principal, experimental);
// This is currently tagged as unsafe because there is no way for users to configure
// clients to expect a non-default principal. As such, configuring a server to login
// as a different one would end up with a cluster that can't be connected to.
// See KUDU-1884.
TAG_FLAG(principal, unsafe);

DEFINE_string(keytab_file, "",
              "Path to the Kerberos Keytab file for this server. Specifying a "
              "keytab file will cause the server to kinit, and enable Kerberos "
              "to be used to authenticate RPC connections.");
TAG_FLAG(keytab_file, stable);

DEFINE_bool(allow_world_readable_credentials, false,
            "Enable the use of keytab files and TLS private keys with "
            "world-readable permissions.");
TAG_FLAG(allow_world_readable_credentials, unsafe);

DEFINE_string(rpc_authentication, "optional",
              "Whether to require RPC connections to authenticate. Must be one "
              "of 'disabled', 'optional', or 'required'. If 'optional', "
              "authentication will be used when the remote end supports it. If "
              "'required', connections which are not able to authenticate "
              "(because the remote end lacks support) are rejected. Secure "
              "clusters should use 'required'.");
DEFINE_string(rpc_encryption, "optional",
              "Whether to require RPC connections to be encrypted. Must be one "
              "of 'disabled', 'optional', or 'required'. If 'optional', "
              "encryption will be used when the remote end supports it. If "
              "'required', connections which are not able to use encryption "
              "(because the remote end lacks support) are rejected. If 'disabled', "
              "encryption will not be used, and RPC authentication "
              "(--rpc_authentication) must also be disabled as well. "
              "Secure clusters should use 'required'.");
TAG_FLAG(rpc_authentication, evolving);
TAG_FLAG(rpc_encryption, evolving);

DEFINE_string(rpc_tls_ciphers,
              kudu::security::SecurityDefaults::kDefaultTlsCiphers,
              "The cipher suite preferences to use for TLS-secured RPC connections. "
              "Uses the OpenSSL cipher preference list format. See man (1) ciphers "
              "for more information.");
TAG_FLAG(rpc_tls_ciphers, advanced);

DEFINE_string(rpc_tls_min_protocol,
              kudu::security::SecurityDefaults::kDefaultTlsMinVersion,
              "The minimum protocol version to allow when for securing RPC "
              "connections with TLS. May be one of 'TLSv1', 'TLSv1.1', or "
              "'TLSv1.2'.");
TAG_FLAG(rpc_tls_min_protocol, advanced);

DEFINE_string(rpc_certificate_file, "",
              "Path to a PEM encoded X509 certificate to use for securing RPC "
              "connections with SSL/TLS. If set, '--rpc_private_key_file' and "
              "'--rpc_ca_certificate_file' must be set as well.");
DEFINE_string(rpc_private_key_file, "",
              "Path to a PEM encoded private key paired with the certificate "
              "from '--rpc_certificate_file'");
DEFINE_string(rpc_ca_certificate_file, "",
              "Path to the PEM encoded X509 certificate of the trusted external "
              "certificate authority. The provided certificate should be the root "
              "issuer of the certificate passed in '--rpc_certificate_file'.");
DEFINE_string(rpc_private_key_password_cmd, "", "A Unix command whose output "
              "returns the password used to decrypt the RPC server's private key "
              "file specified in --rpc_private_key_file. If the .PEM key file is "
              "not password-protected, this flag does not need to be set. "
              "Trailing whitespace will be trimmed before it is used to decrypt "
              "the private key.");

// Setting TLS certs and keys via CLI flags is only necessary for external
// PKI-based security, which is not yet production ready. Instead, see
// internal PKI (ipki) and Kerberos-based authentication.
TAG_FLAG(rpc_certificate_file, experimental);
TAG_FLAG(rpc_private_key_file, experimental);
TAG_FLAG(rpc_ca_certificate_file, experimental);

DEFINE_int32(rpc_default_keepalive_time_ms, 65000,
             "If an RPC connection from a client is idle for this amount of time, the server "
             "will disconnect the client. Setting this to any negative value keeps connections "
             "always alive.");
TAG_FLAG(rpc_default_keepalive_time_ms, advanced);

DEFINE_uint64(gc_tcmalloc_memory_interval_seconds, 30,
             "Interval seconds to GC tcmalloc memory, 0 means disabled.");
TAG_FLAG(gc_tcmalloc_memory_interval_seconds, advanced);
TAG_FLAG(gc_tcmalloc_memory_interval_seconds, runtime);

DEFINE_uint64(server_max_open_files, 0,
              "Maximum number of open file descriptors. If 0, Kudu will "
              "automatically calculate this value. This is a soft limit");
TAG_FLAG(server_max_open_files, advanced);

DECLARE_bool(use_hybrid_clock);
DECLARE_int32(dns_resolver_max_threads_num);
DECLARE_uint32(dns_resolver_cache_capacity_mb);
DECLARE_uint32(dns_resolver_cache_ttl_sec);
DECLARE_string(log_filename);

METRIC_DECLARE_gauge_size(merged_entities_count_of_server);

using kudu::security::RpcAuthentication;
using kudu::security::RpcEncryption;
using std::ostringstream;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

class HostPortPB;

namespace server {

namespace {

bool ValidateKeytabPermissions() {
  if (!FLAGS_keytab_file.empty() && !FLAGS_allow_world_readable_credentials) {
    bool world_readable_keytab;
    Status s = Env::Default()->IsFileWorldReadable(FLAGS_keytab_file, &world_readable_keytab);
    if (!s.ok()) {
      LOG(ERROR) << Substitute("$0: could not verify keytab file does not have world-readable "
                               "permissions: $1", FLAGS_keytab_file, s.ToString());
      return false;
    }
    if (world_readable_keytab) {
      LOG(ERROR) << "cannot use keytab file with world-readable permissions: "
                 << FLAGS_keytab_file;
      return false;
    }
  }

  return true;
}
GROUP_FLAG_VALIDATOR(keytab_permissions, &ValidateKeytabPermissions);

} // namespace

static bool ValidateRpcAuthentication(const char* flag_name, const string& flag_value) {
  security::RpcAuthentication result;
  Status s = ParseTriState(flag_name, flag_value, &result);
  if (!s.ok()) {
    LOG(ERROR) << s.message().ToString();
    return false;
  }
  return true;
}
DEFINE_validator(rpc_authentication, &ValidateRpcAuthentication);

static bool ValidateRpcEncryption(const char* flag_name, const string& flag_value) {
  security::RpcEncryption result;
  Status s = ParseTriState(flag_name, flag_value, &result);
  if (!s.ok()) {
    LOG(ERROR) << s.message().ToString();
    return false;
  }
  return true;
}
DEFINE_validator(rpc_encryption, &ValidateRpcEncryption);

static bool ValidateRpcAuthnFlags() {
  security::RpcAuthentication authentication;
  CHECK_OK(ParseTriState("--rpc_authentication", FLAGS_rpc_authentication, &authentication));

  security::RpcEncryption encryption;
  CHECK_OK(ParseTriState("--rpc_encryption", FLAGS_rpc_encryption, &encryption));

  if (encryption == RpcEncryption::DISABLED && authentication != RpcAuthentication::DISABLED) {
    LOG(ERROR) << "RPC authentication (--rpc_authentication) must be disabled "
                  "if RPC encryption (--rpc_encryption) is disabled";
    return false;
  }

  const bool has_keytab = !FLAGS_keytab_file.empty();
  const bool has_cert = !FLAGS_rpc_certificate_file.empty();
  if (authentication == RpcAuthentication::REQUIRED && !has_keytab && !has_cert) {
    LOG(ERROR) << "RPC authentication (--rpc_authentication) may not be "
                  "required unless Kerberos (--keytab_file) or external PKI "
                  "(--rpc_certificate_file et al) are configured";
    return false;
  }

  return true;
}
GROUP_FLAG_VALIDATOR(rpc_authn_flags, ValidateRpcAuthnFlags);

static bool ValidateExternalPkiFlags() {
  bool has_cert = !FLAGS_rpc_certificate_file.empty();
  bool has_key = !FLAGS_rpc_private_key_file.empty();
  bool has_ca = !FLAGS_rpc_ca_certificate_file.empty();

  if (has_cert != has_key || has_cert != has_ca) {
    LOG(ERROR) << "--rpc_certificate_file, --rpc_private_key_file, and "
                  "--rpc_ca_certificate_file flags must be set as a group; "
                  "i.e. either set all or none of them.";
    return false;
  }

  if (has_key && !FLAGS_allow_world_readable_credentials) {
    bool world_readable_private_key;
    Status s = Env::Default()->IsFileWorldReadable(FLAGS_rpc_private_key_file,
                                                   &world_readable_private_key);
    if (!s.ok()) {
      LOG(ERROR) << Substitute("$0: could not verify private key file does not have "
                               "world-readable permissions: $1",
                               FLAGS_rpc_private_key_file, s.ToString());
      return false;
    }
    if (world_readable_private_key) {
      LOG(ERROR) << "cannot use private key file with world-readable permissions: "
                 << FLAGS_rpc_private_key_file;
      return false;
    }
  }

  return true;
}
GROUP_FLAG_VALIDATOR(external_pki_flags, ValidateExternalPkiFlags);

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

int64_t GetFileCacheCapacity(Env* env) {
  // Maximize this process' open file limit first, if possible.
  static std::once_flag once;
  std::call_once(once, [&]() {
    env->IncreaseResourceLimit(Env::ResourceLimitType::OPEN_FILES_PER_PROCESS);
  });

  uint64_t rlimit =
      env->GetResourceLimit(Env::ResourceLimitType::OPEN_FILES_PER_PROCESS);
  // See server_max_open_files.
  if (FLAGS_server_max_open_files == 0) {
    // Use file-max as a possible upper bound.
    faststring buf;
    uint64_t buf_val;
    if (ReadFileToString(env, "/proc/sys/fs/file-max", &buf).ok() &&
        safe_strtou64(buf.ToString(), &buf_val)) {
      rlimit = std::min(rlimit, buf_val);
    }

    // Callers of this function expect a signed 64-bit integer, and rlimit
    // is an uint64_t type, so we need to avoid overflow.
    // The percentage we currently use is 40% by default, and although in fact
    // 40% of any value of the `uint64_t` type must be less than `kint64max`,
    // but the percentage may be adjusted in the future, such as to 60%, so to
    // prevent accidental overflow, we cap rlimit here.
    return std::min((rlimit / 5) * 2, static_cast<uint64_t>(kint64max));
  }
  LOG_IF(FATAL, FLAGS_server_max_open_files > rlimit) <<
      Substitute(
          "Configured open file limit (server_max_open_files) $0 "
          "exceeds process open file limit (ulimit) $1",
          FLAGS_server_max_open_files, rlimit);
  return FLAGS_server_max_open_files;
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
      file_cache_(new FileCache("file cache", options.env,
                                GetFileCacheCapacity(options.env), metric_entity_)),
      rpc_server_(new RpcServer(options.rpc_opts)),
      result_tracker_(new rpc::ResultTracker(shared_ptr<MemTracker>(
          MemTracker::CreateTracker(-1, "result-tracker", mem_tracker_)))),
      is_first_run_(false),
      dns_resolver_(new DnsResolver(
          FLAGS_dns_resolver_max_threads_num,
          FLAGS_dns_resolver_cache_capacity_mb * 1024 * 1024,
          MonoDelta::FromSeconds(FLAGS_dns_resolver_cache_ttl_sec))),
      options_(options),
      stop_background_threads_latch_(1) {
  metric_entity_->NeverRetire(
      METRIC_merged_entities_count_of_server.InstantiateHidden(metric_entity_, 1));

  FsManagerOpts fs_opts;
  fs_opts.metric_entity = metric_entity_;
  fs_opts.parent_mem_tracker = mem_tracker_;
  fs_opts.block_manager_type = options.fs_opts.block_manager_type;
  fs_opts.wal_root = options.fs_opts.wal_root;
  fs_opts.data_roots = options.fs_opts.data_roots;
  fs_opts.file_cache = file_cache_.get();
  fs_manager_.reset(new FsManager(options.env, std::move(fs_opts)));

  if (FLAGS_use_hybrid_clock) {
    clock_.reset(new clock::HybridClock(metric_entity_));
  } else {
    clock_.reset(new clock::LogicalClock(Timestamp::kInitialTimestamp,
                                         metric_entity_));
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

  RETURN_NOT_OK(security::InitKerberosForServer(FLAGS_principal, FLAGS_keytab_file));

  RETURN_NOT_OK(file_cache_->Init());

  fs::FsReport report;
  Status s = fs_manager_->Open(&report);
  // No instance files existed. Try creating a new FS layout.
  if (s.IsNotFound()) {
    LOG(INFO) << "Could not load existing FS layout: " << s.ToString();
    LOG(INFO) << "Attempting to create new FS layout instead";
    is_first_run_ = true;
    s = fs_manager_->CreateInitialFileSystemLayout();
    if (s.IsAlreadyPresent()) {
      return s.CloneAndPrepend("FS layout already exists; not overwriting existing layout");
    }
    RETURN_NOT_OK_PREPEND(s, "Could not create new FS layout");
    s = fs_manager_->Open(&report);
  }
  RETURN_NOT_OK_PREPEND(s, "Failed to load FS layout");
  RETURN_NOT_OK(report.LogAndCheckForFatalErrors());

  RETURN_NOT_OK(InitAcls());

  // Create the Messenger.
  rpc::MessengerBuilder builder(name_);

  builder.set_num_reactors(FLAGS_num_reactor_threads)
         .set_min_negotiation_threads(FLAGS_min_negotiation_threads)
         .set_max_negotiation_threads(FLAGS_max_negotiation_threads)
         .set_metric_entity(metric_entity())
         .set_connection_keep_alive_time(FLAGS_rpc_default_keepalive_time_ms)
         .set_rpc_negotiation_timeout_ms(FLAGS_rpc_negotiation_timeout_ms)
         .set_rpc_authentication(FLAGS_rpc_authentication)
         .set_rpc_encryption(FLAGS_rpc_encryption)
         .set_rpc_tls_ciphers(FLAGS_rpc_tls_ciphers)
         .set_rpc_tls_min_protocol(FLAGS_rpc_tls_min_protocol)
         .set_epki_cert_key_files(FLAGS_rpc_certificate_file, FLAGS_rpc_private_key_file)
         .set_epki_certificate_authority_file(FLAGS_rpc_ca_certificate_file)
         .set_epki_private_password_key_cmd(FLAGS_rpc_private_key_password_cmd)
         .set_keytab_file(FLAGS_keytab_file)
         .enable_inbound_tls();

  if (options_.rpc_opts.rpc_reuseport) {
    builder.set_reuseport();
  }

  RETURN_NOT_OK(builder.Build(&messenger_));
  rpc_server_->set_too_busy_hook(std::bind(
      &ServerBase::ServiceQueueOverflowed, this, std::placeholders::_1));

  RETURN_NOT_OK(rpc_server_->Init(messenger_));
  RETURN_NOT_OK(rpc_server_->Bind());

  RETURN_NOT_OK_PREPEND(StartMetricsLogging(), "Could not enable metrics logging");

  result_tracker_->StartGCThread();
  RETURN_NOT_OK(StartExcessLogFileDeleterThread());
#ifdef TCMALLOC_ENABLED
  RETURN_NOT_OK(StartTcmallocMemoryGcThread());
#endif

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

Status ServerBase::GetStatusPB(ServerStatusPB* status) const {
  // Node instance
  status->mutable_node_instance()->CopyFrom(*instance_pb_);

  // RPC ports
  {
    vector<Sockaddr> addrs;
    RETURN_NOT_OK_PREPEND(rpc_server_->GetBoundAddresses(&addrs),
                          "could not get bound RPC addresses");
    for (const Sockaddr& addr : addrs) {
      HostPort hp;
      RETURN_NOT_OK_PREPEND(HostPortFromSockaddrReplaceWildcard(addr, &hp),
                            "could not get RPC hostport");
      HostPortPB* pb = status->add_bound_rpc_addresses();
      RETURN_NOT_OK_PREPEND(HostPortToPB(hp, pb),
                            "could not convert RPC hostport");
    }
  }

  // HTTP ports
  if (web_server_) {
    vector<Sockaddr> addrs;
    RETURN_NOT_OK_PREPEND(web_server_->GetBoundAddresses(&addrs),
                          "could not get bound web addresses");
    for (const Sockaddr& addr : addrs) {
      HostPort hp;
      RETURN_NOT_OK_PREPEND(HostPortFromSockaddrReplaceWildcard(addr, &hp),
                            "could not get web hostport");
      HostPortPB* pb = status->add_bound_http_addresses();
      RETURN_NOT_OK_PREPEND(HostPortToPB(hp, pb),
                            "could not convert web hostport");
    }
  }

  VersionInfo::GetVersionInfoPB(status->mutable_version_info());
  return Status::OK();
}

void ServerBase::LogUnauthorizedAccess(rpc::RpcContext* rpc) const {
  LOG(WARNING) << "Unauthorized access attempt to method "
               << rpc->service_name() << "." << rpc->method_name()
               << " from " << rpc->requestor_string();
}

bool ServerBase::IsFromSuperUser(const rpc::RpcContext* rpc) {
  return superuser_acl_.UserAllowed(rpc->remote_user().username());
}

bool ServerBase::Authorize(rpc::RpcContext* rpc, uint32_t allowed_roles) {
  if ((allowed_roles & SUPER_USER) && IsFromSuperUser(rpc)) {
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
  RETURN_NOT_OK_PREPEND(GetStatusPB(&status), "could not get server status");

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

Status ServerBase::RegisterService(unique_ptr<rpc::ServiceIf> rpc_impl) {
  return rpc_server_->RegisterService(std::move(rpc_impl));
}

Status ServerBase::StartMetricsLogging() {
  if (options_.metrics_log_interval_ms <= 0) {
    return Status::OK();
  }
  if (FLAGS_log_dir.empty()) {
    LOG(INFO) << "Not starting metrics log since no log directory was specified.";
    return Status::OK();
  }
  unique_ptr<DiagnosticsLog> l(new DiagnosticsLog(FLAGS_log_dir, FLAGS_log_filename,
      metric_registry_.get()));
  l->SetMetricsLogInterval(MonoDelta::FromMilliseconds(options_.metrics_log_interval_ms));
  RETURN_NOT_OK(l->Start());
  diag_log_ = std::move(l);
  return Status::OK();
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

#ifdef TCMALLOC_ENABLED
Status ServerBase::StartTcmallocMemoryGcThread() {
  return Thread::Create("server", "tcmalloc-memory-gc", &ServerBase::TcmallocMemoryGcThread,
                        this, &tcmalloc_memory_gc_thread_);
}

void ServerBase::TcmallocMemoryGcThread() {
  MonoDelta check_interval;
  do {
    // If GC is disabled, wake up every 60 seconds anyway to recheck the value of the flag.
    check_interval = MonoDelta::FromSeconds(FLAGS_gc_tcmalloc_memory_interval_seconds > 0
      ? FLAGS_gc_tcmalloc_memory_interval_seconds : 60);
    if (FLAGS_gc_tcmalloc_memory_interval_seconds > 0) {
      kudu::process_memory::GcTcmalloc();
    }
  } while (!stop_background_threads_latch_.WaitFor(check_interval));
}
#endif

std::string ServerBase::FooterHtml() const {
  return Substitute("<pre>$0\nserver uuid $1</pre>",
                    VersionInfo::GetVersionInfo(),
                    instance_pb_->permanent_uuid());
}

Status ServerBase::Start() {
  GenerateInstanceID();

  RETURN_NOT_OK(RegisterService(
      unique_ptr<rpc::ServiceIf>(new GenericServiceImpl(this))));
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

  start_time_ = WallTime_Now();

  return Status::OK();
}

void ServerBase::Shutdown() {
  // First, stop accepting incoming requests and wait for any outstanding
  // requests to finish processing.
  //
  // Note: prior to Messenger::Shutdown, it is assumed that any incoming RPCs
  // deferred from reactor threads have already been cleaned up.
  if (web_server_) {
    web_server_->Stop();
  }
  rpc_server_->Shutdown();
  if (messenger_) {
    messenger_->Shutdown();
  }

  // Next, shut down remaining server components.
  stop_background_threads_latch_.CountDown();
  if (diag_log_) {
    diag_log_->Stop();
  }
  if (excess_log_deleter_thread_) {
    excess_log_deleter_thread_->Join();
  }
#ifdef TCMALLOC_ENABLED
  if (tcmalloc_memory_gc_thread_) {
    tcmalloc_memory_gc_thread_->Join();
  }
#endif
}

void ServerBase::UnregisterAllServices() {
  messenger_->UnregisterAllServices();
}

void ServerBase::ServiceQueueOverflowed(rpc::ServicePool* service) {
  if (!diag_log_) return;

  // Logging all of the stacks is relatively heavy-weight, so if we are in a persistent
  // state of overload, it's probably not a good idea to start compounding the issue with
  // a lot of stack-logging activity. So, we limit the frequency of stack-dumping.
  static logging::LogThrottler throttler;
  const int kStackDumpFrequencySecs = 5;
  int suppressed = 0;
  if (PREDICT_TRUE(!throttler.ShouldLog(kStackDumpFrequencySecs, "", &suppressed))) {
    return;
  }

  diag_log_->DumpStacksNow(Substitute("service queue overflowed for $0",
                                      service->service_name()));
}

} // namespace server
} // namespace kudu
