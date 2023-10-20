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
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/clock/clock.h"
#include "kudu/clock/hybrid_clock.h"
#include "kudu/clock/logical_clock.h"
#include "kudu/codegen/compilation_manager.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/fs/fs_report.h"
#include "kudu/gutil/integral_types.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
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
#include "kudu/server/startup_path_handler.h"
#include "kudu/server/tcmalloc_metrics.h"
#include "kudu/server/tracing_path_handlers.h"
#include "kudu/server/webserver.h"
#include "kudu/util/atomic.h"
#include "kudu/util/cloud/instance_detector.h"
#include "kudu/util/cloud/instance_metadata.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/file_cache.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/flag_validators.h"
#include "kudu/util/flags.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/jwt-util.h"
#include "kudu/util/logging.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/minidump.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/dns_resolver.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/timer.h"
#ifdef TCMALLOC_ENABLED
#include "kudu/util/process_memory.h"
#endif
#include "kudu/util/slice.h"
#include "kudu/util/spinlock_profiling.h"
#include "kudu/util/string_case.h"
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
             "Timeout for negotiating an RPC connection, in milliseconds");
TAG_FLAG(rpc_negotiation_timeout_ms, advanced);

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

DEFINE_bool(rpc_listen_on_unix_domain_socket, false,
            "Whether the RPC server should listen on a Unix domain socket. If enabled, "
            "the RPC server will bind to a socket in the \"abstract namespace\" using "
            "a name which uniquely identifies the server instance.");
TAG_FLAG(rpc_listen_on_unix_domain_socket, experimental);

DEFINE_string(rpc_tls_ciphers,
              kudu::security::SecurityDefaults::kDefaultTlsCiphers,
              "TLSv1.2 (and prior) cipher suite preferences to use for "
              "TLS-secured RPC connections. Uses the OpenSSL cipher preference "
              "list format for TLSv1.2 and prior TLS protocol versions, "
              "for customizing TLSv1.3 cipher suites see "
              "--rpc_tls_ciphersuites flag. See 'man (1) ciphers' for more "
              "information.");
TAG_FLAG(rpc_tls_ciphers, advanced);

// The names for the '--rpc_tls_ciphers' and '--rpc_tls_ciphersuites' flags are
// confusingly close to each other, but the idea of leaking TLS versions into
// the flag names sounds even worse. Probably, at some point '--rpc_tls_ciphers'
// may become deprecated once TLSv1.2 is declared obsolete.
DEFINE_string(rpc_tls_ciphersuites,
              kudu::security::SecurityDefaults::kDefaultTlsCipherSuites,
              "TLSv1.3 cipher suite preferences to use for TLS-secured RPC "
              "connections. Uses the OpenSSL TLSv1.3 ciphersuite format. "
              "See 'man (1) ciphers' for more information. This flag is "
              "effective only if Kudu is built with OpenSSL v1.1.1 or newer.");
TAG_FLAG(rpc_tls_ciphersuites, advanced);

DEFINE_string(rpc_tls_min_protocol,
              kudu::security::SecurityDefaults::kDefaultTlsMinVersion,
              "The minimum protocol version to allow when for securing RPC "
              "connections with TLS. May be one of 'TLSv1', 'TLSv1.1', "
              "'TLSv1.2', 'TLSv1.3'.");
TAG_FLAG(rpc_tls_min_protocol, advanced);

DEFINE_string(rpc_tls_excluded_protocols, "",
              "A comma-separated list of TLS protocol versions to exclude from "
              "the set of advertised by the server when securing RPC "
              "connections with TLS. An empty string means the set of "
              "available TLS protocol versions is defined by the OpenSSL "
              "library and --rpc_tls_min_protocol flag.");
TAG_FLAG(rpc_tls_excluded_protocols, advanced);
TAG_FLAG(rpc_tls_excluded_protocols, experimental);

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

DEFINE_bool(enable_jwt_token_auth, false,
    "This enables JWT authentication, meaning that the server expects a valid "
    "JWT to be sent by the client which will be verified when the connection is "
    "being established. When true, read the JWT token out of the RPC and extract "
    "user name from the token payload.");
TAG_FLAG(enable_jwt_token_auth, experimental);

DEFINE_string(jwks_file_path, "",
    "File path of the pre-installed JSON Web Key Set (JWKS) for JWT verification.");
TAG_FLAG(jwks_file_path, experimental);

DEFINE_string(jwks_url, "",
    "URL of the JSON Web Key Set (JWKS) for JWT verification.");
TAG_FLAG(jwks_url, experimental);

// Enables retrieving the JWKS URL with verifying the presented TLS certificate
// from the server.
DEFINE_bool(jwks_verify_server_certificate, true,
            "Specifies if the TLS certificate of the JWKS server is verified when retrieving "
            "the JWKS from the specified JWKS URL. A certificate is considered valid if a "
            "trust chain can be established for it, and the certificate has a common name or "
            "SAN that matches the server's hostname. This should only be set to false for "
            "development / testing.");
TAG_FLAG(jwks_verify_server_certificate, experimental);
TAG_FLAG(jwks_verify_server_certificate, unsafe);

// The targeted use-case for the wall clock jump detection is spotting sudden
// swings of the local clock while it is still reported to be synchronized with
// reference NTP clock.
DEFINE_string(wall_clock_jump_detection, "auto",
              "Whether to run a sanity check on wall clock timestamps using "
              "the readings of the CLOCK_MONOTONIC_RAW clock as the reference. "
              "Acceptable values for this flag are \"auto\", \"enabled\", and "
              "\"disabled\". \"auto\" enables the sanity check in environments "
              "known to be susceptible to such clock jumps (e.g., Azure VMs); "
              "\"enabled\" unconditionally enables the check; \"disabled\" "
              "unconditionally disables the check. The threshold for the "
              "difference between deltas of consecutive timestamps read from "
              "wall and CLOCK_MONOTONIC_RAW clocks is controlled by the "
              "--wall_clock_jump_threshold_sec flag.");
TAG_FLAG(wall_clock_jump_detection, experimental);

// The idea behind having 900 seconds as the default threshold is to have the
// bar set quite high, but still under 1000 seconds which is the default time
// delta threshold for ntpd unless '-g' option is added or 'tinker panic 0'
// or similar directive is present in ntp.conf (ntpd would exit without trying
// to adjust time if it detects the difference between the reference time and
// the local clock time to be greater than 1000 seconds, see 'man ntpd').
DEFINE_uint32(wall_clock_jump_threshold_sec, 15 * 60,
              "Maximum allowed divergence between the wall and monotonic "
              "clocks; effective only when the clock jump protection "
              "is enabled");
TAG_FLAG(wall_clock_jump_threshold_sec, experimental);

DECLARE_bool(use_hybrid_clock);
DECLARE_int32(dns_resolver_max_threads_num);
DECLARE_uint32(dns_resolver_cache_capacity_mb);
DECLARE_uint32(dns_resolver_cache_ttl_sec);
DECLARE_int32(fs_data_dirs_available_space_cache_seconds);
DECLARE_int32(fs_wal_dir_available_space_cache_seconds);
DECLARE_int64(fs_wal_dir_reserved_bytes);
DECLARE_int64(fs_data_dirs_reserved_bytes);
DECLARE_string(log_filename);
DECLARE_string(keytab_file);
DECLARE_string(principal);
DECLARE_string(time_source);
DECLARE_string(trusted_certificate_file);

METRIC_DECLARE_gauge_size(merged_entities_count_of_server);
METRIC_DEFINE_gauge_int64(server, uptime,
                          "Server Uptime",
                          kudu::MetricUnit::kMicroseconds,
                          "Time interval since the server has started",
                          kudu::MetricLevel::kInfo);
METRIC_DEFINE_gauge_int64(server, wal_dir_space_available_bytes,
                          "WAL Directory Space Free",
                          kudu::MetricUnit::kBytes,
                          "Total WAL directory space available. Set to "
                          "-1 if reading the disk fails",
                          kudu::MetricLevel::kInfo);
METRIC_DEFINE_gauge_int64(server, data_dirs_space_available_bytes,
                          "Data Directories Space Free",
                          kudu::MetricUnit::kBytes,
                          "Total space available in all the data directories. Set to "
                          "-1 if reading any of the disks fails",
                          kudu::MetricLevel::kInfo);

#ifdef TCMALLOC_ENABLED
METRIC_DEFINE_gauge_int64(server, memory_usage,
                          "Current Memory Usage",
                          kudu::MetricUnit::kBytes,
                          "Current memory usage of the server process",
                          kudu::MetricLevel::kInfo);
#endif // #ifdef TCMALLOC_ENABLED

using kudu::cloud::CloudType;
using kudu::cloud::InstanceDetector;
using kudu::cloud::InstanceMetadata;
using kudu::security::RpcAuthentication;
using kudu::security::RpcEncryption;
using std::ostringstream;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

bool IsValidTlsProtocolStr(const string& str) {
  return
      iequals(str, "TLSv1.3") ||
      iequals(str, "TLSv1.2") ||
      iequals(str, "TLSv1.1") ||
      iequals(str, "TLSv1");
}

namespace server {

namespace {

enum class TriState {
  AUTO,
  ENABLED,
  DISABLED,
};

// This is a helper function to parse a flag that has three possible values:
// "auto", "enabled", "disabled".  That directly maps into the TriState enum.
Status ParseTriStateFlag(const string& name,
                         const string& value,
                         TriState* result = nullptr) {
  if (iequals(value, "auto")) {
    if (result) {
      *result = TriState::AUTO;
    }
    return Status::OK();
  }
  if (iequals(value, "enabled")) {
    if (result) {
      *result = TriState::ENABLED;
    }
    return Status::OK();
  }
  if (iequals(value, "disabled")) {
    if (result) {
      *result = TriState::DISABLED;
    }
    return Status::OK();
  }
  return Status::InvalidArgument(
      Substitute("$0: invalid value for flag --$1", value, name));
}

bool ValidateWallClockJumpDetection(const char* name, const string& value) {
  const auto s = ParseTriStateFlag(name, value);
  if (s.ok()) {
    return true;
  }
  LOG(ERROR) << s.ToString();
  return false;
}
DEFINE_validator(wall_clock_jump_detection, &ValidateWallClockJumpDetection);

bool ValidateWallClockJumpThreshold(const char* name, uint32_t value) {
  if (value == 0) {
    LOG(ERROR) << Substitute("--$0 must be greater than 0", name);
    return false;
  }
  return true;
}
DEFINE_validator(wall_clock_jump_threshold_sec, &ValidateWallClockJumpThreshold);

bool ValidateTlsProtocol(const char* /*flagname*/, const string& value) {
  return IsValidTlsProtocolStr(value);
}
DEFINE_validator(rpc_tls_min_protocol, &ValidateTlsProtocol);

bool ValidateTlsExcludedProtocols(const char* /*flagname*/,
                                  const std::string& value) {
  if (value.empty()) {
    return true;
  }

  vector<string> str_protos = strings::Split(value, ",", strings::SkipEmpty());
  for (const auto& str : str_protos) {
    if (IsValidTlsProtocolStr(str)) {
      continue;
    }
    return false;
  }
  return true;
}
DEFINE_validator(rpc_tls_excluded_protocols, &ValidateTlsExcludedProtocols);

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

bool ValidateJWKSNotEmpty() {
  if (FLAGS_enable_jwt_token_auth && (FLAGS_jwks_file_path.empty() && FLAGS_jwks_url.empty())) {
    LOG(ERROR) << "'jwt_token_auth' is enabled, but 'jwks_filepath' and 'jwks_url' are both empty";
    return false;
  }

  return true;
}

GROUP_FLAG_VALIDATOR(jwk_not_empty_validator, &ValidateJWKSNotEmpty);

bool ValidateEitherJWKSFilePathOrUrlSet() {
  if (!FLAGS_jwks_url.empty() && !FLAGS_jwks_file_path.empty()) {
    LOG(ERROR) << "only set either 'jwks_url' or 'jwks_file_path' but not both";
    return false;
  }
  return true;
}

GROUP_FLAG_VALIDATOR(jwks_file_or_url_set_validator, &ValidateEitherJWKSFilePathOrUrlSet);

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

// Calculates the free space on the given WAL/data directory's disk. Returns -1 in case of disk
// failure.
inline int64_t CalculateAvailableSpace(const ServerBaseOptions& options, const string& dir,
                                       int64_t flag_reserved_bytes, SpaceInfo* space_info) {
  if (!options.env->GetSpaceInfo(dir, space_info).ok()) {
    return -1;
  }
  bool should_reserve_one_percent = flag_reserved_bytes == -1;
  int reserved_bytes = should_reserve_one_percent ?
      space_info->capacity_bytes / 100 : flag_reserved_bytes;
  return std::max(static_cast<int64_t>(0), space_info->free_bytes - reserved_bytes);
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
      start_time_(MonoTime::Min()),
      minidump_handler_(new MinidumpExceptionHandler()),
      mem_tracker_(CreateMemTrackerForServer()),
      metric_registry_(new MetricRegistry()),
      metric_entity_(METRIC_ENTITY_server.Instantiate(metric_registry_.get(),
                                                      metric_namespace)),
      file_cache_(new FileCache("file cache", options.env,
                                GetFileCacheCapacity(options.env), metric_entity_)),
      rpc_server_(new RpcServer(options.rpc_opts)),
      startup_path_handler_(new StartupPathHandler(metric_entity_)),
      result_tracker_(new rpc::ResultTracker(shared_ptr<MemTracker>(
          MemTracker::CreateTracker(-1, "result-tracker", mem_tracker_)))),
      is_first_run_(false),
      stop_background_threads_latch_(1),
      dns_resolver_(new DnsResolver(
          FLAGS_dns_resolver_max_threads_num,
          FLAGS_dns_resolver_cache_capacity_mb * 1024 * 1024,
          MonoDelta::FromSeconds(FLAGS_dns_resolver_cache_ttl_sec))),
      options_(options) {
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

  if (FLAGS_webserver_enabled) {
    web_server_.reset(new Webserver(options.webserver_opts));
  }

  CHECK_OK(StartThreadInstrumentation(metric_entity_, web_server_.get()));
  CHECK_OK(codegen::CompilationManager::GetSingleton()->StartInstrumentation(
               metric_entity_));
}

ServerBase::~ServerBase() {
  ShutdownImpl();
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
  if (!FLAGS_use_hybrid_clock) {
    clock_.reset(new clock::LogicalClock(Timestamp::kInitialTimestamp,
                                         metric_entity_));
  } else {
    uint64_t threshold_usec = 0;
    unique_ptr<InstanceMetadata> im;
    RETURN_NOT_OK(WallClockJumpDetectionNeeded(&threshold_usec, &im));
    if (threshold_usec > 0) {
      LOG(INFO) << "enabling wall clock jump detection";
    }
    clock_.reset(new clock::HybridClock(
        metric_entity_, threshold_usec, std::move(im)));
  }

  // Initialize the clock immediately. This checks that the clock is synchronized
  // so we're less likely to get into a partially initialized state on disk during startup
  // if we're having clock problems.
  RETURN_NOT_OK_PREPEND(clock_->Init(), "Cannot initialize clock");

  Timer* init = startup_path_handler_->init_progress();
  Timer* read_filesystem = startup_path_handler_->read_filesystem_progress();
  init->Start();
  glog_metrics_.reset(new ScopedGLogMetrics(metric_entity_));
  tcmalloc::RegisterMetrics(metric_entity_);
  RegisterSpinLockContentionMetrics(metric_entity_);

  InitSpinLockContentionProfiling();

  // Get the FQDN of the node where the server is running. If fetching of the
  // FQDN fails, it attempts to set the 'hostname_' field to the local hostname.
  string hostname;
  if (auto s = GetFQDN(&hostname); !s.ok()) {
    const auto& msg = Substitute("could not determine host FQDN: $0", s.ToString());
    if (hostname.empty()) {
      LOG(ERROR) << msg;
      return s;
    }
    LOG(WARNING) << msg;
  }
  DCHECK(!hostname.empty());

  RETURN_NOT_OK(security::InitKerberosForServer(FLAGS_principal, FLAGS_keytab_file));
  RETURN_NOT_OK(file_cache_->Init());

  // Register the startup web handler and start the web server to make the web UI
  // available while the server is initializing, loading the file system, etc.
  //
  // NOTE: unlike the other path handlers, we register this path handler
  // separately, as the startup handler is meant to be displayed before all of
  // Kudu's subsystems have finished initializing.
  if (options_.fs_opts.block_manager_type == "file") {
    startup_path_handler_->set_is_using_lbm(false);
  }
  if (web_server_) {
    startup_path_handler_->RegisterStartupPathHandler(web_server_.get());
    AddPreInitializedDefaultPathHandlers(web_server_.get());
    web_server_->set_footer_html(FooterHtml());
    RETURN_NOT_OK(web_server_->Start());
  }

  fs::FsReport report;
  init->Stop();
  read_filesystem->Start();
  Status s = fs_manager_->Open(&report,
                               startup_path_handler_->read_instance_metadata_files_progress(),
                               startup_path_handler_->read_data_directories_progress(),
                               startup_path_handler_->containers_processed(),
                               startup_path_handler_->containers_total());
  // No instance files existed. Try creating a new FS layout.
  if (s.IsNotFound()) {
    LOG(INFO) << "This appears to be a new deployment of Kudu; creating new FS layout";
    is_first_run_ = true;

    if (options_.server_key_info.server_key.empty() &&
        options_.tenant_key_info.tenant_key.empty()) {
      s = fs_manager_->CreateInitialFileSystemLayout();
    } else if (!options_.tenant_key_info.tenant_key.empty()) {
      // The priority of tenant key is higher than that of server key.
      s = fs_manager_->CreateInitialFileSystemLayout(std::nullopt,
                                                     options_.tenant_key_info.tenant_name,
                                                     options_.tenant_key_info.tenant_id,
                                                     options_.tenant_key_info.tenant_key,
                                                     options_.tenant_key_info.tenant_key_iv,
                                                     options_.tenant_key_info.tenant_key_version);
    } else {
      s = fs_manager_->CreateInitialFileSystemLayout(std::nullopt,
                                                     std::nullopt,
                                                     std::nullopt,
                                                     options_.server_key_info.server_key,
                                                     options_.server_key_info.server_key_iv,
                                                     options_.server_key_info.server_key_version);
    }

    if (s.IsAlreadyPresent()) {
      return s.CloneAndPrepend("FS layout already exists; not overwriting existing layout");
    }
    RETURN_NOT_OK_PREPEND(s, "Could not create new FS layout");
    s = fs_manager_->Open(&report, startup_path_handler_->read_instance_metadata_files_progress(),
                          startup_path_handler_->read_data_directories_progress());
  }
  RETURN_NOT_OK_PREPEND(s, "Failed to load FS layout");
  RETURN_NOT_OK(report.LogAndCheckForFatalErrors());
  read_filesystem->Stop();
  RETURN_NOT_OK(InitAcls());

  vector<string> rpc_tls_excluded_protocols = strings::Split(
      FLAGS_rpc_tls_excluded_protocols, ",", strings::SkipEmpty());

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
         .set_rpc_tls_ciphersuites(FLAGS_rpc_tls_ciphersuites)
         .set_rpc_tls_excluded_protocols(std::move(rpc_tls_excluded_protocols))
         .set_rpc_tls_min_protocol(FLAGS_rpc_tls_min_protocol)
         .set_epki_cert_key_files(FLAGS_rpc_certificate_file, FLAGS_rpc_private_key_file)
         .set_epki_certificate_authority_file(FLAGS_rpc_ca_certificate_file)
         .set_epki_private_password_key_cmd(FLAGS_rpc_private_key_password_cmd)
         .set_keytab_file(FLAGS_keytab_file)
         .set_hostname(hostname)
         .enable_inbound_tls();

  if (options_.rpc_opts.rpc_reuseport) {
    builder.set_reuseport();
  }

  if (!FLAGS_keytab_file.empty()) {
    string service_name;
    RETURN_NOT_OK(security::MapPrincipalToLocalName(FLAGS_principal, &service_name));
    builder.set_sasl_proto_name(service_name);
  }

  if (FLAGS_enable_jwt_token_auth) {
    if (!FLAGS_jwks_url.empty()) {
      builder.set_jwt_verifier(std::make_shared<KeyBasedJwtVerifier>(
          FLAGS_jwks_url,
          FLAGS_jwks_verify_server_certificate,
          FLAGS_trusted_certificate_file));
    } else if (!FLAGS_jwks_file_path.empty()) {
      builder.set_jwt_verifier(std::make_shared<KeyBasedJwtVerifier>(
          FLAGS_jwks_file_path));
    } else {
      LOG(WARNING) << Substitute("JWT authentication enabled, but neither "
                                 "'jwks_url' nor 'jwks_file_path' is set");
    }
  }

  RETURN_NOT_OK(builder.Build(&messenger_));
  rpc_server_->set_too_busy_hook([this](rpc::ServicePool* pool) {
    this->ServiceQueueOverflowed(pool);
  });

  RETURN_NOT_OK(rpc_server_->Init(messenger_));

  if (FLAGS_rpc_listen_on_unix_domain_socket) {
    VLOG(1) << "Enabling listening on unix domain socket.";
    Sockaddr addr;
#if !defined(__APPLE__)
    RETURN_NOT_OK_PREPEND(addr.ParseUnixDomainPath(Substitute("@kudu-$0", fs_manager_->uuid())),
                          "unable to parse provided UNIX socket path");
#else
    RETURN_NOT_OK_PREPEND(addr.ParseUnixDomainPath(Substitute("/tmp/kudu-$0", fs_manager_->uuid())),
                          "unable to parse provided UNIX socket path");
#endif
    RETURN_NOT_OK_PREPEND(rpc_server_->AddBindAddress(addr),
                          "unable to add configured UNIX socket path to list of bind addresses "
                          "for RPC server");
  }

  return rpc_server_->Bind();
}

Status ServerBase::WallClockJumpDetectionNeeded(
    uint64_t* threshold_usec, unique_ptr<InstanceMetadata>* im) {
  DCHECK(threshold_usec);
  DCHECK(im);

  TriState st = TriState::AUTO;
  RETURN_NOT_OK(ParseTriStateFlag(
      "wall_clock_jump_detection", FLAGS_wall_clock_jump_detection, &st));
  switch (st) {
    case TriState::DISABLED:
      *threshold_usec = 0;
      return Status::OK();
    case TriState::ENABLED:
      *threshold_usec = FLAGS_wall_clock_jump_threshold_sec * 1000UL * 1000UL;
      return Status::OK();
    default:
      break;
  }
  DCHECK(st == TriState::AUTO);

  InstanceDetector detector;
  unique_ptr<InstanceMetadata> metadata;
  if (const auto s = detector.Detect(&metadata); !s.ok()) {
    LOG(INFO) << Substitute("$0: unable to detect cloud type of this node, "
                            "probably running in non-cloud environment", s.ToString());
    *threshold_usec = 0;
    return Status::OK();
  }
  LOG(INFO) << Substitute("running on $0 node",
                          cloud::TypeToString(metadata->type()));

  // Enable an extra check to detect clock jumps on Azure VMs: those seem
  // to be prone to clock jumps that aren't reflected in NTP clock
  // synchronization status reported by ntp_adjtime()/ntp_gettime().
  // Perhaps, that's due to the VMICTimeSync service interfering with the
  // kernel NTP discipline after a 'memory preserving' maintenance: when a
  // VM is 'unfrozen' after the update, the VMICTimeSync service updates the
  // VM's clock to compensate for the pause (see [1]).
  // [1] https://learn.microsoft.com/en-us/azure/virtual-machines/linux/time-sync
  if (metadata->type() == CloudType::AZURE) {
    *threshold_usec = FLAGS_wall_clock_jump_threshold_sec * 1000UL * 1000UL;
  } else {
    *threshold_usec = 0;
  }
  *im = std::move(metadata);

  return Status::OK();
}

Status ServerBase::InitAcls() {
  string service_user;
  std::optional<string> keytab_user = security::GetLoggedInUsernameFromKeytab();
  if (keytab_user) {
    // If we're logged in from a keytab, then everyone should be, and we expect them
    // to use the same mapped username.
    service_user = *keytab_user;
  } else {
    // If we aren't logged in from a keytab, then just assume that the services
    // will be running as the same Unix user as we are.
    RETURN_NOT_OK_PREPEND(GetLoggedInUser(&service_user),
                          "could not determine local username");
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
    vector<HostPort> hps;
    RETURN_NOT_OK(rpc_server_->GetBoundHostPorts(&hps));
    for (const auto& hp : hps) {
      *status->add_bound_rpc_addresses() = HostPortToPB(hp);
    }
  }

  // HTTP ports
  if (web_server_) {
    vector<HostPort> hps;
    RETURN_NOT_OK(web_server_->GetBoundHostPorts(&hps));
    for (const auto& hp : hps) {
      *status->add_bound_http_addresses() = HostPortToPB(hp);
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

bool ServerBase::IsFromSuperUser(const rpc::RpcContext* rpc) const {
  return superuser_acl_.UserAllowed(rpc->remote_user().username());
}

bool ServerBase::IsServiceUserOrSuperUser(const string& user) const {
  return service_acl_.UserAllowed(user) || superuser_acl_.UserAllowed(user);
}

bool ServerBase::Authorize(rpc::RpcContext* rpc, uint32_t allowed_roles) const {
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

  if (iequals(format, "json")) {
    string json = JsonWriter::ToJson(status, JsonWriter::PRETTY);
    RETURN_NOT_OK(WriteStringToFile(options_.env, Slice(json), path));
  } else if (iequals(format, "pb")) {
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
  return Thread::Create("server", "excess-log-deleter",
                        [this]() { this->ExcessLogFileDeleterThread(); },
                        &excess_log_deleter_thread_);
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

void ServerBase::ShutdownImpl() {
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

  security::DestroyKerberosForServer();
}

#ifdef TCMALLOC_ENABLED
Status ServerBase::StartTcmallocMemoryGcThread() {
  return Thread::Create("server", "tcmalloc-memory-gc",
                        [this]() { this->TcmallocMemoryGcThread(); },
                        &tcmalloc_memory_gc_thread_);
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
  if (instance_pb_) {
      return Substitute("<pre>$0\nserver uuid $1</pre>",
                        VersionInfo::GetVersionInfo(),
                        instance_pb_->permanent_uuid());
  }
  // Load the footer with UUID only if the UUID is available
  return Substitute("<pre>$0\n</pre>",
                    VersionInfo::GetVersionInfo());
}

Status ServerBase::Start() {
  GenerateInstanceID();

  DCHECK(!fs_manager_->uuid().empty());
  metric_entity_->SetAttribute("uuid", fs_manager_->uuid());
  DCHECK(!messenger_->hostname().empty());
  metric_entity_->SetAttribute("hostname", messenger_->hostname());

  RETURN_NOT_OK(RegisterService(
      unique_ptr<rpc::ServiceIf>(new GenericServiceImpl(this))));

  // Webserver shows the wall clock time when server was started and exposes
  // the server's uptime along with all other metrics, and the metrics logger
  // accesses the uptime metric as well, so it's necessary to have corresponding
  // information ready at this point.
  start_time_ = MonoTime::Now();
  start_walltime_ = static_cast<int64_t>(WallTime_Now());
  wal_dir_space_last_check_ = MonoTime::Min();
  wal_dir_available_space_ = -1;
  data_dirs_space_last_check_ = MonoTime::Min();
  data_dirs_available_space_ = -1;

  METRIC_uptime.InstantiateFunctionGauge(
      metric_entity_,
      [this]() {return (MonoTime::Now() - this->start_time()).ToMicroseconds();})->
          AutoDetachToLastValue(&metric_detacher_);

#ifdef TCMALLOC_ENABLED
  METRIC_memory_usage.InstantiateFunctionGauge(
      metric_entity_,
      []() {return process_memory::CurrentConsumption();})->
          AutoDetachToLastValue(&metric_detacher_);
#endif // #ifdef TCMALLOC_ENABLED

  METRIC_data_dirs_space_available_bytes.InstantiateFunctionGauge(
      metric_entity_,
      [this]() {
        return RefreshDataDirAvailableSpaceIfExpired(options_, *fs_manager_);
      })
      ->AutoDetachToLastValue(&metric_detacher_);

  METRIC_wal_dir_space_available_bytes.InstantiateFunctionGauge(
      metric_entity_,
      [this]() {
        return RefreshWalDirAvailableSpaceIfExpired(options_, *fs_manager_);
      })
      ->AutoDetachToLastValue(&metric_detacher_);


  RETURN_NOT_OK_PREPEND(StartMetricsLogging(), "Could not enable metrics logging");

  result_tracker_->StartGCThread();
  RETURN_NOT_OK(StartExcessLogFileDeleterThread());
#ifdef TCMALLOC_ENABLED
  RETURN_NOT_OK(StartTcmallocMemoryGcThread());
#endif
  Timer* start_rpc_server = startup_path_handler_->start_rpc_server_progress();
  start_rpc_server->Start();
  RETURN_NOT_OK(rpc_server_->Start());
  start_rpc_server->Stop();
  if (web_server_) {
    AddPostInitializedDefaultPathHandlers(web_server_.get());
    AddRpczPathHandlers(messenger_, web_server_.get());
    RegisterMetricsJsonHandler(web_server_.get(), metric_registry_.get());
    RegisterMetricsPrometheusHandler(web_server_.get(), metric_registry_.get());
    TracingPathHandlers::RegisterHandlers(web_server_.get());
    web_server_->set_footer_html(FooterHtml());
    web_server_->SetStartupComplete(true);
  }

  if (!options_.dump_info_path.empty()) {
    RETURN_NOT_OK_PREPEND(DumpServerInfo(options_.dump_info_path, options_.dump_info_format),
                          "Failed to dump server info to " + options_.dump_info_path);
  }

  return Status::OK();
}

int64_t ServerBase::RefreshWalDirAvailableSpaceIfExpired(const ServerBaseOptions& options,
                                                         const FsManager& fs_manager) {
  {
    std::lock_guard<simple_spinlock> l(lock_);
    if (MonoTime::Now() < wal_dir_space_last_check_ + MonoDelta::FromSeconds(
            FLAGS_fs_wal_dir_available_space_cache_seconds))
      return wal_dir_available_space_;
  }
  SpaceInfo space_info_waldir;
  int64_t wal_dir_space = CalculateAvailableSpace(options, fs_manager.GetWalsRootDir(),
                                                  FLAGS_fs_wal_dir_reserved_bytes,
                                                  &space_info_waldir);
  std::lock_guard<simple_spinlock> l(lock_);
  wal_dir_space_last_check_ = MonoTime::Now();
  wal_dir_available_space_ = wal_dir_space;
  return wal_dir_available_space_;
}

int64_t ServerBase::RefreshDataDirAvailableSpaceIfExpired(const ServerBaseOptions& options,
                                                          const FsManager& fs_manager) {
  {
    std::lock_guard<simple_spinlock> l(lock_);
    if (MonoTime::Now() < data_dirs_space_last_check_ + MonoDelta::FromSeconds(
            FLAGS_fs_data_dirs_available_space_cache_seconds))
      return data_dirs_available_space_;
  }
  SpaceInfo space_info_datadir;
  std::set<int64_t> fs_id;
  int64_t data_dirs_available_space = 0;
  for (const string& data_dir : fs_manager.GetDataRootDirs()) {
    int64_t available_space = CalculateAvailableSpace(options, data_dir,
                                                      FLAGS_fs_data_dirs_reserved_bytes,
                                                      &space_info_datadir);
    if (available_space == -1) {
      data_dirs_available_space = -1;
      break;
    }
    if (InsertIfNotPresent(&fs_id, space_info_datadir.filesystem_id)) {
      data_dirs_available_space += available_space;
    }
  }
  std::lock_guard<simple_spinlock> l(lock_);
  data_dirs_space_last_check_ = MonoTime::Now();
  data_dirs_available_space_ = data_dirs_available_space;
  return data_dirs_available_space_;
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
