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

#include "kudu/master/sentry_privileges_fetcher.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <mutex>
#include <ostream>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/common/table_util.h"
#include "kudu/gutil/callback.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/sentry_client_metrics.h"
#include "kudu/master/sentry_privileges_cache_metrics.h"
#include "kudu/sentry/sentry_action.h"
#include "kudu/sentry/sentry_authorizable_scope.h"
#include "kudu/sentry/sentry_client.h"
#include "kudu/sentry/sentry_policy_service_types.h"
#include "kudu/thrift/client.h"
#include "kudu/thrift/ha_client_metrics.h"
#include "kudu/util/async_util.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/malloc.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/test_util_prod.h"
#include "kudu/util/ttl_cache_metrics.h"

DEFINE_string(sentry_service_rpc_addresses, "",
              "Comma-separated list of RPC addresses of the Sentry service(s). When "
              "set, Sentry integration is enabled, fine-grained access control is "
              "enforced in the master, and clients are issued authorization tokens. "
              "Must match the value of the sentry.service.client.server.rpc-addresses "
              "option in the Sentry server configuration.");
TAG_FLAG(sentry_service_rpc_addresses, experimental);

DEFINE_string(server_name, "server1",
              "Configures which server namespace the Kudu instance belongs to for defining "
              "server-level privileges in Sentry. Used to distinguish a particular Kudu "
              "cluster in case of a multi-cluster setup. Must match the value of the "
              "hive.sentry.server option in the HiveServer2 configuration, and the value "
              "of the --server_name in Impala configuration.");
TAG_FLAG(server_name, experimental);

DEFINE_string(kudu_service_name, "kudu",
              "The service name of the Kudu server. Must match the service name "
              "used for Kudu server of sentry.service.admin.group option in the "
              "Sentry server configuration.");
TAG_FLAG(kudu_service_name, experimental);

DEFINE_string(sentry_service_kerberos_principal, "sentry",
              "The service principal of the Sentry server. Must match the primary "
              "(user) portion of sentry.service.server.principal option in the "
              "Sentry server configuration.");
TAG_FLAG(sentry_service_kerberos_principal, experimental);

DEFINE_string(sentry_service_security_mode, "kerberos",
              "Configures whether Thrift connections to the Sentry server use "
              "SASL (Kerberos) security. Must match the value of the "
              "‘sentry.service.security.mode’ option in the Sentry server "
              "configuration.");
TAG_FLAG(sentry_service_security_mode, experimental);

DEFINE_int32(sentry_service_retry_count, 1,
             "The number of times that Sentry operations will retry after "
             "encountering retriable failures, such as network errors.");
TAG_FLAG(sentry_service_retry_count, advanced);
TAG_FLAG(sentry_service_retry_count, experimental);

DEFINE_int32(sentry_service_send_timeout_seconds, 60,
             "Configures the socket send timeout, in seconds, for Thrift "
             "connections to the Sentry server.");
TAG_FLAG(sentry_service_send_timeout_seconds, advanced);
TAG_FLAG(sentry_service_send_timeout_seconds, experimental);

DEFINE_int32(sentry_service_recv_timeout_seconds, 60,
             "Configures the socket receive timeout, in seconds, for Thrift "
             "connections to the Sentry server.");
TAG_FLAG(sentry_service_recv_timeout_seconds, advanced);
TAG_FLAG(sentry_service_recv_timeout_seconds, experimental);

DEFINE_int32(sentry_service_conn_timeout_seconds, 60,
             "Configures the socket connect timeout, in seconds, for Thrift "
             "connections to the Sentry server.");
TAG_FLAG(sentry_service_conn_timeout_seconds, advanced);
TAG_FLAG(sentry_service_conn_timeout_seconds, experimental);

DEFINE_int32(sentry_service_max_message_size_bytes, 100 * 1024 * 1024,
             "Maximum size of Sentry objects that can be received by the "
             "Sentry client in bytes. Must match the value of the "
             "sentry.policy.client.thrift.max.message.size option in the "
             "Sentry server configuration.");
TAG_FLAG(sentry_service_max_message_size_bytes, advanced);
TAG_FLAG(sentry_service_max_message_size_bytes, experimental);

// TODO(aserbin): provide some reasonable default value for the
//                --sentry_privileges_cache_capacity_mb flag. Maybe, make it
//                a multiple of FLAG_sentry_service_max_message_size_bytes ?
DEFINE_uint32(sentry_privileges_cache_capacity_mb, 256,
              "Capacity for the authz cache, in MiBytes. The cache stores "
              "information received from Sentry. A value of 0 means Sentry "
              "responses will not be cached.");
TAG_FLAG(sentry_privileges_cache_capacity_mb, advanced);

DEFINE_uint32(sentry_privileges_cache_ttl_factor, 10,
              "Factor of multiplication for the authz token validity interval "
              "defined by --authz_token_validity_seconds flag. The result of "
              "the multiplication of this factor and authz token validity "
              "defines the TTL of entries in the authz cache.");
TAG_FLAG(sentry_privileges_cache_ttl_factor, advanced);
TAG_FLAG(sentry_privileges_cache_ttl_factor, experimental);

DECLARE_int64(authz_token_validity_seconds);
DECLARE_string(kudu_service_name);
DECLARE_string(server_name);

using kudu::sentry::AuthorizableScopesSet;
using kudu::sentry::SentryAction;
using kudu::sentry::SentryAuthorizableScope;
using kudu::sentry::SentryClient;
using sentry::TListSentryPrivilegesRequest;
using sentry::TListSentryPrivilegesResponse;
using sentry::TSentryAuthorizable;
using sentry::TSentryGrantOption;
using sentry::TSentryPrivilege;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace master {

// Validates the sentry_service_rpc_addresses gflag.
static bool ValidateAddresses(const char* flag_name, const string& addresses) {
  vector<HostPort> host_ports;
  Status s = HostPort::ParseStringsWithScheme(addresses,
                                              SentryClient::kDefaultSentryPort,
                                              &host_ports);
  if (!s.ok()) {
    LOG(ERROR) << "invalid flag " << flag_name << ": " << s.ToString();
  }
  return s.ok();
}
DEFINE_validator(sentry_service_rpc_addresses, &ValidateAddresses);

namespace {

// Returns an authorizable based on the table identifier (in the format
// <database-name>.<table-name>) and the given scope.
Status GetAuthorizable(const string& table_ident,
                       SentryAuthorizableScope::Scope scope,
                       TSentryAuthorizable* authorizable) {
  Slice database;
  Slice table;
  // We should only ever request privileges from Sentry for authorizables of
  // scope equal to or higher than 'TABLE'.
  DCHECK_NE(scope, SentryAuthorizableScope::Scope::COLUMN);
  switch (scope) {
    case SentryAuthorizableScope::Scope::TABLE:
      RETURN_NOT_OK(ParseHiveTableIdentifier(table_ident, &database, &table));
      DCHECK(!table.empty());
      authorizable->__set_table(table.ToString());
      FALLTHROUGH_INTENDED;
    case SentryAuthorizableScope::Scope::DATABASE:
      if (database.empty() && table.empty()) {
        RETURN_NOT_OK(ParseHiveTableIdentifier(table_ident, &database, &table));
      }
      DCHECK(!database.empty());
      authorizable->__set_db(database.ToString());
      FALLTHROUGH_INTENDED;
    case SentryAuthorizableScope::Scope::SERVER:
      authorizable->__set_server(FLAGS_server_name);
      break;
    default:
      LOG(FATAL) << "unsupported SentryAuthorizableScope: "
                 << sentry::ScopeToString(scope);
      break;
  }

  return Status::OK();
}

// A utility class to help with Sentry privilege scoping, generating sequence
// of keys to lookup corresponding entries in the cache.
class AuthzInfoKey {
 public:
  // The maximum possible number of the elements in the key lookup sequence
  // returned by the key_sequence() method (see below). Maximum number of keys
  // to lookup in the cache is 2. See the comment for the GenerateKeySequence()
  // method below for more details.
  constexpr static size_t kKeySequenceMaxSize = 2;

  AuthzInfoKey(const string& user,
               const ::sentry::TSentryAuthorizable& authorizable);

  // Get the key to lookup the corresponding entry in the cache with the scope
  // of the authorizable widened as specified by the 'scope' parameter.
  // E.g., if the original scope of the autorizable specified in the constructor
  // was COLUMN, with the 'scope' set to TABLE the returned key is 'U/S/D/T',
  // while the key for the authorizable as is would be 'U/S/D/T/C'.
  const string& GetKey(SentryAuthorizableScope::Scope scope) const;

  // This method returns the sequence of keys to look up in the cache
  // if retrieving privileges granted to the 'user' on the 'authorizable'
  // specified in the constructor.
  const vector<string>& key_sequence() const {
    return key_sequence_;
  }

 private:
  // Generate the raw key sequence: a sequence of keys for the authz scope
  // hierarchy, starting from the very top (i.e. SERVER scope) and narrowing
  // down to the scope of the 'authorizable' specified in the constructor.
  //
  // For example, for user 'U' and authorizable { server:S, db:D, table:T }
  // the raw sequence of keys is { 'U/S', 'U/S/D', 'U/S/D/T' }.
  static vector<string> GenerateRawKeySequence(
      const string& user, const ::sentry::TSentryAuthorizable& authorizable);

  // Generate the cache key lookup sequence: a sequence of keys to use while
  // looking up corresponding entry in the authz cache. The maximum
  // length of the returned sequence is limited by kCacheKeySequenceMaxSize.
  //
  // For authorizables of the TABLE scope and narrower, it returns sequence
  // { 'U/S/D', 'U/S/D/T' }. For authorizables of the DATABASE scope it returns
  // { 'U/S/D' }. For authorizables of the SERVER scope it returns { 'U/S' }.
  static vector<string> GenerateKeySequence(const vector<string>& raw_sequence);

  // Convert the Sentry authz scope to an index in the list
  // { SERVER, DATABASE, TABLE, COLUMN }.
  static size_t ScopeToRawSequenceIdx(SentryAuthorizableScope::Scope scope);

  const vector<string> raw_key_sequence_;
  const vector<string> key_sequence_;
};

AuthzInfoKey::AuthzInfoKey(const string& user,
                           const ::sentry::TSentryAuthorizable& authorizable)
    : raw_key_sequence_(GenerateRawKeySequence(user, authorizable)),
      key_sequence_(GenerateKeySequence(raw_key_sequence_)) {
  DCHECK(!raw_key_sequence_.empty());
  DCHECK(!key_sequence_.empty());
  DCHECK_GE(kKeySequenceMaxSize, key_sequence_.size());
}

const string& AuthzInfoKey::GetKey(SentryAuthorizableScope::Scope scope) const {
  const size_t level = ScopeToRawSequenceIdx(scope);
  if (level < raw_key_sequence_.size()) {
    return raw_key_sequence_[level];
  }
  return raw_key_sequence_.back();
}

vector<string> AuthzInfoKey::GenerateRawKeySequence(
    const string& user, const ::sentry::TSentryAuthorizable& authorizable) {
  DCHECK(!user.empty());
  DCHECK(!authorizable.server.empty());
  if (!authorizable.__isset.db || authorizable.db.empty()) {
    return {
      Substitute("/$0/$1", user, authorizable.server),
    };
  }

  if (!authorizable.__isset.table || authorizable.table.empty()) {
    auto k0 = Substitute("/$0/$1", user, authorizable.server);
    auto k1 = Substitute("$0/$1", k0, authorizable.db);
    return { std::move(k0), std::move(k1), };
  }

  if (!authorizable.__isset.column || authorizable.column.empty()) {
    auto k0 = Substitute("/$0/$1", user, authorizable.server);
    auto k1 = Substitute("$0/$1", k0, authorizable.db);
    auto k2 = Substitute("$0/$1", k1, authorizable.table);
    return { std::move(k0), std::move(k1), std::move(k2), };
  }

  auto k0 = Substitute("/$0/$1", user, authorizable.server);
  auto k1 = Substitute("$0/$1", k0, authorizable.db);
  auto k2 = Substitute("$0/$1", k1, authorizable.table);
  auto k3 = Substitute("$0/$1", k2, authorizable.column);
  return { std::move(k0), std::move(k1), std::move(k2), std::move(k3), };
}

vector<string> AuthzInfoKey::GenerateKeySequence(
    const vector<string>& raw_sequence) {
  DCHECK(!raw_sequence.empty());
  vector<string> sequence;
  const auto idx_db = ScopeToRawSequenceIdx(SentryAuthorizableScope::DATABASE);
  if (idx_db < raw_sequence.size()) {
    sequence.emplace_back(raw_sequence[idx_db]);
  }
  const auto idx_table = ScopeToRawSequenceIdx(SentryAuthorizableScope::TABLE);
  if (idx_table < raw_sequence.size()) {
    sequence.emplace_back(raw_sequence[idx_table]);
  }
  if (sequence.empty()) {
    sequence.emplace_back(raw_sequence.back());
  }
  DCHECK_GE(kKeySequenceMaxSize, sequence.size());
  return sequence;
}

size_t AuthzInfoKey::ScopeToRawSequenceIdx(SentryAuthorizableScope::Scope scope) {
  size_t idx = 0;
  switch (scope) {
    case SentryAuthorizableScope::Scope::SERVER:
      idx = 0;
      break;
    case SentryAuthorizableScope::Scope::DATABASE:
      idx = 1;
      break;
    case SentryAuthorizableScope::Scope::TABLE:
      idx = 2;
      break;
    case SentryAuthorizableScope::Scope::COLUMN:
      idx = 3;
      break;
    default:
      LOG(DFATAL) << "unexpected scope: " << static_cast<int16_t>(scope);
      break;
  }

  return idx;
}

// Returns a unique string key for the given authorizable, at the given scope.
// The authorizable must be a well-formed at the given scope.
string GetKey(const string& server,
              const string& db,
              const string& table,
              const string& column,
              SentryAuthorizableScope::Scope scope) {
  DCHECK(!server.empty());
  switch (scope) {
    case SentryAuthorizableScope::SERVER:
      return server;
    case SentryAuthorizableScope::DATABASE:
      DCHECK(!db.empty());
      return Substitute("$0/$1", server, db);
    case SentryAuthorizableScope::TABLE:
      DCHECK(!db.empty());
      DCHECK(!table.empty());
      return Substitute("$0/$1/$2", server, db, table);
    case SentryAuthorizableScope::COLUMN:
      DCHECK(!db.empty());
      DCHECK(!table.empty());
      DCHECK(!column.empty());
      return Substitute("$0/$1/$2/$3", server, db, table, column);
    default:
      LOG(DFATAL) << "not reachable";
      break;
  }
  return "";
}

} // anonymous namespace


SentryPrivilegesBranch::SentryPrivilegesBranch(
    const ::sentry::TSentryAuthorizable& authorizable,
    const TListSentryPrivilegesResponse& response) {
  DoInit(authorizable, response);
}

size_t SentryPrivilegesBranch::memory_footprint() const {
  size_t res = kudu_malloc_usable_size(this);
  // This is a simple approximation: the exact information could be available
  // from the allocator of std::vector and std::string.
  res += privileges_.capacity() * sizeof(AuthorizablePrivileges);
  for (const auto& p : privileges_) {
    res += p.db_name.capacity();
    res += p.table_name.capacity();
    res += p.column_name.capacity();
    res += sizeof(decltype(p.allowed_actions));
  }
  return res;
}

void SentryPrivilegesBranch::Merge(const SentryPrivilegesBranch& other) {
  std::copy(other.privileges_.begin(), other.privileges_.end(),
            std::back_inserter(privileges_));
}

void SentryPrivilegesBranch::Split(
    SentryPrivilegesBranch* other_scope_db,
    SentryPrivilegesBranch* other_scope_table) const {
  SentryPrivilegesBranch scope_db;
  SentryPrivilegesBranch scope_table;
  for (const auto& e : privileges_) {
    switch (e.scope) {
      case SentryAuthorizableScope::SERVER:
      case SentryAuthorizableScope::DATABASE:
        scope_db.privileges_.emplace_back(e);
        break;
      case SentryAuthorizableScope::TABLE:
      case SentryAuthorizableScope::COLUMN:
        scope_table.privileges_.emplace_back(e);
        break;
      default:
        LOG(DFATAL) << "not reachable";
        break;
    }
  }
  *other_scope_db = std::move(scope_db);
  *other_scope_table = std::move(scope_table);
}

void SentryPrivilegesBranch::DoInit(
    const ::sentry::TSentryAuthorizable& authorizable,
    const TListSentryPrivilegesResponse& response) {
  unordered_map<string, AuthorizablePrivileges> privileges_map;
  for (const auto& privilege_resp : response.privileges) {
    SentryAuthorizableScope::Scope scope;
    SentryAction::Action action;
    if (!SentryPrivilegesFetcher::SentryPrivilegeIsWellFormed(
        privilege_resp, authorizable, &scope, &action)) {
      if (VLOG_IS_ON(1)) {
        std::ostringstream os;
        privilege_resp.printTo(os);
        VLOG(1) << Substitute("ignoring privilege response: $0", os.str());
      }
      continue;
    }
    const auto& db = privilege_resp.dbName;
    const auto& table = privilege_resp.tableName;
    const auto& column = privilege_resp.columnName;
    const string authorizable_key = GetKey(privilege_resp.serverName,
                                           db, table, column, scope);
    auto& privilege = LookupOrInsert(&privileges_map, authorizable_key,
        AuthorizablePrivileges(scope, db, table, column));
    InsertIfNotPresent(&privilege.allowed_actions, action);
    if (action == SentryAction::ALL || action == SentryAction::OWNER) {
      privilege.all_with_grant =
          (privilege_resp.grantOption == TSentryGrantOption::ENABLED);
    }
    if (VLOG_IS_ON(1)) {
      std::ostringstream os;
      privilege_resp.printTo(os);
      if (action != SentryAction::ALL && action != SentryAction::OWNER &&
          privilege_resp.grantOption == TSentryGrantOption::ENABLED) {
        VLOG(1) << "ignoring ENABLED grant option for unexpected action: "
                << static_cast<int16_t>(action);
      }
    }
  }
  EmplaceValuesFromMap(std::move(privileges_map), &privileges_);
}

SentryPrivilegesFetcher::SentryPrivilegesFetcher(
    scoped_refptr<MetricEntity> metric_entity)
    : metric_entity_(std::move(metric_entity)) {
  if (metric_entity_) {
    std::unique_ptr<SentryClientMetrics> metrics(
        new SentryClientMetrics(metric_entity_));
    sentry_client_.SetMetrics(std::move(metrics));
  }
}

Status SentryPrivilegesFetcher::Start() {
  ResetCache();

  vector<HostPort> addresses;
  RETURN_NOT_OK(HostPort::ParseStringsWithScheme(
      FLAGS_sentry_service_rpc_addresses,
      SentryClient::kDefaultSentryPort,
      &addresses));

  thrift::ClientOptions options;
  options.enable_kerberos = boost::iequals(
      FLAGS_sentry_service_security_mode, "kerberos");
  options.service_principal =
      FLAGS_sentry_service_kerberos_principal;
  options.send_timeout = MonoDelta::FromSeconds(
      FLAGS_sentry_service_send_timeout_seconds);
  options.recv_timeout = MonoDelta::FromSeconds(
      FLAGS_sentry_service_recv_timeout_seconds);
  options.conn_timeout = MonoDelta::FromSeconds(
      FLAGS_sentry_service_conn_timeout_seconds);
  options.max_buf_size =
      FLAGS_sentry_service_max_message_size_bytes;
  options.retry_count =
      FLAGS_sentry_service_retry_count;

  return sentry_client_.Start(std::move(addresses), std::move(options));
}

void SentryPrivilegesFetcher::Stop() {
  sentry_client_.Stop();
}

Status SentryPrivilegesFetcher::GetSentryPrivileges(
    SentryAuthorizableScope::Scope requested_scope,
    const string& table_name,
    const string& user,
    SentryPrivilegesBranch* privileges) {
  TSentryAuthorizable authorizable;
  RETURN_NOT_OK(GetAuthorizable(table_name, requested_scope, &authorizable));

  if (PREDICT_FALSE(requested_scope == SentryAuthorizableScope::SERVER &&
                    !IsGTest())) {
    // A request for an authorizable of the scope wider than DATABASE is served,
    // but the response from Sentry is not cached. With current privilege
    // scheme, SentryPrivilegesFetcher is not expected to request authorizables
    // of the SERVER scope unless this method is called from test code.
    LOG(DFATAL) << Substitute(
        "requesting privileges of the SERVER scope from Sentry "
        "on authorizable '$0' for user '$1'", table_name, user);
  }

  // Not expecting requests for authorizables of the scope narrower than TABLE,
  // even in tests.
  DCHECK_NE(SentryAuthorizableScope::COLUMN, requested_scope);

  const AuthzInfoKey aggregate_key(user, authorizable);
  // Do not query Sentry for authz scopes narrower than 'TABLE'.
  const auto& key = aggregate_key.GetKey(SentryAuthorizableScope::TABLE);
  const auto& key_seq = aggregate_key.key_sequence();
  vector<typename AuthzInfoCache::EntryHandle> handles;
  handles.reserve(AuthzInfoKey::kKeySequenceMaxSize);
  if (PREDICT_TRUE(cache_)) {
    for (const auto& e : key_seq) {
      auto handle = cache_->Get(e);
      VLOG(3) << Substitute("'$0': '$1' key lookup", key, e);
      if (!handle) {
        continue;
      }
      VLOG(2) << Substitute("'$0': '$1' key found", key, e);
      handles.emplace_back(std::move(handle));
    }
  }
  // If the cache contains all the necessary information, repackage the
  // cached information and return as the result.
  if (handles.size() == key_seq.size()) {
    SentryPrivilegesBranch result;
    for (const auto& e : handles) {
      DCHECK(e);
      result.Merge(e.value());
    }
    *privileges = std::move(result);
    return Status::OK();
  }

  Synchronizer sync;
  bool is_first_request = false;
  // The result (i.e. the retrieved informaton on privileges) might be used
  // independently by multiple threads. The shared ownership approach simplifies
  // passing the information around.
  shared_ptr<SentryPrivilegesBranch> fetched_privileges;
  {
    std::lock_guard<simple_spinlock> l(pending_requests_lock_);
    auto& pending_request = LookupOrEmplace(&pending_requests_,
                                            key, SentryRequestsInfo());
    // Is the queue of pending requests for the same key empty?
    // If yes, that's the first request being sent out.
    is_first_request = pending_request.callbacks.empty();
    pending_request.callbacks.emplace_back(sync.AsStatusCallback());
    if (is_first_request) {
      DCHECK(!pending_request.result);
      pending_request.result = std::make_shared<SentryPrivilegesBranch>();
    }
    fetched_privileges = pending_request.result;
  }
  if (!is_first_request) {
    RETURN_NOT_OK(sync.Wait());
    *privileges = *fetched_privileges;
    return Status::OK();
  }

  const auto s = FetchPrivilegesFromSentry(FLAGS_kudu_service_name,
                                           user, authorizable,
                                           fetched_privileges.get());
  if (s.ok() && PREDICT_TRUE(cache_)) {
    // Put the result into the cache. Negative results (i.e. errors) are not
    // cached. Split the information on privileges into at most two cache
    // entries, for authorizables of scope:
    //   * SERVER, DATABASE
    //   * TABLE, COLUMN
    //
    // From this perspective, privileges on a corresponding authorizable of the
    // DATABASE scope might be cached as a by-product when the original request
    // comes for an authorizable of the TABLE scope.
    SentryPrivilegesBranch priv_srv_db;
    SentryPrivilegesBranch priv_table_column;
    fetched_privileges->Split(&priv_srv_db, &priv_table_column);
    if (requested_scope != SentryAuthorizableScope::SERVER) {
      unique_ptr<SentryPrivilegesBranch> result_ptr(
          new SentryPrivilegesBranch(std::move(priv_srv_db)));
      const auto& key = aggregate_key.GetKey(
          SentryAuthorizableScope::DATABASE);
      const auto result_footprint = result_ptr->memory_footprint() +
          key.capacity();
      cache_->Put(key, std::move(result_ptr), result_footprint);
      VLOG(2) << Substitute(
          "added entry of size $0 bytes for key '$1' (server-database scope)",
          result_footprint, key);
    }
    // Don't add table-level records from the response into the cache
    // if the request to Sentry was of database or higher scope. Due to the
    // tree-like structure of Sentry's responses, those responses might contain
    // information on non-Kudu tables which are not relevant in the context of
    // Kudu's AuthzProvider. Upon detecting a cache miss for a table-scope key,
    // the fetcher requests information on corresponding table-scope privileges
    // from Sentry explicitly.
    if ((requested_scope != SentryAuthorizableScope::SERVER) &&
        (requested_scope != SentryAuthorizableScope::DATABASE)) {
      unique_ptr<SentryPrivilegesBranch> result_ptr(
          new SentryPrivilegesBranch(std::move(priv_table_column)));
      const auto& key = aggregate_key.GetKey(
          SentryAuthorizableScope::TABLE);
      const auto result_footprint = result_ptr->memory_footprint() +
          key.capacity();
      cache_->Put(key, std::move(result_ptr), result_footprint);
      VLOG(2) << Substitute(
          "added entry of size $0 bytes for key '$1' (table-column scope)",
          result_footprint, key);
    }
  }

  SentryRequestsInfo info;
  {
    std::lock_guard<simple_spinlock> l(pending_requests_lock_);
    info = EraseKeyReturnValuePtr(&pending_requests_, key);
  }
  CHECK_LE(1, info.callbacks.size());
  for (auto& cb : info.callbacks) {
    cb.Run(s);
  }
  RETURN_NOT_OK(s);
  *privileges = *fetched_privileges;
  return Status::OK();
}

// In addition to sanity checking of the contents of TSentryPrivilege in
// 'privilege', this function has DCHECKs to spot programmer's errors
// with regard to correctly setting fields of the 'requested_authorizable'
// parameter.
bool SentryPrivilegesFetcher::SentryPrivilegeIsWellFormed(
    const TSentryPrivilege& privilege,
    const TSentryAuthorizable& requested_authorizable,
    SentryAuthorizableScope::Scope* scope,
    SentryAction::Action* action) {
  DCHECK_EQ(FLAGS_server_name, requested_authorizable.server);
  DCHECK(!requested_authorizable.server.empty());
  DCHECK(requested_authorizable.column.empty());

  // A requested table must be accompanied by a database.
  bool authorizable_has_db = !requested_authorizable.db.empty();
  bool authorizable_has_table = !requested_authorizable.table.empty();
  DCHECK((authorizable_has_db && authorizable_has_table) || !authorizable_has_table);

  // Ignore anything that isn't a Kudu-related privilege.
  SentryAuthorizableScope granted_scope;
  SentryAction granted_action;
  Status s = SentryAuthorizableScope::FromString(privilege.privilegeScope, &granted_scope)
      .AndThen([&] {
        return SentryAction::FromString(privilege.action, &granted_action);
      });
  if (!s.ok()) {
    return false;
  }

  // Make sure that there aren't extraneous fields set in the privilege.
  for (const auto& empty_field : ExpectedEmptyFields(granted_scope.scope())) {
    switch (empty_field) {
      case SentryAuthorizableScope::COLUMN:
        if (!privilege.columnName.empty()) {
          return false;
        }
        break;
      case SentryAuthorizableScope::TABLE:
        if (!privilege.tableName.empty()) {
          return false;
        }
        break;
      case SentryAuthorizableScope::DATABASE:
        if (!privilege.dbName.empty()) {
          return false;
        }
        break;
      case SentryAuthorizableScope::SERVER:
        if (!privilege.serverName.empty()) {
          return false;
        }
        break;
      default:
        LOG(DFATAL) << Substitute("Granted privilege has invalid scope: $0",
                                  sentry::ScopeToString(granted_scope.scope()));
    }
  }
  // Make sure that all expected fields are set, and that they match those in
  // the requested authorizable. Sentry auhtorizables are case-insensitive
  // due to the properties of Kudu-HMS integration.
  for (const auto& nonempty_field : ExpectedNonEmptyFields(granted_scope.scope())) {
    switch (nonempty_field) {
      case SentryAuthorizableScope::COLUMN:
        if (!privilege.__isset.columnName || privilege.columnName.empty()) {
          return false;
        }
        break;
      case SentryAuthorizableScope::TABLE:
        if (!privilege.__isset.tableName || privilege.tableName.empty() ||
            (authorizable_has_table &&
             !boost::iequals(privilege.tableName, requested_authorizable.table))) {
          return false;
        }
        break;
      case SentryAuthorizableScope::DATABASE:
        if (!privilege.__isset.dbName || privilege.dbName.empty() ||
            (authorizable_has_db &&
             !boost::iequals(privilege.dbName, requested_authorizable.db))) {
          return false;
        }
        break;
      case SentryAuthorizableScope::SERVER:
        if (privilege.serverName.empty() ||
            !boost::iequals(privilege.serverName, requested_authorizable.server)) {
          return false;
        }
        break;
      default:
        LOG(DFATAL) << Substitute("Granted privilege has invalid scope: $0",
                                  sentry::ScopeToString(granted_scope.scope()));
    }
  }
  *scope = granted_scope.scope();
  *action = granted_action.action();
  return true;
}

const AuthorizableScopesSet& SentryPrivilegesFetcher::ExpectedEmptyFields(
    SentryAuthorizableScope::Scope scope) {
  static const AuthorizableScopesSet kServerFields{ SentryAuthorizableScope::DATABASE,
                                                    SentryAuthorizableScope::TABLE,
                                                    SentryAuthorizableScope::COLUMN };
  static const AuthorizableScopesSet kDbFields{ SentryAuthorizableScope::TABLE,
                                                SentryAuthorizableScope::COLUMN };
  static const AuthorizableScopesSet kTableFields{ SentryAuthorizableScope::COLUMN };
  static const AuthorizableScopesSet kColumnFields{};
  switch (scope) {
    case SentryAuthorizableScope::SERVER:
      return kServerFields;
    case SentryAuthorizableScope::DATABASE:
      return kDbFields;
    case SentryAuthorizableScope::TABLE:
      return kTableFields;
    case SentryAuthorizableScope::COLUMN:
      return kColumnFields;
    default:
      LOG(DFATAL) << "not reachable";
  }
  return kColumnFields;
}

const AuthorizableScopesSet& SentryPrivilegesFetcher::ExpectedNonEmptyFields(
    SentryAuthorizableScope::Scope scope) {
  static const AuthorizableScopesSet kColumnFields{ SentryAuthorizableScope::SERVER,
                                                    SentryAuthorizableScope::DATABASE,
                                                    SentryAuthorizableScope::TABLE,
                                                    SentryAuthorizableScope::COLUMN };
  static const AuthorizableScopesSet kTableFields{ SentryAuthorizableScope::SERVER,
                                                   SentryAuthorizableScope::DATABASE,
                                                   SentryAuthorizableScope::TABLE };
  static const AuthorizableScopesSet kDbFields{ SentryAuthorizableScope::SERVER,
                                                SentryAuthorizableScope::DATABASE };
  static const AuthorizableScopesSet kServerFields{ SentryAuthorizableScope::SERVER };
  switch (scope) {
    case SentryAuthorizableScope::COLUMN:
      return kColumnFields;
    case SentryAuthorizableScope::TABLE:
      return kTableFields;
    case SentryAuthorizableScope::DATABASE:
      return kDbFields;
    case SentryAuthorizableScope::SERVER:
      return kServerFields;
    default:
      LOG(DFATAL) << "not reachable";
  }
  return kColumnFields;
}

Status SentryPrivilegesFetcher::FetchPrivilegesFromSentry(
    const string& service_name,
    const string& user,
    const TSentryAuthorizable& authorizable,
    SentryPrivilegesBranch* result) {
  TListSentryPrivilegesRequest request;
  request.__set_requestorUserName(service_name);
  request.__set_principalName(user);
  request.__set_authorizableHierarchy(authorizable);
  TListSentryPrivilegesResponse response;
  RETURN_NOT_OK(sentry_client_.Execute(
      [&] (SentryClient* client) {
        return client->ListPrivilegesByUser(request, &response);
      }));
  *result = SentryPrivilegesBranch(authorizable, response);
  return Status::OK();
}

Status SentryPrivilegesFetcher::ResetCache() {
  const auto cache_capacity_bytes =
      FLAGS_sentry_privileges_cache_capacity_mb * 1024 * 1024;
  if (cache_capacity_bytes == 0) {
    cache_.reset();
  } else {
    const auto ttl_sec = FLAGS_authz_token_validity_seconds *
        FLAGS_sentry_privileges_cache_ttl_factor;
    cache_.reset(new AuthzInfoCache(cache_capacity_bytes,
                                    MonoDelta::FromSeconds(ttl_sec)));
    if (metric_entity_) {
      unique_ptr<SentryPrivilegesCacheMetrics> metrics(
          new SentryPrivilegesCacheMetrics(metric_entity_));
      cache_->SetMetrics(std::move(metrics));
    }
  }
  return Status::OK();
}

} // namespace master
} // namespace kudu
