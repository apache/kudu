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

#include "kudu/client/client.h"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <set>
#include <string>
#include <type_traits>
#include <vector>

#include <glog/logging.h>
#include <google/protobuf/stubs/common.h>

#include "kudu/client/callbacks.h"
#include "kudu/client/client-internal.h"
#include "kudu/client/client.pb.h"
#include "kudu/client/client_builder-internal.h"
#include "kudu/client/columnar_scan_batch.h"
#include "kudu/client/error-internal.h"
#include "kudu/client/error_collector.h"
#include "kudu/client/master_proxy_rpc.h"
#include "kudu/client/meta_cache.h"
#include "kudu/client/partitioner-internal.h"
#include "kudu/client/replica-internal.h"
#include "kudu/client/row_result.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/scan_configuration.h"
#include "kudu/client/scan_predicate-internal.h"
#include "kudu/client/scan_token-internal.h"
#include "kudu/client/scanner-internal.h"
#include "kudu/client/schema-internal.h"
#include "kudu/client/session-internal.h"
#include "kudu/client/table-internal.h"
#include "kudu/client/table_alterer-internal.h"
#include "kudu/client/table_creator-internal.h"
#include "kudu/client/table_statistics-internal.h"
#include "kudu/client/tablet-internal.h"
#include "kudu/client/tablet_server-internal.h"
#include "kudu/client/transaction-internal.h"
#include "kudu/client/value.h"
#include "kudu/client/write_op.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/partition.h"
#include "kudu/common/partition_pruner.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/row_operations.pb.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/common/txn_id.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/request_tracker.h"
#include "kudu/rpc/rpc.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/sasl_common.h"
#include "kudu/rpc/user_credentials.h"
#include "kudu/security/cert.h"
#include "kudu/security/tls_context.h"
#include "kudu/security/token.pb.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_service.proxy.h" // IWYU pragma: keep
#include "kudu/util/async_util.h"
#include "kudu/util/debug-util.h"
#include "kudu/util/init.h"
#include "kudu/util/logging.h"
#include "kudu/util/logging_callback.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/openssl_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/version_info.h"

using kudu::client::internal::AsyncLeaderMasterRpc;
using kudu::client::internal::MetaCache;
using kudu::client::sp::shared_ptr;
using kudu::consensus::RaftPeerPB;
using kudu::master::AlterTableRequestPB;
using kudu::master::AlterTableResponsePB;
using kudu::master::CreateTableRequestPB;
using kudu::master::CreateTableResponsePB;
using kudu::master::GetTableStatisticsRequestPB;
using kudu::master::GetTableStatisticsResponsePB;
using kudu::master::GetTabletLocationsRequestPB;
using kudu::master::GetTabletLocationsResponsePB;
using kudu::master::ListTabletServersRequestPB;
using kudu::master::ListTabletServersResponsePB;
using kudu::master::ListTabletServersResponsePB_Entry;
using kudu::master::MasterServiceProxy;
using kudu::master::TSInfoPB;
using kudu::master::TableIdentifierPB;
using kudu::master::TabletLocationsPB;
using kudu::rpc::BackoffType;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;
using kudu::rpc::UserCredentials;
using kudu::tserver::ScanResponsePB;
using std::map;
using std::make_optional;
using std::nullopt;
using std::optional;
using std::set;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;


MAKE_ENUM_LIMITS(kudu::client::KuduSession::FlushMode,
                 kudu::client::KuduSession::AUTO_FLUSH_SYNC,
                 kudu::client::KuduSession::MANUAL_FLUSH);

MAKE_ENUM_LIMITS(kudu::client::KuduSession::ExternalConsistencyMode,
                 kudu::client::KuduSession::CLIENT_PROPAGATED,
                 kudu::client::KuduSession::COMMIT_WAIT);

MAKE_ENUM_LIMITS(kudu::client::KuduScanner::ReadMode,
                 kudu::client::KuduScanner::READ_LATEST,
                 kudu::client::KuduScanner::READ_YOUR_WRITES);

MAKE_ENUM_LIMITS(kudu::client::KuduScanner::OrderMode,
                 kudu::client::KuduScanner::UNORDERED,
                 kudu::client::KuduScanner::ORDERED);

struct tm;

namespace kudu {

class BlockBloomFilter;
class simple_spinlock;

namespace client {

class ResourceMetrics;

const char* kVerboseEnvVar = "KUDU_CLIENT_VERBOSE";

#if defined(kudu_client_exported_EXPORTS)
static const char* kProgName = "kudu_client";

// We need to reroute all logging to stderr when the client library is
// loaded. GoogleOnceInit() can do that, but there are multiple entry
// points into the client code, and it'd need to be called in each one.
// So instead, let's use a constructor function.
//
// This is restricted to the exported client builds only. In case of linking
// with non-exported kudu client library, logging must be initialized
// from the main() function of the corresponding binary: usually, that's done
// by calling InitGoogleLoggingSafe(argv[0]).
__attribute__((constructor))
static void InitializeBasicLogging() {
  InitGoogleLoggingSafeBasic(kProgName);

  SetVerboseLevelFromEnvVar();
}
#endif

// Set Client logging verbose level from environment variable.
void SetVerboseLevelFromEnvVar() {
  int32_t level = 0; // this is the default logging level;
  const char* env_verbose_level = std::getenv(kVerboseEnvVar);
  if (env_verbose_level != nullptr) {
     if (safe_strto32(env_verbose_level, &level) && (level >= 0)) {
       SetVerboseLogLevel(level);
     } else {
       LOG(WARNING) << "Invalid verbose level from environment variable " << kVerboseEnvVar;
     }
  }
}

// Adapts between the internal LogSeverity and the client's KuduLogSeverity.
static void LoggingAdapterCB(KuduLoggingCallback* user_cb,
                             LogSeverity severity,
                             const char* filename,
                             int line_number,
                             const struct ::tm* time,
                             const char* message,
                             size_t message_len) {
  KuduLogSeverity client_severity;
  switch (severity) {
    case kudu::SEVERITY_INFO:
      client_severity = SEVERITY_INFO;
      break;
    case kudu::SEVERITY_WARNING:
      client_severity = SEVERITY_WARNING;
      break;
    case kudu::SEVERITY_ERROR:
      client_severity = SEVERITY_ERROR;
      break;
    case kudu::SEVERITY_FATAL:
      client_severity = SEVERITY_FATAL;
      break;
    default:
      LOG(FATAL) << "Unknown Kudu log severity: " << severity;
  }
  user_cb->Run(client_severity, filename, line_number, time,
               message, message_len);
}

void InstallLoggingCallback(KuduLoggingCallback* cb) {
  RegisterLoggingCallback(
      [=](LogSeverity severity, const char* filename, int line_number,
          const struct ::tm* time, const char* message, size_t message_len) {
        LoggingAdapterCB(cb, severity, filename, line_number, time, message, message_len);
      });
}

void UninstallLoggingCallback() {
  UnregisterLoggingCallback();
}

void SetVerboseLogLevel(int level) {
  FLAGS_v = level;
}

Status SetInternalSignalNumber(int signum) {
  return SetStackTraceSignal(signum);
}

Status DisableSaslInitialization() {
  return kudu::rpc::DisableSaslInitialization();
}

Status DisableOpenSSLInitialization() {
  return kudu::security::DisableOpenSSLInitialization();
}

string GetShortVersionString() {
  return VersionInfo::GetVersionInfo();
}

string GetAllVersionInfo() {
  return VersionInfo::GetAllVersionInfo();
}

KuduClientBuilder::KuduClientBuilder()
  : data_(new KuduClientBuilder::Data()) {
}

KuduClientBuilder::~KuduClientBuilder() {
  delete data_;
}

KuduClientBuilder& KuduClientBuilder::clear_master_server_addrs() {
  data_->master_server_addrs_.clear();
  return *this;
}

KuduClientBuilder& KuduClientBuilder::master_server_addrs(const vector<string>& addrs) {
  for (const string& addr : addrs) {
    data_->master_server_addrs_.push_back(addr);
  }
  return *this;
}

KuduClientBuilder& KuduClientBuilder::add_master_server_addr(const string& addr) {
  data_->master_server_addrs_.push_back(addr);
  return *this;
}

KuduClientBuilder& KuduClientBuilder::default_admin_operation_timeout(const MonoDelta& timeout) {
  data_->default_admin_operation_timeout_ = timeout;
  return *this;
}

KuduClientBuilder& KuduClientBuilder::default_rpc_timeout(const MonoDelta& timeout) {
  data_->default_rpc_timeout_ = timeout;
  return *this;
}

KuduClientBuilder& KuduClientBuilder::connection_negotiation_timeout(
    const MonoDelta& timeout) {
  data_->connection_negotiation_timeout_ = timeout;
  return *this;
}

KuduClientBuilder& KuduClientBuilder::import_authentication_credentials(string authn_creds) {
  data_->authn_creds_ = std::move(authn_creds);
  return *this;
}

KuduClientBuilder& KuduClientBuilder::num_reactors(int num_reactors) {
  data_->num_reactors_ = num_reactors;
  return *this;
}

KuduClientBuilder& KuduClientBuilder::sasl_protocol_name(const string& sasl_protocol_name) {
  data_->sasl_protocol_name_ = sasl_protocol_name;
  return *this;
}

KuduClientBuilder& KuduClientBuilder::encryption_policy(EncryptionPolicy encryption_policy) {
  data_->encryption_policy_ = encryption_policy;
  return *this;
}

KuduClientBuilder& KuduClientBuilder::require_authentication(bool require_authentication) {
  data_->require_authentication_ = require_authentication;
  return *this;
}

namespace {
Status ImportAuthnCreds(const string& authn_creds,
                        Messenger* messenger,
                        UserCredentials* user_credentials) {
  AuthenticationCredentialsPB pb;
  if (!pb.ParseFromString(authn_creds)) {
    return Status::InvalidArgument("invalid authentication data");
  }
  if (pb.has_authn_token()) {
    const auto& tok = pb.authn_token();
    if (!tok.has_token_data() ||
        !tok.has_signature() ||
        !tok.has_signing_key_seq_num()) {
      return Status::InvalidArgument("invalid authentication token");
    }
    messenger->set_authn_token(tok);
  }
  if (pb.has_jwt()) {
    messenger->set_jwt(pb.jwt());
  }
  if (pb.has_real_user()) {
    user_credentials->set_real_user(pb.real_user());
  }
  for (const string& cert_der : pb.ca_cert_ders()) {
    security::Cert cert;
    RETURN_NOT_OK_PREPEND(cert.FromString(cert_der, security::DataFormat::DER),
                          "could not import CA cert");
    RETURN_NOT_OK_PREPEND(messenger->mutable_tls_context()->AddTrustedCertificate(cert),
                          "could not trust CA cert");
  }
  return Status::OK();
}
} // anonymous namespace

Status KuduClientBuilder::Build(shared_ptr<KuduClient>* client) {
  RETURN_NOT_OK(CheckCPUFlags());

  // Init messenger.
  MessengerBuilder builder("client");
  if (data_->connection_negotiation_timeout_.Initialized()) {
    builder.set_rpc_negotiation_timeout_ms(
        data_->connection_negotiation_timeout_.ToMilliseconds());
  }
  if (data_->num_reactors_) {
    builder.set_num_reactors(*data_->num_reactors_);
  }
  if (!data_->sasl_protocol_name_.empty()) {
    builder.set_sasl_proto_name(data_->sasl_protocol_name_);
  }
  if (data_->require_authentication_) {
    builder.set_rpc_authentication("required");
  }
  if (data_->encryption_policy_ != OPTIONAL) {
    builder.set_rpc_encryption("required");
    if (data_->encryption_policy_ == REQUIRED) {
      builder.set_rpc_loopback_encryption(true);
    }
  }
  std::shared_ptr<Messenger> messenger;
  RETURN_NOT_OK(builder.Build(&messenger));
  UserCredentials user_credentials;

  // Parse and import the provided authn data, if any.
  if (!data_->authn_creds_.empty()) {
    RETURN_NOT_OK(ImportAuthnCreds(data_->authn_creds_, messenger.get(), &user_credentials));
  }
  if (!user_credentials.has_real_user()) {
    // If there are no authentication credentials, then set the real user to the
    // currently logged-in user.
    RETURN_NOT_OK(user_credentials.SetLoggedInRealUser());
  }

  shared_ptr<KuduClient> c(new KuduClient);
  c->data_->messenger_ = std::move(messenger);
  c->data_->user_credentials_ = std::move(user_credentials);
  c->data_->master_server_addrs_ = data_->master_server_addrs_;
  c->data_->default_admin_operation_timeout_ = data_->default_admin_operation_timeout_;
  c->data_->default_rpc_timeout_ = data_->default_rpc_timeout_;

  // Let's allow for plenty of time for discovering the master the first
  // time around.
  MonoTime deadline = MonoTime::Now() + c->default_admin_operation_timeout();
  RETURN_NOT_OK_PREPEND(c->data_->ConnectToCluster(c.get(), deadline),
                        "Could not connect to the cluster");

  c->data_->meta_cache_.reset(new MetaCache(c.get(), data_->replica_visibility_));

  // Init local host names used for locality decisions.
  RETURN_NOT_OK_PREPEND(c->data_->InitLocalHostNames(),
                        "Could not determine local host names");

  c->data_->request_tracker_ = new rpc::RequestTracker(c->data_->client_id_);

  client->swap(c);
  return Status::OK();
}

KuduTransaction::SerializationOptions::SerializationOptions()
    : data_(new Data) {
}

KuduTransaction::SerializationOptions::~SerializationOptions() {
  delete data_;
}

bool KuduTransaction::SerializationOptions::keepalive() const {
  return data_->enable_keepalive_;
}

KuduTransaction::SerializationOptions&
KuduTransaction::SerializationOptions::enable_keepalive(bool enable) {
  data_->enable_keepalive_ = enable;
  return *this;
}

KuduTransaction::KuduTransaction(const sp::shared_ptr<KuduClient>& client)
    : data_(new KuduTransaction::Data(client)) {
}

KuduTransaction::~KuduTransaction() {
  delete data_;
}

Status KuduTransaction::CreateSession(sp::shared_ptr<KuduSession>* session) {
  return data_->CreateSession(session);
}

Status KuduTransaction::Commit() {
  return data_->Commit(KuduTransaction::Data::CommitMode::WAIT_FOR_COMPLETION);
}

Status KuduTransaction::StartCommit() {
  return data_->Commit(KuduTransaction::Data::CommitMode::START_ONLY);
}

Status KuduTransaction::IsCommitComplete(
    bool* is_complete, Status* completion_status) {
  return data_->IsCommitComplete(is_complete, completion_status);
}

Status KuduTransaction::Rollback() {
  return data_->Rollback();
}

Status KuduTransaction::Serialize(
    string* serialized_txn,
    const SerializationOptions& options) const {
  return data_->Serialize(serialized_txn, options);
}

Status KuduTransaction::Deserialize(const sp::shared_ptr<KuduClient>& client,
                                    const string& serialized_txn,
                                    sp::shared_ptr<KuduTransaction>* txn) {
  return Data::Deserialize(client, serialized_txn, txn);
}

KuduClient::KuduClient()
  : data_(new KuduClient::Data()) {
  static ObjectIdGenerator oid_generator;
  data_->client_id_ = oid_generator.Next();
}

KuduClient::~KuduClient() {
  delete data_;
}

KuduTableCreator* KuduClient::NewTableCreator() {
  return new KuduTableCreator(this);
}

Status KuduClient::IsCreateTableInProgress(const string& table_name,
                                           bool* create_in_progress) {
  MonoTime deadline = MonoTime::Now() + default_admin_operation_timeout();
  TableIdentifierPB table;
  table.set_table_name(table_name);
  return data_->IsCreateTableInProgress(this, std::move(table), deadline,
                                        create_in_progress);
}

Status KuduClient::DeleteTable(const string& table_name) {
  // The default param 'reserve_seconds' not be set means the behavior of DeleteRPC is
  // controlled by the 'default_deleted_table_reserve_seconds' flag.
  return DeleteTableInCatalogs(table_name, true);
}

Status KuduClient::SoftDeleteTable(const string& table_name,
                                   uint32_t reserve_seconds) {
  return DeleteTableInCatalogs(table_name, true, reserve_seconds);
}

Status KuduClient::DeleteTableInCatalogs(const string& table_name,
                                         bool modify_external_catalogs,
                                         int32_t reserve_seconds) {
  MonoTime deadline = MonoTime::Now() + default_admin_operation_timeout();
  std::optional<uint32_t> reserve_time = reserve_seconds >= 0 ?
      std::make_optional(reserve_seconds) : std::nullopt;
  return  KuduClient::Data::DeleteTable(this, table_name, deadline, modify_external_catalogs,
                                        reserve_time);
}

Status KuduClient::RecallTable(const string& table_id, const string& new_table_name) {
  MonoTime deadline = MonoTime::Now() + default_admin_operation_timeout();
  return KuduClient::Data::RecallTable(this, table_id, deadline, new_table_name);
}

KuduTableAlterer* KuduClient::NewTableAlterer(const string& table_name) {
  return new KuduTableAlterer(this, table_name);
}

Status KuduClient::IsAlterTableInProgress(const string& table_name,
                                          bool* alter_in_progress) {
  MonoTime deadline = MonoTime::Now() + default_admin_operation_timeout();
  TableIdentifierPB table;
  table.set_table_name(table_name);
  return data_->IsAlterTableInProgress(this, std::move(table), deadline,
                                       alter_in_progress);
}

Status KuduClient::GetTableSchema(const string& table_name,
                                  KuduSchema* schema) {
  MonoTime deadline = MonoTime::Now() + default_admin_operation_timeout();
  TableIdentifierPB table;
  table.set_table_name(table_name);
  return data_->GetTableSchema(this,
                               deadline,
                               table,
                               schema,
                               nullptr, // partition schema
                               nullptr, // table id
                               nullptr, // table name
                               nullptr, // number of replicas
                               nullptr, // owner
                               nullptr, // comment
                               nullptr); // extra configs
}

Status KuduClient::ListTabletServers(vector<KuduTabletServer*>* tablet_servers) {
  ListTabletServersRequestPB req;
  ListTabletServersResponsePB resp;
  MonoTime deadline = MonoTime::Now() + default_admin_operation_timeout();
  RETURN_NOT_OK(data_->ListTabletServers(this, deadline, req, &resp));
  for (int i = 0; i < resp.servers_size(); i++) {
    const ListTabletServersResponsePB_Entry& e = resp.servers(i);
    HostPort hp = HostPortFromPB(e.registration().rpc_addresses(0));
    unique_ptr<KuduTabletServer> ts(new KuduTabletServer);
    ts->data_ = new KuduTabletServer::Data(e.instance_id().permanent_uuid(), hp, e.location());
    tablet_servers->push_back(ts.release());
  }
  return Status::OK();
}

Status KuduClient::ListTables(vector<string>* tables, const string& filter) {
  vector<Data::TableInfo> tables_info;
  RETURN_NOT_OK(data_->ListTablesWithInfo(this, &tables_info, filter, false));
  tables->clear();
  tables->reserve(tables_info.size());
  for (auto& info : tables_info) {
    tables->emplace_back(std::move(info.table_name));
  }
  return Status::OK();
}

Status KuduClient::ListSoftDeletedTables(vector<string>* tables, const string& filter) {
  vector<Data::TableInfo> tables_info;
  RETURN_NOT_OK(data_->ListTablesWithInfo(this, &tables_info, filter,
      /*list_tablet_with_partition=*/ true, /*show_soft_deleted=*/ true));
  tables->clear();
  tables->reserve(tables_info.size());
  for (auto& info : tables_info) {
    tables->emplace_back(std::move(info.table_name));
  }
  return Status::OK();
}

Status KuduClient::TableExists(const string& table_name, bool* exists) {
  auto s = GetTableSchema(table_name, nullptr);
  if (s.ok()) {
    *exists = true;
  } else if (s.IsNotFound()) {
    *exists = false;
    s = Status::OK();
  }
  return s;
}

Status KuduClient::OpenTable(const string& table_name,
                             shared_ptr<KuduTable>* table) {
  TableIdentifierPB table_identifier;
  table_identifier.set_table_name(table_name);
  return data_->OpenTable(this,
                          table_identifier,
                          table);
}

shared_ptr<KuduSession> KuduClient::NewSession() {
  shared_ptr<KuduSession> ret(new KuduSession(shared_from_this()));
  ret->data_->Init(ret);
  return ret;
}

Status KuduClient::NewTransaction(sp::shared_ptr<KuduTransaction>* txn) {
  shared_ptr<KuduTransaction> ret(new KuduTransaction(shared_from_this()));
  const auto s = ret->data_->Begin(ret);
  if (s.ok()) {
    *txn = std::move(ret);
  }
  return s;
}

Status KuduClient::GetTablet(const string& tablet_id, KuduTablet** tablet) {
  GetTabletLocationsRequestPB req;
  GetTabletLocationsResponsePB resp;

  req.add_tablet_ids(tablet_id);
  req.set_intern_ts_infos_in_response(true);
  MonoTime deadline = MonoTime::Now() + default_admin_operation_timeout();
  Synchronizer sync;
  AsyncLeaderMasterRpc<GetTabletLocationsRequestPB, GetTabletLocationsResponsePB> rpc(
      deadline, this, BackoffType::EXPONENTIAL, req, &resp,
      &MasterServiceProxy::GetTabletLocationsAsync, "GetTabletLocations",
      sync.AsStatusCallback(), {});
  rpc.SendRpc();
  RETURN_NOT_OK(sync.Wait());
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  if (resp.tablet_locations_size() == 0) {
    return Status::NotFound(Substitute("$0: tablet not found", tablet_id));
  }
  if (resp.tablet_locations_size() != 1) {
    return Status::IllegalState(Substitute(
        "Expected only one tablet, but received $0",
        resp.tablet_locations_size()));
  }
  const TabletLocationsPB& t = resp.tablet_locations(0);

  vector<const KuduReplica*> replicas;
  ElementDeleter deleter(&replicas);

  auto add_replica_func = [](const TSInfoPB& ts_info,
                             const RaftPeerPB::Role role,
                             vector<const KuduReplica*>* replicas) {
    if (ts_info.rpc_addresses_size() == 0) {
      return Status::IllegalState(Substitute(
          "No RPC addresses found for tserver $0",
          ts_info.permanent_uuid()));
    }
    HostPort hp = HostPortFromPB(ts_info.rpc_addresses(0));
    unique_ptr<KuduTabletServer> ts(new KuduTabletServer);
    ts->data_ = new KuduTabletServer::Data(ts_info.permanent_uuid(), hp, ts_info.location());

    // TODO(aserbin): try to use member_type instead of role for metacache.
    bool is_leader = role == RaftPeerPB::LEADER;
    bool is_voter = is_leader || role == RaftPeerPB::FOLLOWER;
    unique_ptr<KuduReplica> replica(new KuduReplica);
    replica->data_ = new KuduReplica::Data(is_leader, is_voter, std::move(ts));

    replicas->push_back(replica.release());
    return Status::OK();
  };

  // Handle "old-style" non-interned replicas. It's used for backward compatibility.
  for (const auto& r : t.deprecated_replicas()) {
    RETURN_NOT_OK(add_replica_func(r.ts_info(), r.role(), &replicas));
  }
  // Handle interned replicas.
  for (const auto& r : t.interned_replicas()) {
    RETURN_NOT_OK(add_replica_func(resp.ts_infos(r.ts_info_idx()), r.role(), &replicas));
  }

  unique_ptr<KuduTablet> client_tablet(new KuduTablet);
  client_tablet->data_ = new KuduTablet::Data(tablet_id, std::move(replicas),
                                              t.table_id(),
                                              t.table_name());
  replicas.clear();

  *tablet = client_tablet.release();
  return Status::OK();
}

Status KuduClient::GetTableStatistics(const string& table_name,
                                      KuduTableStatistics** statistics) {
  GetTableStatisticsRequestPB req;
  GetTableStatisticsResponsePB resp;
  TableIdentifierPB* table = req.mutable_table();
  table->set_table_name(table_name);
  MonoTime deadline = MonoTime::Now() + default_admin_operation_timeout();
  Synchronizer sync;
  AsyncLeaderMasterRpc<GetTableStatisticsRequestPB, GetTableStatisticsResponsePB> rpc(
      deadline, this, BackoffType::EXPONENTIAL, req, &resp,
      &MasterServiceProxy::GetTableStatisticsAsync, "GetTableStatistics",
      sync.AsStatusCallback(), {});
  rpc.SendRpc();
  RETURN_NOT_OK(sync.Wait());
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  unique_ptr<KuduTableStatistics> table_statistics(new KuduTableStatistics);
  table_statistics->data_ = new KuduTableStatistics::Data(
      resp.has_on_disk_size() ? optional<int64_t>(resp.on_disk_size()) : nullopt,
      resp.has_live_row_count() ? optional<int64_t>(resp.live_row_count()) : nullopt,
      resp.has_disk_size_limit() ? optional<int64_t>(resp.disk_size_limit()) : nullopt,
      resp.has_row_count_limit() ? optional<int64_t>(resp.row_count_limit()) : nullopt);

  *statistics = table_statistics.release();
  return Status::OK();
}

string KuduClient::GetMasterAddresses() const {
  return HostPort::ToCommaSeparatedString(data_->master_hostports());
}

bool KuduClient::IsMultiMaster() const {
  return data_->master_server_addrs_.size() > 1;
}

const MonoDelta& KuduClient::default_admin_operation_timeout() const {
  return data_->default_admin_operation_timeout_;
}

const MonoDelta& KuduClient::default_rpc_timeout() const {
  return data_->default_rpc_timeout_;
}

MonoDelta KuduClient::connection_negotiation_timeout() const {
  DCHECK(data_->messenger_);
  return MonoDelta::FromMilliseconds(
      data_->messenger_->rpc_negotiation_timeout_ms());
}

const uint64_t KuduClient::kNoTimestamp = 0;

uint64_t KuduClient::GetLatestObservedTimestamp() const {
  return data_->GetLatestObservedTimestamp();
}

void KuduClient::SetLatestObservedTimestamp(uint64_t ht_timestamp) {
  data_->UpdateLatestObservedTimestamp(ht_timestamp);
}

Status KuduClient::ExportAuthenticationCredentials(string* authn_creds) const {
  AuthenticationCredentialsPB pb;

  if (auto tok = data_->messenger_->authn_token(); tok) {
    pb.mutable_authn_token()->CopyFrom(*tok);
  }
  auto jwt = data_->messenger_->jwt();
  if (jwt) {
    pb.mutable_jwt()->CopyFrom(*jwt);
  }
  pb.set_real_user(data_->user_credentials_.real_user());

  vector<string> cert_ders;
  RETURN_NOT_OK_PREPEND(data_->messenger_->tls_context().DumpTrustedCerts(&cert_ders),
                        "could not export trusted certs");
  for (auto& der : cert_ders) {
    pb.add_ca_cert_ders()->assign(std::move(der));
  }

  if (!pb.SerializeToString(authn_creds)) {
    return Status::RuntimeError("could not serialize authentication data");
  }

  return Status::OK();
}

string KuduClient::GetHiveMetastoreUris() const {
  std::lock_guard<simple_spinlock> l(data_->leader_master_lock_);
  return data_->hive_metastore_uris_;
}

bool KuduClient::GetHiveMetastoreSaslEnabled() const {
  std::lock_guard<simple_spinlock> l(data_->leader_master_lock_);
  return data_->hive_metastore_sasl_enabled_;
}

string KuduClient::GetHiveMetastoreUuid() const {
  std::lock_guard<simple_spinlock> l(data_->leader_master_lock_);
  return data_->hive_metastore_uuid_;
}

string KuduClient::location() const {
  return data_->location();
}

string KuduClient::cluster_id() const {
  return data_->cluster_id();
}

////////////////////////////////////////////////////////////
// KuduTableCreator
////////////////////////////////////////////////////////////

KuduTableCreator::KuduTableCreator(KuduClient* client)
  : data_(new KuduTableCreator::Data(client)) {
}

KuduTableCreator::~KuduTableCreator() {
  delete data_;
}

KuduTableCreator& KuduTableCreator::table_name(const string& name) {
  data_->table_name_ = name;
  return *this;
}

KuduTableCreator& KuduTableCreator::schema(const KuduSchema* schema) {
  data_->schema_ = schema;
  return *this;
}

KuduTableCreator& KuduTableCreator::add_hash_partitions(const vector<string>& columns,
                                                        int32_t num_buckets) {
  return add_hash_partitions(columns, num_buckets, 0);
}

KuduTableCreator& KuduTableCreator::add_hash_partitions(const vector<string>& columns,
                                                        int32_t num_buckets,
                                                        int32_t seed) {
  auto* hash_dimension = data_->partition_schema_.add_hash_schema();
  for (const string& col_name : columns) {
    hash_dimension->add_columns()->set_name(col_name);
  }
  hash_dimension->set_num_buckets(num_buckets);
  hash_dimension->set_seed(seed);
  return *this;
}

KuduTableCreator& KuduTableCreator::set_range_partition_columns(const vector<string>& columns) {
  PartitionSchemaPB::RangeSchemaPB* range_schema =
    data_->partition_schema_.mutable_range_schema();
  range_schema->Clear();
  for (const string& col_name : columns) {
    range_schema->add_columns()->set_name(col_name);
  }

  return *this;
}

KuduTableCreator& KuduTableCreator::add_range_partition_split(KuduPartialRow* split_row) {
  data_->range_partition_splits_.emplace_back(split_row);
  return *this;
}

KuduTableCreator& KuduTableCreator::set_owner(const string& owner) {
  data_->owner_ = owner;
  return *this;
}

KuduTableCreator& KuduTableCreator::set_comment(const string& comment) {
  data_->comment_ = comment;
  return *this;
}

KuduTableCreator& KuduTableCreator::split_rows(const vector<const KuduPartialRow*>& rows) {
  for (const KuduPartialRow* row : rows) {
    data_->range_partition_splits_.emplace_back(const_cast<KuduPartialRow*>(row));
  }
  return *this;
}

KuduTableCreator& KuduTableCreator::add_range_partition(
    KuduPartialRow* lower_bound,
    KuduPartialRow* upper_bound,
    RangePartitionBound lower_bound_type,
    RangePartitionBound upper_bound_type) {
  unique_ptr<KuduRangePartition> range_partition(new KuduRangePartition(
      lower_bound, upper_bound, lower_bound_type, upper_bound_type));
  // Using KuduTableCreator::add_range_partition() assumes the range partition
  // uses the table-wide schema.
  range_partition->data_->is_table_wide_hash_schema_ = true;
  data_->range_partitions_.emplace_back(std::move(range_partition));
  return *this;
}

KuduTableCreator& KuduTableCreator::add_custom_range_partition(
    KuduRangePartition* partition) {
  CHECK(partition);
  data_->range_partitions_.emplace_back(partition);
  return *this;
}

KuduTableCreator& KuduTableCreator::num_replicas(int num_replicas) {
  data_->num_replicas_ = num_replicas;
  return *this;
}

KuduTableCreator& KuduTableCreator::dimension_label(const std::string& dimension_label) {
  data_->dimension_label_ = dimension_label;
  return *this;
}

KuduTableCreator& KuduTableCreator::extra_configs(const map<string, string>& extra_configs) {
  data_->extra_configs_ = extra_configs;
  return *this;
}

KuduTableCreator& KuduTableCreator::timeout(const MonoDelta& timeout) {
  data_->timeout_ = timeout;
  return *this;
}

KuduTableCreator& KuduTableCreator::wait(bool wait) {
  data_->wait_ = wait;
  return *this;
}

Status KuduTableCreator::Create() {
  if (!data_->table_name_.length()) {
    return Status::InvalidArgument("Missing table name");
  }
  if (!data_->schema_) {
    return Status::InvalidArgument("Missing schema");
  }
  if (!data_->partition_schema_.has_range_schema() &&
      data_->partition_schema_.hash_schema().empty()) {
    return Status::InvalidArgument(
        "Table partitioning must be specified using "
        "add_hash_partitions or set_range_partition_columns");
  }

  // Build request.
  CreateTableRequestPB req;
  req.set_name(data_->table_name_);
  if (data_->num_replicas_) {
    req.set_num_replicas(*data_->num_replicas_);
  }
  if (data_->dimension_label_) {
    req.set_dimension_label(*data_->dimension_label_);
  }
  if (data_->extra_configs_) {
    req.mutable_extra_configs()->insert(data_->extra_configs_->begin(),
                                        data_->extra_configs_->end());
  }
  if (data_->owner_) {
    req.set_owner(*data_->owner_);
  }
  if (data_->comment_) {
    req.set_comment(*data_->comment_);
  }
  RETURN_NOT_OK_PREPEND(SchemaToPB(*data_->schema_->schema_, req.mutable_schema(),
                                   SCHEMA_PB_WITHOUT_WRITE_DEFAULT),
                        "Invalid schema");

  bool has_range_splits = false;
  RowOperationsPBEncoder splits_encoder(req.mutable_split_rows_range_bounds());
  for (const auto& row : data_->range_partition_splits_) {
    if (!row) {
      return Status::InvalidArgument("range split row must not be null");
    }
    splits_encoder.Add(RowOperationsPB::SPLIT_ROW, *row);
    has_range_splits = true;
  }

  bool has_range_with_custom_hash_schema = false;
  for (const auto& p : data_->range_partitions_) {
    if (!p->data_->is_table_wide_hash_schema_) {
      has_range_with_custom_hash_schema = true;
      break;
    }
  }

  if (has_range_splits && has_range_with_custom_hash_schema) {
    // For simplicity, don't allow having both range splits (deprecated) and
    // custom hash bucket schemas per range partition.
    return Status::InvalidArgument(
        "split rows and custom hash bucket schemas for ranges are incompatible: "
        "choose one or the other");
  }

  auto* partition_schema = req.mutable_partition_schema();
  partition_schema->CopyFrom(data_->partition_schema_);

  for (const auto& p : data_->range_partitions_) {
    const auto* range = p->data_;
    if (!range->lower_bound_ || !range->upper_bound_) {
      return Status::InvalidArgument("range bounds must not be null");
    }

    const RowOperationsPB_Type lower_bound_type =
        range->lower_bound_type_ == KuduTableCreator::INCLUSIVE_BOUND
        ? RowOperationsPB::RANGE_LOWER_BOUND
        : RowOperationsPB::EXCLUSIVE_RANGE_LOWER_BOUND;

    const RowOperationsPB_Type upper_bound_type =
        range->upper_bound_type_ == KuduTableCreator::EXCLUSIVE_BOUND
        ? RowOperationsPB::RANGE_UPPER_BOUND
        : RowOperationsPB::INCLUSIVE_RANGE_UPPER_BOUND;

    if (!has_range_with_custom_hash_schema) {
      splits_encoder.Add(lower_bound_type, *range->lower_bound_);
      splits_encoder.Add(upper_bound_type, *range->upper_bound_);
    } else {
      auto* range_pb = partition_schema->add_custom_hash_schema_ranges();
      RowOperationsPBEncoder encoder(range_pb->mutable_range_bounds());
      encoder.Add(lower_bound_type, *range->lower_bound_);
      encoder.Add(upper_bound_type, *range->upper_bound_);
      // Now, after adding the information range bounds, add the information
      // on hash schema for the range.
      if (range->is_table_wide_hash_schema_) {
        // With the presence of a range with custom hash schema when the
        // table-wide hash schema is used for this particular range, also add an
        // element into PartitionSchemaPB::custom_hash_schema_ranges to satisfy
        // the convention used by the backend.
        range_pb->mutable_hash_schema()->CopyFrom(
            data_->partition_schema_.hash_schema());
      } else {
        // In case of per-range custom hash bucket schema, add corresponding
        // element into PartitionSchemaPB::custom_hash_schema_ranges.
        for (const auto& hash_dimension : range->hash_schema_) {
          auto* hash_dimension_pb = range_pb->add_hash_schema();
          hash_dimension_pb->set_seed(hash_dimension.seed);
          hash_dimension_pb->set_num_buckets(hash_dimension.num_buckets);
          for (const auto& column_name : hash_dimension.column_names) {
            hash_dimension_pb->add_columns()->set_name(column_name);
          }
        }
      }
    }
  }

  bool has_immutable_column_schema = false;
  for (size_t i = 0; i < data_->schema_->num_columns(); i++) {
    const auto& col_schema = data_->schema_->Column(i);
    if (col_schema.is_immutable()) {
      has_immutable_column_schema = true;
      break;
    }
  }

  if (data_->table_type_) {
    req.set_table_type(*data_->table_type_);
  }

  MonoTime deadline = MonoTime::Now();
  if (data_->timeout_.Initialized()) {
    deadline += data_->timeout_;
  } else {
    deadline += data_->client_->default_admin_operation_timeout();
  }

  CreateTableResponsePB resp;
  RETURN_NOT_OK_PREPEND(
      data_->client_->data_->CreateTable(data_->client_,
                                         req,
                                         &resp,
                                         deadline,
                                         !data_->range_partitions_.empty(),
                                         has_range_with_custom_hash_schema,
                                         has_immutable_column_schema),
      Substitute("Error creating table $0 on the master", data_->table_name_));
  // Spin until the table is fully created, if requested.
  if (data_->wait_) {
    TableIdentifierPB table;
    table.set_table_id(resp.table_id());
    RETURN_NOT_OK(data_->client_->data_->WaitForCreateTableToFinish(
        data_->client_, table, deadline));
  }

  return Status::OK();
}

KuduRangePartition::KuduRangePartition(
    KuduPartialRow* lower_bound,
    KuduPartialRow* upper_bound,
    KuduTableCreator::RangePartitionBound lower_bound_type,
    KuduTableCreator::RangePartitionBound upper_bound_type)
    : data_(new Data(lower_bound, upper_bound, lower_bound_type, upper_bound_type)) {
}

KuduRangePartition::~KuduRangePartition() {
  delete data_;
}

Status KuduRangePartition::add_hash_partitions(
    const vector<string>& columns,
    int32_t num_buckets,
    int32_t seed) {
  if (seed < 0) {
    // int32_t, not uint32_t for seed is used to be "compatible" with the type
    // of the 'seed' parameter for KuduTableCreator::add_hash_partitions().
    return Status::InvalidArgument("hash seed must be non-negative");
  }
  return data_->add_hash_partitions(columns, num_buckets, seed);
}

////////////////////////////////////////////////////////////
// KuduTableStatistics
////////////////////////////////////////////////////////////

KuduTableStatistics::KuduTableStatistics() : data_(nullptr) {
}

KuduTableStatistics::~KuduTableStatistics() {
  delete data_;
}

int64_t KuduTableStatistics::on_disk_size() const {
  return data_->on_disk_size_ ? *data_->on_disk_size_ : -1;
}

int64_t KuduTableStatistics::live_row_count() const {
  return data_->live_row_count_ ? *data_->live_row_count_ : -1;
}

int64_t KuduTableStatistics::on_disk_size_limit() const {
  return data_->on_disk_size_limit_ ? *data_->on_disk_size_limit_ : -1;
}

int64_t KuduTableStatistics::live_row_count_limit() const {
  return data_->live_row_count_limit_ ? *data_->live_row_count_limit_ : -1;
}

std::string KuduTableStatistics::ToString() const {
  return data_->ToString();
}

////////////////////////////////////////////////////////////
// KuduTable
////////////////////////////////////////////////////////////

KuduTable::KuduTable(const shared_ptr<KuduClient>& client,
                     const string& name,
                     const string& id,
                     int num_replicas,
                     const string& owner,
                     const string& comment,
                     const KuduSchema& schema,
                     const PartitionSchema& partition_schema,
                     const map<string, string>& extra_configs)
  : data_(new KuduTable::Data(client, name, id, num_replicas, owner, comment,
                              schema, partition_schema, extra_configs)) {
}

KuduTable::~KuduTable() {
  delete data_;
}

const string& KuduTable::name() const {
  return data_->name_;
}

const string& KuduTable::id() const {
  return data_->id_;
}

const KuduSchema& KuduTable::schema() const {
  return data_->schema_;
}

const string& KuduTable::comment() const {
  return data_->comment_;
}

int KuduTable::num_replicas() const {
  return data_->num_replicas_;
}

const string& KuduTable::owner() const {
  return data_->owner_;
}

KuduInsert* KuduTable::NewInsert() {
  return new KuduInsert(shared_from_this());
}

KuduInsertIgnore* KuduTable::NewInsertIgnore() {
  return new KuduInsertIgnore(shared_from_this());
}

KuduUpsert* KuduTable::NewUpsert() {
  return new KuduUpsert(shared_from_this());
}

KuduUpsertIgnore* KuduTable::NewUpsertIgnore() {
  return new KuduUpsertIgnore(shared_from_this());
}

KuduUpdate* KuduTable::NewUpdate() {
  return new KuduUpdate(shared_from_this());
}

KuduUpdateIgnore* KuduTable::NewUpdateIgnore() {
  return new KuduUpdateIgnore(shared_from_this());
}

KuduDelete* KuduTable::NewDelete() {
  return new KuduDelete(shared_from_this());
}

KuduDeleteIgnore* KuduTable::NewDeleteIgnore() {
  return new KuduDeleteIgnore(shared_from_this());
}

KuduClient* KuduTable::client() const {
  return data_->client_.get();
}

const PartitionSchema& KuduTable::partition_schema() const {
  return data_->partition_schema_;
}

const map<string, string>& KuduTable::extra_configs() const {
  return data_->extra_configs_;
}

KuduPredicate* KuduTable::NewComparisonPredicate(const Slice& col_name,
                                                 KuduPredicate::ComparisonOp op,
                                                 KuduValue* value) {
  // We always take ownership of value; this ensures cleanup if the predicate is invalid.
  auto cleanup = MakeScopedCleanup([&]() {
    delete value;
  });
  return data_->MakePredicate(col_name, [&](const ColumnSchema& col_schema) {
    // Ownership of value is passed to the valid returned predicate.
    cleanup.cancel();
    return new KuduPredicate(new ComparisonPredicateData(col_schema, op, value));
  });
}

KuduPredicate* KuduTable::NewInBloomFilterPredicate(const Slice& col_name,
                                                    vector<KuduBloomFilter*>* bloom_filters) {
  // We always take ownership of values; this ensures cleanup if the predicate is invalid.
  auto cleanup = MakeScopedCleanup([&]() {
    STLDeleteElements(bloom_filters);
  });

  // Empty vector of bloom filters will select all rows. Hence disallowed.
  if (bloom_filters->empty()) {
    return new KuduPredicate(
        new ErrorPredicateData(Status::InvalidArgument("No Bloom filters supplied")));
  }

  // Transfer the Bloom filter raw ptrs over to vector of unique ptrs.
  // There is a possibility of emplace_back() throwing exception, so in such a case the
  // transferred Bloom filters in unique_ptrs will be cleaned-up on exiting scope
  // automatically and the non-nullptr Bloom filters in input "bloom_filters" vector
  // will be cleaned up by the explicit scoped "cleanup".
  vector<unique_ptr<KuduBloomFilter>> bloom_filters_owned;
  bloom_filters_owned.reserve(bloom_filters->size());
  for (auto& bf : *bloom_filters) {
    bloom_filters_owned.emplace_back(bf);
    bf = nullptr;
  }

  return data_->MakePredicate(col_name, [&](const ColumnSchema& col_schema) {
    // At this point we could cancel the scoped "cleanup". But the scoped cleanup
    // not only deletes pointers contained in the vector but also clears the vector
    // and we want the vector be cleared as expected by the caller.
    return new KuduPredicate(
        new InBloomFilterPredicateData(col_schema, std::move(bloom_filters_owned)));
  });
}

KuduPredicate* KuduTable::NewInBloomFilterPredicate(const Slice& col_name,
                                                    const vector<Slice>& bloom_filters) {
  // Empty vector of bloom filters will select all rows. Hence disallowed.
  if (bloom_filters.empty()) {
    return new KuduPredicate(
        new ErrorPredicateData(Status::InvalidArgument("No Bloom filters supplied")));
  }

  // Extract the Block Bloom filters.
  vector<DirectBlockBloomFilterUniqPtr> bbf_vec;
  for (const auto& bf_slice : bloom_filters) {
    auto* bbf =
        reinterpret_cast<BlockBloomFilter*>(const_cast<uint8_t*>(bf_slice.data()));
    // In this case, the Block Bloom filters are supplied as opaque pointers
    // and the predicate will convert them to well-defined pointer types
    // but will NOT take ownership of those pointers. Hence a custom deleter,
    // DirectBloomFilterDataDeleter, is used that gives control over ownership.
    DirectBlockBloomFilterUniqPtr bf_uniq_ptr(
        bbf, DirectBloomFilterDataDeleter<BlockBloomFilter>(false /*owned*/));
    bbf_vec.emplace_back(std::move(bf_uniq_ptr));
  }

  return data_->MakePredicate(col_name, [&](const ColumnSchema& col_schema) {
    return new KuduPredicate(
        new InDirectBloomFilterPredicateData(col_schema, std::move(bbf_vec)));
  });
}

KuduPredicate* KuduTable::NewInListPredicate(const Slice& col_name,
                                             vector<KuduValue*>* values) {
  // We always take ownership of values; this ensures cleanup if the predicate is invalid.
  auto cleanup = MakeScopedCleanup([&]() {
    STLDeleteElements(values);
  });
  return data_->MakePredicate(col_name, [&](const ColumnSchema& col_schema) {
    // Ownership of values is passed to the valid returned predicate.
    cleanup.cancel();
    return new KuduPredicate(new InListPredicateData(col_schema, values));
  });
}

KuduPredicate* KuduTable::NewIsNotNullPredicate(const Slice& col_name) {
  return data_->MakePredicate(col_name, [&](const ColumnSchema& col_schema) {
    return new KuduPredicate(new IsNotNullPredicateData(col_schema));
  });
}

KuduPredicate* KuduTable::NewIsNullPredicate(const Slice& col_name) {
  return data_->MakePredicate(col_name, [&](const ColumnSchema& col_schema) {
    return new KuduPredicate(new IsNullPredicateData(col_schema));
  });
}

// The strategy for retrieving the partitions from the metacache is adapted
// from KuduScanTokenBuilder::Data::Build.
Status KuduTable::ListPartitions(vector<Partition>* partitions) {
  DCHECK(partitions);
  partitions->clear();
  auto& client = data_->client_;
  const auto deadline = MonoTime::Now() + client->default_admin_operation_timeout();
  PartitionPruner pruner;
  pruner.Init(*data_->schema_.schema_, data_->partition_schema_, ScanSpec());
  while (pruner.HasMorePartitionKeyRanges()) {
    scoped_refptr<client::internal::RemoteTablet> tablet;
    Synchronizer sync;
    const auto& partition_key = pruner.NextPartitionKey();
    client->data_->meta_cache_->LookupTabletByKey(
        this,
        partition_key,
        deadline,
        client::internal::MetaCache::LookupType::kLowerBound,
        &tablet,
        sync.AsStatusCallback());
    Status s = sync.Wait();
    if (s.IsNotFound()) {
      // No more tablets.
      break;
    }
    RETURN_NOT_OK(s);

    partitions->emplace_back(tablet->partition());
    pruner.RemovePartitionKeyRange(tablet->partition().end());
  }
  return Status::OK();
}

////////////////////////////////////////////////////////////
// Error
////////////////////////////////////////////////////////////

const Status& KuduError::status() const {
  return data_->status_;
}

const KuduWriteOperation& KuduError::failed_op() const {
  return *data_->failed_op_;
}

KuduWriteOperation* KuduError::release_failed_op() {
  CHECK_NOTNULL(data_->failed_op_.get());
  return data_->failed_op_.release();
}

bool KuduError::was_possibly_successful() const {
  // TODO: implement me - right now be conservative.
  return true;
}

KuduError::KuduError(KuduWriteOperation* failed_op,
                     const Status& status)
  : data_(new KuduError::Data(unique_ptr<KuduWriteOperation>(failed_op),
                              status)) {
}

KuduError::~KuduError() {
  delete data_;
}

////////////////////////////////////////////////////////////
// KuduSession
////////////////////////////////////////////////////////////

KuduSession::KuduSession(const shared_ptr<KuduClient>& client)
    : data_(new KuduSession::Data(client, client->data_->messenger_)) {
}

KuduSession::KuduSession(const shared_ptr<KuduClient>& client, const TxnId& txn_id)
    : data_(new KuduSession::Data(client, client->data_->messenger_, txn_id)) {
}

KuduSession::~KuduSession() {
  WARN_NOT_OK(data_->Close(true), "Closed Session with pending operations.");
  delete data_;
}

Status KuduSession::SetFlushMode(FlushMode m) {
  if (!tight_enum_test<FlushMode>(m)) {
    // Be paranoid in client code.
    return Status::InvalidArgument("Bad flush mode");
  }
  return data_->SetFlushMode(m);
}

Status KuduSession::SetExternalConsistencyMode(ExternalConsistencyMode m) {
  if (!tight_enum_test<ExternalConsistencyMode>(m)) {
    // Be paranoid in client code.
    return Status::InvalidArgument("Bad external consistency mode");
  }
  return data_->SetExternalConsistencyMode(m);
}

Status KuduSession::SetMutationBufferSpace(size_t size) {
  return data_->SetBufferBytesLimit(size);
}

Status KuduSession::SetMutationBufferFlushWatermark(double watermark_pct) {
  return data_->SetBufferFlushWatermark(
      static_cast<int32_t>(100.0 * watermark_pct));
}

Status KuduSession::SetMutationBufferFlushInterval(unsigned int millis) {
  return data_->SetBufferFlushInterval(millis);
}

Status KuduSession::SetMutationBufferMaxNum(unsigned int max_num) {
  return data_->SetMaxBatchersNum(max_num);
}

void KuduSession::SetTimeoutMillis(int timeout_ms) {
  data_->SetTimeoutMillis(timeout_ms);
}

Status KuduSession::Apply(KuduWriteOperation* write_op) {
  RETURN_NOT_OK(data_->ApplyWriteOp(write_op));
  // Thread-safety note: this method should not be called concurrently
  // with other methods which modify the KuduSession::Data members, so it
  // should be safe to read KuduSession::Data members without protection.
  if (data_->flush_mode_ == AUTO_FLUSH_SYNC) {
    RETURN_NOT_OK(data_->Flush());
  }
  return Status::OK();
}

Status KuduSession::Flush() {
  return data_->Flush();
}

void KuduSession::FlushAsync(KuduStatusCallback* user_callback) {
  data_->FlushAsync(user_callback);
}

Status KuduSession::Close() {
  return data_->Close(false);
}

bool KuduSession::HasPendingOperations() const {
  return data_->HasPendingOperations();
}

int KuduSession::CountBufferedOperations() const {
  return data_->CountBufferedOperations();
}

Status KuduSession::SetErrorBufferSpace(size_t size_bytes) {
  return data_->error_collector_->SetMaxMemSize(size_bytes);
}

int KuduSession::CountPendingErrors() const {
  return data_->error_collector_->CountErrors();
}

void KuduSession::GetPendingErrors(vector<KuduError*>* errors, bool* overflowed) {
  data_->error_collector_->GetErrors(errors, overflowed);
}

KuduClient* KuduSession::client() const {
  return data_->client_.get();
}

const ResourceMetrics& KuduSession::GetWriteOpMetrics() const {
  return data_->write_op_metrics_;
}

////////////////////////////////////////////////////////////
// KuduTableAlterer
////////////////////////////////////////////////////////////
KuduTableAlterer::KuduTableAlterer(KuduClient* client, const string& name)
  : data_(new Data(client, name)) {
}

KuduTableAlterer::~KuduTableAlterer() {
  delete data_;
}

KuduTableAlterer* KuduTableAlterer::RenameTo(const string& new_name) {
  data_->rename_to_ = new_name;
  return this;
}

KuduTableAlterer* KuduTableAlterer::SetOwner(const string& new_owner) {
  data_->set_owner_to_ = new_owner;
  return this;
}

KuduTableAlterer* KuduTableAlterer::SetComment(const string& new_comment) {
  data_->set_comment_to_ = new_comment;
  return this;
}

KuduColumnSpec* KuduTableAlterer::AddColumn(const string& name) {
  Data::Step s = { AlterTableRequestPB::ADD_COLUMN,
                   new KuduColumnSpec(name), nullptr };
  auto* spec = s.spec;
  data_->steps_.emplace_back(std::move(s));
  return spec;
}

KuduColumnSpec* KuduTableAlterer::AlterColumn(const string& name) {
  Data::Step s = { AlterTableRequestPB::ALTER_COLUMN,
                   new KuduColumnSpec(name), nullptr };
  auto* spec = s.spec;
  data_->steps_.emplace_back(std::move(s));
  return spec;
}

KuduTableAlterer* KuduTableAlterer::DropColumn(const string& name) {
  Data::Step s = { AlterTableRequestPB::DROP_COLUMN,
                   new KuduColumnSpec(name), nullptr };
  data_->steps_.emplace_back(std::move(s));
  return this;
}

KuduTableAlterer* KuduTableAlterer::AddRangePartition(
    KuduPartialRow* lower_bound,
    KuduPartialRow* upper_bound,
    KuduTableCreator::RangePartitionBound lower_bound_type,
    KuduTableCreator::RangePartitionBound upper_bound_type) {
  return AddRangePartitionWithDimension(
      lower_bound, upper_bound, "", lower_bound_type, upper_bound_type);
}

KuduTableAlterer* KuduTableAlterer::AddRangePartitionWithDimension(
    KuduPartialRow* lower_bound,
    KuduPartialRow* upper_bound,
    const std::string& dimension_label,
    KuduTableCreator::RangePartitionBound lower_bound_type,
    KuduTableCreator::RangePartitionBound upper_bound_type) {

  if (lower_bound == nullptr || upper_bound == nullptr) {
    data_->status_ = Status::InvalidArgument("range partition bounds may not be null");
    return this;
  }
  if (*lower_bound->schema() != *upper_bound->schema()) {
    data_->status_ = Status::InvalidArgument("range partition bounds must have matching schemas");
    return this;
  }
  if (data_->schema_ == nullptr) {
    data_->schema_ = lower_bound->schema();
  } else if (*lower_bound->schema() != *data_->schema_) {
    data_->status_ = Status::InvalidArgument("range partition bounds must have matching schemas");
    return this;
  }

  Data::Step s { AlterTableRequestPB::ADD_RANGE_PARTITION,
                 nullptr,
                 std::unique_ptr<KuduRangePartition>(new KuduRangePartition(
                     lower_bound, upper_bound, lower_bound_type, upper_bound_type)),
                 dimension_label.empty() ? nullopt : make_optional(dimension_label) };
  data_->steps_.emplace_back(std::move(s));
  data_->has_alter_partitioning_steps = true;
  return this;
}

KuduTableAlterer* KuduTableAlterer::AddRangePartition(
    KuduRangePartition* partition) {
  CHECK(partition);
  if (partition->data_->lower_bound_ == nullptr || partition->data_->upper_bound_  == nullptr) {
    data_->status_ = Status::InvalidArgument("range partition bounds may not be null");
    return this;
  }
  if (partition->data_->lower_bound_->schema() != partition->data_->upper_bound_->schema()) {
    data_->status_ = Status::InvalidArgument("range partition bounds must have matching schemas");
    return this;
  }
  if (data_->schema_ == nullptr) {
    data_->schema_ = partition->data_->lower_bound_->schema();
  } else if (partition->data_->lower_bound_->schema() != data_->schema_) {
    data_->status_ = Status::InvalidArgument("range partition bounds must have matching schemas");
    return this;
  }

  Data::Step s { AlterTableRequestPB::ADD_RANGE_PARTITION,
                 nullptr,
                 std::unique_ptr<KuduRangePartition>(partition),
                 nullopt };
  data_->steps_.emplace_back(std::move(s));
  data_->has_alter_partitioning_steps = true;
  if (!data_->steps_.back().range_partition->data_->is_table_wide_hash_schema_) {
    data_->adding_range_with_custom_hash_schema = true;
  }
  return this;
}

KuduTableAlterer* KuduTableAlterer::DropRangePartition(
    KuduPartialRow* lower_bound,
    KuduPartialRow* upper_bound,
    KuduTableCreator::RangePartitionBound lower_bound_type,
    KuduTableCreator::RangePartitionBound upper_bound_type) {
  if (lower_bound == nullptr || upper_bound == nullptr) {
    data_->status_ = Status::InvalidArgument("range partition bounds may not be null");
    return this;
  }
  if (*lower_bound->schema() != *upper_bound->schema()) {
    data_->status_ = Status::InvalidArgument("range partition bounds must have matching schemas");
    return this;
  }
  if (data_->schema_ == nullptr) {
    data_->schema_ = lower_bound->schema();
  } else if (*lower_bound->schema() != *data_->schema_) {
    data_->status_ = Status::InvalidArgument("range partition bounds must have matching schemas");
    return this;
  }

  Data::Step s { AlterTableRequestPB::DROP_RANGE_PARTITION,
                 nullptr,
                 std::unique_ptr<KuduRangePartition>(new KuduRangePartition(
                     lower_bound, upper_bound, lower_bound_type, upper_bound_type)) };
  data_->steps_.emplace_back(std::move(s));
  data_->has_alter_partitioning_steps = true;
  return this;
}

KuduTableAlterer* KuduTableAlterer::AlterExtraConfig(const map<string, string>& extra_configs) {
  data_->new_extra_configs_ = extra_configs;
  return this;
}

KuduTableAlterer* KuduTableAlterer::SetTableDiskSizeLimit(int64_t disk_size_limit) {
  data_->disk_size_limit_ = disk_size_limit;
  return this;
}

KuduTableAlterer* KuduTableAlterer::SetTableRowCountLimit(int64_t row_count_limit) {
  data_->row_count_limit_ = row_count_limit;
  return this;
}

KuduTableAlterer* KuduTableAlterer::timeout(const MonoDelta& timeout) {
  data_->timeout_ = timeout;
  return this;
}

KuduTableAlterer* KuduTableAlterer::wait(bool wait) {
  data_->wait_ = wait;
  return this;
}

KuduTableAlterer* KuduTableAlterer::modify_external_catalogs(
    bool modify_external_catalogs) {
  data_->modify_external_catalogs_ = modify_external_catalogs;
  return this;
}

Status KuduTableAlterer::Alter() {
  AlterTableRequestPB req;
  AlterTableResponsePB resp;
  RETURN_NOT_OK(data_->ToRequest(&req));

  bool has_immutable_column_schema = false;
  for (const auto& step : data_->steps_) {
    if ((step.step_type == AlterTableRequestPB::ADD_COLUMN ||
         step.step_type == AlterTableRequestPB::ALTER_COLUMN) &&
        step.spec->data_->immutable) {
      has_immutable_column_schema = true;
      break;
    }
  }

  MonoDelta timeout = data_->timeout_.Initialized() ?
    data_->timeout_ :
    data_->client_->default_admin_operation_timeout();
  MonoTime deadline = MonoTime::Now() + timeout;
  RETURN_NOT_OK(data_->client_->data_->AlterTable(
      data_->client_, req, &resp, deadline,
      data_->has_alter_partitioning_steps,
      data_->adding_range_with_custom_hash_schema,
      has_immutable_column_schema));

  if (data_->has_alter_partitioning_steps) {
    // If the table partitions change, clear the local meta cache so that the
    // new tablets can immediately be written to and scanned, and the old
    // tablets won't be seen again. This also prevents rows being batched for
    // the wrong tablet when a partition is dropped and added in the same alter
    // table transaction. We could clear the meta cache for just the table being
    // altered or just the partition key ranges being changed, but that would
    // require opening the table in order to get the ID, schema, and partition
    // schema.
    //
    // It is not necessary to wait for the alteration to be completed before
    // clearing the cache (i.e. the tablets to be created), because the master
    // has its soft state updated as part of handling the alter table RPC. When
    // the meta cache looks up the new tablet locations during a subsequent
    // write or scan, the master will return a ServiceUnavailable response if
    // the new tablets are not yet running. The meta cache will automatically
    // retry after a delay when it encounters this error.
    data_->client_->data_->meta_cache_->ClearCache();
  }

  if (data_->wait_) {
    if (!resp.has_table_id()) {
      return Status::NotSupported("Alter Table succeeded but the server's "
          "response did not include a table ID. This server is too old to wait "
          "for alter table to finish");
    }
    TableIdentifierPB table;
    table.set_table_id(resp.table_id());
    RETURN_NOT_OK(data_->client_->data_->WaitForAlterTableToFinish(
        data_->client_, table, deadline));
  }

  return Status::OK();
}

////////////////////////////////////////////////////////////
// KuduScanner
////////////////////////////////////////////////////////////

KuduScanner::KuduScanner(KuduTable* table)
  : data_(new KuduScanner::Data(table)) {
}

KuduScanner::~KuduScanner() {
  Close();
  delete data_;
}

Status KuduScanner::SetProjectedColumns(const vector<string>& col_names) {
  return SetProjectedColumnNames(col_names);
}

Status KuduScanner::SetProjectedColumnNames(const vector<string>& col_names) {
  if (data_->open_) {
    return Status::IllegalState("Projection must be set before Open()");
  }
  return data_->mutable_configuration()->SetProjectedColumnNames(col_names);
}

Status KuduScanner::SetProjectedColumnIndexes(const vector<int>& col_indexes) {
  if (data_->open_) {
    return Status::IllegalState("Projection must be set before Open()");
  }
  return data_->mutable_configuration()->SetProjectedColumnIndexes(col_indexes);
}

Status KuduScanner::SetBatchSizeBytes(uint32_t batch_size) {
  return data_->mutable_configuration()->SetBatchSizeBytes(batch_size);
}

Status KuduScanner::SetReadMode(ReadMode read_mode) {
  if (data_->open_) {
    return Status::IllegalState("Read mode must be set before Open()");
  }
  if (!tight_enum_test<ReadMode>(read_mode)) {
    return Status::InvalidArgument("Bad read mode");
  }
  return data_->mutable_configuration()->SetReadMode(read_mode);
}

Status KuduScanner::SetOrderMode(OrderMode order_mode) {
  if (data_->open_) {
    return Status::IllegalState("Order mode must be set before Open()");
  }
  if (!tight_enum_test<OrderMode>(order_mode)) {
    return Status::InvalidArgument("Bad order mode");
  }
  return data_->mutable_configuration()->SetFaultTolerant(order_mode == ORDERED);
}

Status KuduScanner::SetFaultTolerant() {
  if (data_->open_) {
    return Status::IllegalState("Fault-tolerance must be set before Open()");
  }
  return data_->mutable_configuration()->SetFaultTolerant(true);
}

Status KuduScanner::SetSnapshotMicros(uint64_t snapshot_timestamp_micros) {
  if (data_->open_) {
    return Status::IllegalState("Snapshot timestamp must be set before Open()");
  }
  data_->mutable_configuration()->SetSnapshotMicros(snapshot_timestamp_micros);
  return Status::OK();
}

Status KuduScanner::SetSnapshotRaw(uint64_t snapshot_timestamp) {
  if (data_->open_) {
    return Status::IllegalState("Snapshot timestamp must be set before Open()");
  }
  if (snapshot_timestamp == 0) {
    return Status::IllegalState("Snapshot timestamp must be set bigger than 0");
  }
  data_->mutable_configuration()->SetSnapshotRaw(snapshot_timestamp);
  return Status::OK();
}

Status KuduScanner::SetDiffScan(uint64_t start_timestamp, uint64_t end_timestamp) {
  if (data_->open_) {
    return Status::IllegalState("Diff scan must be set before Open()");
  }
  return data_->mutable_configuration()->SetDiffScan(start_timestamp, end_timestamp);
}

Status KuduScanner::SetSelection(KuduClient::ReplicaSelection selection) {
  if (data_->open_) {
    return Status::IllegalState("Replica selection must be set before Open()");
  }
  return data_->mutable_configuration()->SetSelection(selection);
}

Status KuduScanner::SetTimeoutMillis(int millis) {
  if (data_->open_) {
    return Status::IllegalState("Timeout must be set before Open()");
  }
  data_->mutable_configuration()->SetTimeoutMillis(millis);
  return Status::OK();
}

Status KuduScanner::AddConjunctPredicate(KuduPredicate* pred) {
  // Take ownership even if returning non-OK status.
  unique_ptr<KuduPredicate> p(pred);
  if (data_->open_) {
    return Status::IllegalState("Predicate must be set before Open()");
  }
  return data_->mutable_configuration()->AddConjunctPredicate(std::move(p));
}

Status KuduScanner::AddLowerBound(const KuduPartialRow& key) {
  return data_->mutable_configuration()->AddLowerBound(key);
}

Status KuduScanner::AddLowerBoundRaw(const Slice& key) {
  return data_->mutable_configuration()->AddLowerBoundRaw(key);
}

Status KuduScanner::AddExclusiveUpperBound(const KuduPartialRow& key) {
  return data_->mutable_configuration()->AddUpperBound(key);
}

Status KuduScanner::AddExclusiveUpperBoundRaw(const Slice& key) {
  return data_->mutable_configuration()->AddUpperBoundRaw(key);
}

Status KuduScanner::AddLowerBoundPartitionKeyRaw(const Slice& partition_key) {
  // The number of hash dimensions in all hash schemas of a table is an
  // invariant and checked throughout the code. With that, the table-wide hash
  // schema is used as a proxy to find the number of hash dimensions to separate
  // the hash-related prefix from the rest of the encoded partition key in the
  // code below.
  //
  // TODO(KUDU-2671) update this code if allowing for different number of
  //                 dimensions in range-specific hash schemas
  const auto& hash_schema = GetKuduTable()->partition_schema().hash_schema();
  return data_->mutable_configuration()->AddLowerBoundPartitionKeyRaw(
      Partition::StringToPartitionKey(partition_key.ToString(),
                                      hash_schema.size()));
}

Status KuduScanner::AddExclusiveUpperBoundPartitionKeyRaw(const Slice& partition_key) {
  // The number of hash dimensions in all hash schemas of a table is an
  // invariant and checked throughout the code. With that, the table-wide hash
  // schema is used as a proxy to find the number of hash dimensions to separate
  // the hash-related prefix from the rest of the encoded partition key in the
  // code below.
  //
  // TODO(KUDU-2671) update this code if allowing for different number of
  //                 dimensions in range-specific hash schemas
  const auto& hash_schema = GetKuduTable()->partition_schema().hash_schema();
  return data_->mutable_configuration()->AddUpperBoundPartitionKeyRaw(
      Partition::StringToPartitionKey(partition_key.ToString(),
                                      hash_schema.size()));
}

Status KuduScanner::SetCacheBlocks(bool cache_blocks) {
  if (data_->open_) {
    return Status::IllegalState("Block caching must be set before Open()");
  }
  return data_->mutable_configuration()->SetCacheBlocks(cache_blocks);
}

KuduSchema KuduScanner::GetProjectionSchema() const {
  return KuduSchema::FromSchema(*data_->configuration().projection());
}

shared_ptr<KuduTable> KuduScanner::GetKuduTable() {
  return data_->table_;
}

Status KuduScanner::SetRowFormatFlags(uint64_t flags) {
  switch (flags) {
    case NO_FLAGS:
    case PAD_UNIXTIME_MICROS_TO_16_BYTES:
    case COLUMNAR_LAYOUT:
      break;
    default:
      return Status::InvalidArgument(Substitute("Invalid row format flags: $0", flags));
  }
  if (data_->open_) {
    return Status::IllegalState("Row format flags must be set before Open()");
  }

  return data_->mutable_configuration()->SetRowFormatFlags(flags);
}

Status KuduScanner::SetLimit(int64_t limit) {
  if (data_->open_) {
    return Status::IllegalState("Limit must be set before Open()");
  }
  return data_->mutable_configuration()->SetLimit(limit);
}

const ResourceMetrics& KuduScanner::GetResourceMetrics() const {
  return data_->resource_metrics_;
}

namespace {
// Callback for the RPC sent by Close().
// We can't use the KuduScanner response and RPC controller members for this
// call, because the scanner object may be destructed while the call is still
// being processed.
struct CloseCallback {
  RpcController controller;
  ScanResponsePB response;
  string scanner_id;
  void Callback() {
    if (!controller.status().ok()) {
      LOG(WARNING) << "Couldn't close scanner " << scanner_id << ": "
                   << controller.status().ToString();
    }
    delete this;
  }
};
} // anonymous namespace

string KuduScanner::ToString() const {
  return KUDU_DISABLE_REDACTION(Substitute(
      "$0: $1",
      data_->table_->name(),
      data_->configuration().spec().ToString(*data_->table_->schema().schema_)));
}

Status KuduScanner::Open() {
  CHECK(!data_->open_) << "Scanner already open";

  if (data_->configuration().has_start_timestamp()) {
    RETURN_NOT_OK(data_->mutable_configuration()->AddIsDeletedColumn());
  }
  data_->mutable_configuration()->OptimizeScanSpec();
  data_->partition_pruner_.Init(*data_->table_->schema().schema_,
                                data_->table_->partition_schema(),
                                data_->configuration().spec());

  if (data_->configuration().spec().CanShortCircuit() ||
      !data_->partition_pruner_.HasMorePartitionKeyRanges()) {
    VLOG(2) << "Short circuiting scan " << data_->DebugString();
    data_->open_ = true;
    data_->short_circuit_ = true;
    return Status::OK();
  }

  // For READ_YOUR_WRITES scan mode, get the latest observed timestamp and store it
  // to scan config. Always use this one as propagation timestamp for the duration
  // of the scan to avoid unnecessarily wait.
  if (data_->configuration().read_mode() == READ_YOUR_WRITES) {
    const uint64_t lo_ts = data_->table_->client()->data_->GetLatestObservedTimestamp();
    data_->mutable_configuration()->SetScanLowerBoundTimestampRaw(lo_ts);
  }

  if (data_->configuration().read_mode() != READ_AT_SNAPSHOT &&
      data_->configuration().has_snapshot_timestamp()) {
    return Status::InvalidArgument("Snapshot timestamp should only be configured "
                                   "for READ_AT_SNAPSHOT scan mode.");
  }

  VLOG(2) << "Beginning " << data_->DebugString();

  MonoTime deadline = MonoTime::Now() + data_->configuration().timeout();
  set<string> blacklist;

  RETURN_NOT_OK(data_->OpenNextTablet(deadline, &blacklist));

  data_->open_ = true;
  return Status::OK();
}

Status KuduScanner::KeepAlive() {
  return data_->KeepAlive();
}

void KuduScanner::Close() {
  if (!data_->open_) return;

  VLOG(2) << "Ending " << data_->DebugString();

  // Close the scanner on the server-side, if necessary.
  //
  // If the scan did not match any rows, the tserver will not assign a scanner ID.
  // This is reflected in the Open() response. In this case, there is no server-side state
  // to clean up.
  if (!data_->next_req_.scanner_id().empty()) {
    CHECK(data_->proxy_);
    unique_ptr<CloseCallback> closer(new CloseCallback);
    closer->scanner_id = data_->next_req_.scanner_id();
    data_->PrepareRequest(KuduScanner::Data::CLOSE);
    data_->next_req_.set_close_scanner(true);
    closer->controller.set_timeout(data_->configuration().timeout());
    // CloseCallback::Callback() deletes the closer.
    CloseCallback* closer_raw = closer.release();
    data_->proxy_->ScanAsync(data_->next_req_, &closer_raw->response, &closer_raw->controller,
                             [closer_raw]() { closer_raw->Callback(); });
  }
  data_->proxy_.reset();
  data_->open_ = false;
  return;
}

bool KuduScanner::HasMoreRows() const {
  CHECK(data_->open_);
  return !data_->short_circuit_ &&                 // The scan is not short circuited
      (data_->data_in_open_ ||                     // more data in hand
       data_->last_response_.has_more_results() || // more data in this tablet
       data_->MoreTablets());                      // more tablets to scan, possibly with more data
}

Status KuduScanner::NextBatch(vector<KuduRowResult>* rows) {
  if (PREDICT_FALSE(data_->configuration().row_format_flags() != KuduScanner::NO_FLAGS)) {
    return Status::IllegalState(
        Substitute("Cannot extract rows. Row format modifier flags were selected: $0",
                   data_->configuration().row_format_flags()));
  }
  RETURN_NOT_OK(NextBatch(&data_->batch_for_old_api_));
  data_->batch_for_old_api_.data_->ExtractRows(rows);
  return Status::OK();
}

Status KuduScanner::NextBatch(KuduScanBatch* batch) {
  return NextBatch(batch->data_);
}

Status KuduScanner::NextBatch(KuduColumnarScanBatch* batch) {
  return NextBatch(batch->data_);
}

Status KuduScanner::NextBatch(internal::ScanBatchDataInterface* batch_data) {

  // TODO: do some double-buffering here -- when we return this batch
  // we should already have fired off the RPC for the next batch, but
  // need to do some swapping of the response objects around to avoid
  // stomping on the memory the user is looking at.
  CHECK(data_->open_);

  batch_data->Clear();

  if (data_->short_circuit_) {
    return Status::OK();
  }

  if (data_->data_in_open_) {
    // We have data from a previous scan.
    CHECK(data_->proxy_);
    VLOG(2) << "Extracting data from " << data_->DebugString();
    data_->data_in_open_ = false;
    return batch_data->Reset(&data_->controller_,
                             data_->configuration().projection(),
                             data_->configuration().client_projection(),
                             data_->configuration().row_format_flags(),
                             &data_->last_response_);
  }

  if (data_->last_response_.has_more_results()) {
    // More data is available in this tablet.
    CHECK(data_->proxy_);
    VLOG(2) << "Continuing " << data_->DebugString();

    MonoTime batch_deadline = MonoTime::Now() + data_->configuration().timeout();
    data_->PrepareRequest(KuduScanner::Data::CONTINUE);

    while (true) {
      bool allow_time_for_failover = data_->configuration().is_fault_tolerant();
      ScanRpcStatus result = data_->SendScanRpc(batch_deadline, allow_time_for_failover);

      // Success case.
      if (result.result == ScanRpcStatus::OK) {
        if (data_->last_response_.has_last_primary_key()) {
          data_->last_primary_key_ = data_->last_response_.last_primary_key();
        }
        data_->scan_attempts_ = 0;
        return batch_data->Reset(&data_->controller_,
                                 data_->configuration().projection(),
                                 data_->configuration().client_projection(),
                                 data_->configuration().row_format_flags(),
                                 &data_->last_response_);
      }

      data_->scan_attempts_++;

      // Error handling.
      set<string> blacklist;
      bool needs_reopen = false;
      Status s = data_->HandleError(result, batch_deadline, &blacklist, &needs_reopen);
      if (!s.ok()) {
        LOG(WARNING) << "Scan on tablet server " << data_->ts_->ToString() << " with "
                     << data_->DebugString() << " failed: " << result.status.ToString();
        return s;
      }

      if (data_->configuration().is_fault_tolerant()) {
        LOG(WARNING) << "Attempting to retry " << data_->DebugString()
                     << " elsewhere.";
        return data_->ReopenCurrentTablet(batch_deadline, &blacklist);
      }

      if (blacklist.empty() && !needs_reopen) {
        // If we didn't blacklist the current server, we can just retry again.
        continue;
      }
      // If we blacklisted the current server, and it's not fault-tolerant, we can't
      // retry anywhere, so just propagate the error.
      return result.status;
    }
  } else if (data_->MoreTablets()) {
    // More data may be available in other tablets.
    // No need to close the current tablet; we scanned all the data so the
    // server closed it for us.
    VLOG(2) << "Scanning next tablet " << data_->DebugString();
    data_->last_primary_key_.clear();
    MonoTime deadline = MonoTime::Now() + data_->configuration().timeout();
    set<string> blacklist;

    RETURN_NOT_OK(data_->OpenNextTablet(deadline, &blacklist));
    if (data_->data_in_open_) {
      // Avoid returning an empty batch in between tablets if we have data
      // we can return from this call.
      return NextBatch(batch_data);
    }
    return Status::OK();
  } else {
    // No more data anywhere.
    return Status::OK();
  }
}

Status KuduScanner::GetCurrentServer(KuduTabletServer** server) {
  CHECK(data_->open_);
  internal::RemoteTabletServer* rts = data_->ts_;
  CHECK(rts);
  vector<HostPort> host_ports;
  rts->GetHostPorts(&host_ports);
  if (host_ports.empty()) {
    return Status::IllegalState(Substitute("No HostPort found for RemoteTabletServer $0",
                                           rts->ToString()));
  }
  unique_ptr<KuduTabletServer> client_server(new KuduTabletServer);
  client_server->data_ = new KuduTabletServer::Data(rts->permanent_uuid(),
                                                    host_ports[0],
                                                    rts->location());
  *server = client_server.release();
  return Status::OK();
}

////////////////////////////////////////////////////////////
// KuduScanToken
////////////////////////////////////////////////////////////

KuduScanToken::KuduScanToken()
    : data_(nullptr) {
}

KuduScanToken::~KuduScanToken() {
  delete data_;
}

Status KuduScanToken::IntoKuduScanner(KuduScanner** scanner) const {
  return data_->IntoKuduScanner(scanner);
}

const KuduTablet& KuduScanToken::tablet() const {
  return data_->tablet();
}

Status KuduScanToken::Serialize(string* buf) const {
  return data_->Serialize(buf);
}

Status KuduScanToken::DeserializeIntoScanner(KuduClient* client,
                                             const string& serialized_token,
                                             KuduScanner** scanner) {
  return KuduScanToken::Data::DeserializeIntoScanner(
      client, serialized_token, scanner);
}

////////////////////////////////////////////////////////////
// KuduScanTokenBuilder
////////////////////////////////////////////////////////////

KuduScanTokenBuilder::KuduScanTokenBuilder(KuduTable* table)
    : data_(new KuduScanTokenBuilder::Data(table)) {
}

KuduScanTokenBuilder::~KuduScanTokenBuilder() {
  delete data_;
}

Status KuduScanTokenBuilder::SetProjectedColumnNames(const vector<string>& col_names) {
  return data_->mutable_configuration()->SetProjectedColumnNames(col_names);
}

Status KuduScanTokenBuilder::SetProjectedColumnIndexes(const vector<int>& col_indexes) {
  return data_->mutable_configuration()->SetProjectedColumnIndexes(col_indexes);
}

Status KuduScanTokenBuilder::SetBatchSizeBytes(uint32_t batch_size) {
  return data_->mutable_configuration()->SetBatchSizeBytes(batch_size);
}

Status KuduScanTokenBuilder::SetReadMode(KuduScanner::ReadMode read_mode) {
  if (!tight_enum_test<KuduScanner::ReadMode>(read_mode)) {
    return Status::InvalidArgument("Bad read mode");
  }
  return data_->mutable_configuration()->SetReadMode(read_mode);
}

Status KuduScanTokenBuilder::SetFaultTolerant() {
  return data_->mutable_configuration()->SetFaultTolerant(true);
}

Status KuduScanTokenBuilder::SetSnapshotMicros(uint64_t snapshot_timestamp_micros) {
  data_->mutable_configuration()->SetSnapshotMicros(snapshot_timestamp_micros);
  return Status::OK();
}

Status KuduScanTokenBuilder::SetDiffScan(uint64_t start_timestamp, uint64_t end_timestamp) {
  return data_->mutable_configuration()->SetDiffScan(start_timestamp, end_timestamp);
}

Status KuduScanTokenBuilder::SetSnapshotRaw(uint64_t snapshot_timestamp) {
  data_->mutable_configuration()->SetSnapshotRaw(snapshot_timestamp);
  return Status::OK();
}

Status KuduScanTokenBuilder::SetSelection(KuduClient::ReplicaSelection selection) {
  return data_->mutable_configuration()->SetSelection(selection);
}

Status KuduScanTokenBuilder::SetTimeoutMillis(int millis) {
  data_->mutable_configuration()->SetTimeoutMillis(millis);
  return Status::OK();
}

Status KuduScanTokenBuilder::IncludeTableMetadata(bool include_metadata) {
  data_->IncludeTableMetadata(include_metadata);
  return Status::OK();
}

Status KuduScanTokenBuilder::IncludeTabletMetadata(bool include_metadata) {
  data_->IncludeTabletMetadata(include_metadata);
  return Status::OK();
}

Status KuduScanTokenBuilder::AddConjunctPredicate(KuduPredicate* pred) {
  unique_ptr<KuduPredicate> p(pred);
  return data_->mutable_configuration()->AddConjunctPredicate(std::move(p));
}

Status KuduScanTokenBuilder::AddLowerBound(const KuduPartialRow& key) {
  return data_->mutable_configuration()->AddLowerBound(key);
}

Status KuduScanTokenBuilder::AddUpperBound(const KuduPartialRow& key) {
  return data_->mutable_configuration()->AddUpperBound(key);
}

Status KuduScanTokenBuilder::SetCacheBlocks(bool cache_blocks) {
  return data_->mutable_configuration()->SetCacheBlocks(cache_blocks);
}

Status KuduScanTokenBuilder::Build(vector<KuduScanToken*>* tokens) {
  return data_->Build(tokens);
}

void KuduScanTokenBuilder::SetSplitSizeBytes(uint64_t split_size_bytes) {
  return data_->SplitSizeBytes(split_size_bytes);
}

////////////////////////////////////////////////////////////
// KuduReplica
////////////////////////////////////////////////////////////

KuduReplica::KuduReplica()
  : data_(nullptr) {
}

KuduReplica::~KuduReplica() {
  delete data_;
}

bool KuduReplica::is_leader() const {
  return data_->is_leader_;
}

const KuduTabletServer& KuduReplica::ts() const {
  return *data_->ts_;
}

////////////////////////////////////////////////////////////
// KuduTablet
////////////////////////////////////////////////////////////

KuduTablet::KuduTablet()
  : data_(nullptr) {
}

KuduTablet::~KuduTablet() {
  delete data_;
}

const string& KuduTablet::id() const {
  return data_->id_;
}

const string& KuduTablet::table_id() const {
  return data_->table_id_;
}

const string& KuduTablet::table_name() const {
  return data_->table_name_;
}

const vector<const KuduReplica*>& KuduTablet::replicas() const {
  return data_->replicas_;
}

////////////////////////////////////////////////////////////
// KuduTabletServer
////////////////////////////////////////////////////////////

KuduTabletServer::KuduTabletServer()
  : data_(nullptr) {
}

KuduTabletServer::~KuduTabletServer() {
  delete data_;
}

const string& KuduTabletServer::uuid() const {
  return data_->uuid_;
}

const string& KuduTabletServer::hostname() const {
  return data_->hp_.host();
}

uint16_t KuduTabletServer::port() const {
  return data_->hp_.port();
}

const string& KuduTabletServer::location() const {
  return data_->location_;
}

////////////////////////////////////////////////////////////
// KuduPartitionerBuilder
////////////////////////////////////////////////////////////
KuduPartitionerBuilder::KuduPartitionerBuilder(sp::shared_ptr<KuduTable> table)
    : data_(new Data(std::move(table))) {
}

KuduPartitionerBuilder::~KuduPartitionerBuilder() {
  delete data_;
}

KuduPartitionerBuilder* KuduPartitionerBuilder::SetBuildTimeout(MonoDelta timeout) {
  data_->SetBuildTimeout(timeout);
  return this;
}

Status KuduPartitionerBuilder::Build(KuduPartitioner** partitioner) {
  return data_->Build(partitioner);
}

////////////////////////////////////////////////////////////
// KuduPartitioner
////////////////////////////////////////////////////////////
KuduPartitioner::KuduPartitioner(Data* data)
    : data_(CHECK_NOTNULL(data)) {
}

KuduPartitioner::~KuduPartitioner() {
  delete data_;
}

int KuduPartitioner::NumPartitions() const {
  return data_->num_partitions_;
}

Status KuduPartitioner::PartitionRow(const KuduPartialRow& row, int* partition) {
  return data_->PartitionRow(row, partition);
}

} // namespace client
} // namespace kudu
