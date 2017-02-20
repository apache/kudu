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
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/bind.hpp>
#include <boost/optional.hpp>

#include "kudu/client/batcher.h"
#include "kudu/client/callbacks.h"
#include "kudu/client/client-internal.h"
#include "kudu/client/client_builder-internal.h"
#include "kudu/client/error-internal.h"
#include "kudu/client/error_collector.h"
#include "kudu/client/meta_cache.h"
#include "kudu/client/replica-internal.h"
#include "kudu/client/row_result.h"
#include "kudu/client/scan_predicate-internal.h"
#include "kudu/client/scan_token-internal.h"
#include "kudu/client/scanner-internal.h"
#include "kudu/client/schema-internal.h"
#include "kudu/client/session-internal.h"
#include "kudu/client/table-internal.h"
#include "kudu/client/table_alterer-internal.h"
#include "kudu/client/table_creator-internal.h"
#include "kudu/client/tablet-internal.h"
#include "kudu/client/tablet_server-internal.h"
#include "kudu/client/write_op.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partition.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/request_tracker.h"
#include "kudu/rpc/sasl_common.h"
#include "kudu/security/cert.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/tls_context.h"
#include "kudu/util/init.h"
#include "kudu/util/logging.h"
#include "kudu/util/net/dns_resolver.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/version_info.h"

using kudu::master::AlterTableRequestPB;
using kudu::master::AlterTableRequestPB_Step;
using kudu::master::AlterTableResponsePB;
using kudu::master::CreateTableRequestPB;
using kudu::master::CreateTableResponsePB;
using kudu::master::DeleteTableRequestPB;
using kudu::master::DeleteTableResponsePB;
using kudu::master::GetTableSchemaRequestPB;
using kudu::master::GetTableSchemaResponsePB;
using kudu::master::GetTabletLocationsRequestPB;
using kudu::master::GetTabletLocationsResponsePB;
using kudu::master::ListTablesRequestPB;
using kudu::master::ListTablesResponsePB;
using kudu::master::ListTabletServersRequestPB;
using kudu::master::ListTabletServersResponsePB;
using kudu::master::ListTabletServersResponsePB_Entry;
using kudu::master::MasterServiceProxy;
using kudu::master::TabletLocationsPB;
using kudu::master::TSInfoPB;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;
using kudu::tserver::ScanResponsePB;
using std::pair;
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
                 kudu::client::KuduScanner::READ_AT_SNAPSHOT);

MAKE_ENUM_LIMITS(kudu::client::KuduScanner::OrderMode,
                 kudu::client::KuduScanner::UNORDERED,
                 kudu::client::KuduScanner::ORDERED);

namespace kudu {
namespace client {

using internal::Batcher;
using internal::ErrorCollector;
using internal::MetaCache;
using sp::shared_ptr;

static const char* kProgName = "kudu_client";

// We need to reroute all logging to stderr when the client library is
// loaded. GoogleOnceInit() can do that, but there are multiple entry
// points into the client code, and it'd need to be called in each one.
// So instead, let's use a constructor function.
//
// Should this be restricted to just the exported client build? Probably
// not, as any application using the library probably wants stderr logging
// more than file logging.
__attribute__((constructor))
static void InitializeBasicLogging() {
  InitGoogleLoggingSafeBasic(kProgName);
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
  RegisterLoggingCallback(Bind(&LoggingAdapterCB, Unretained(cb)));
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
  return VersionInfo::GetShortVersionString();
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

KuduClientBuilder& KuduClientBuilder::import_authentication_credentials(string authn_creds) {
  data_->authn_creds_ = std::move(authn_creds);
  return *this;
}

namespace {
Status ImportAuthnCredsToMessenger(const string& authn_creds,
                                   Messenger* messenger) {
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
  std::shared_ptr<Messenger> messenger;
  RETURN_NOT_OK(builder.Build(&messenger));

  // Parse and import the provided authn data, if any.
  if (!data_->authn_creds_.empty()) {
    RETURN_NOT_OK(ImportAuthnCredsToMessenger(data_->authn_creds_, messenger.get()));
  }

  shared_ptr<KuduClient> c(new KuduClient());
  c->data_->messenger_ = std::move(messenger);
  c->data_->master_server_addrs_ = data_->master_server_addrs_;
  c->data_->default_admin_operation_timeout_ = data_->default_admin_operation_timeout_;
  c->data_->default_rpc_timeout_ = data_->default_rpc_timeout_;

  // Let's allow for plenty of time for discovering the master the first
  // time around.
  MonoTime deadline = MonoTime::Now() + c->default_admin_operation_timeout();
  RETURN_NOT_OK_PREPEND(c->data_->ConnectToCluster(c.get(), deadline),
                        "Could not connect to the cluster");

  c->data_->meta_cache_.reset(new MetaCache(c.get()));
  c->data_->dns_resolver_.reset(new DnsResolver());

  // Init local host names used for locality decisions.
  RETURN_NOT_OK_PREPEND(c->data_->InitLocalHostNames(),
                        "Could not determine local host names");

  c->data_->request_tracker_ = new rpc::RequestTracker(c->data_->client_id_);

  client->swap(c);
  return Status::OK();
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
                                           bool *create_in_progress) {
  MonoTime deadline = MonoTime::Now() + default_admin_operation_timeout();
  return data_->IsCreateTableInProgress(this, table_name, deadline, create_in_progress);
}

Status KuduClient::DeleteTable(const string& table_name) {
  MonoTime deadline = MonoTime::Now() + default_admin_operation_timeout();
  return data_->DeleteTable(this, table_name, deadline);
}

KuduTableAlterer* KuduClient::NewTableAlterer(const string& name) {
  return new KuduTableAlterer(this, name);
}

Status KuduClient::IsAlterTableInProgress(const string& table_name,
                                          bool *alter_in_progress) {
  MonoTime deadline = MonoTime::Now() + default_admin_operation_timeout();
  return data_->IsAlterTableInProgress(this, table_name, deadline, alter_in_progress);
}

Status KuduClient::GetTableSchema(const string& table_name,
                                  KuduSchema* schema) {
  MonoTime deadline = MonoTime::Now() + default_admin_operation_timeout();
  return data_->GetTableSchema(this,
                               table_name,
                               deadline,
                               schema,
                               nullptr, // partition schema
                               nullptr, // table id
                               nullptr); // number of replicas
}

Status KuduClient::ListTabletServers(vector<KuduTabletServer*>* tablet_servers) {
  ListTabletServersRequestPB req;
  ListTabletServersResponsePB resp;

  MonoTime deadline = MonoTime::Now() + default_admin_operation_timeout();
  Status s =
      data_->SyncLeaderMasterRpc<ListTabletServersRequestPB, ListTabletServersResponsePB>(
          deadline,
          this,
          req,
          &resp,
          "ListTabletServers",
          &MasterServiceProxy::ListTabletServers,
          {});
  RETURN_NOT_OK(s);
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  for (int i = 0; i < resp.servers_size(); i++) {
    const ListTabletServersResponsePB_Entry& e = resp.servers(i);
    HostPort hp;
    RETURN_NOT_OK(HostPortFromPB(e.registration().rpc_addresses(0), &hp));
    unique_ptr<KuduTabletServer> ts(new KuduTabletServer);
    ts->data_ = new KuduTabletServer::Data(e.instance_id().permanent_uuid(), hp);
    tablet_servers->push_back(ts.release());
  }
  return Status::OK();
}

Status KuduClient::ListTables(vector<string>* tables,
                              const string& filter) {
  ListTablesRequestPB req;
  ListTablesResponsePB resp;

  if (!filter.empty()) {
    req.set_name_filter(filter);
  }
  MonoTime deadline = MonoTime::Now() + default_admin_operation_timeout();
  Status s =
      data_->SyncLeaderMasterRpc<ListTablesRequestPB, ListTablesResponsePB>(
          deadline,
          this,
          req,
          &resp,
          "ListTables",
          &MasterServiceProxy::ListTables,
          {});
  RETURN_NOT_OK(s);
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  for (int i = 0; i < resp.tables_size(); i++) {
    tables->push_back(resp.tables(i).name());
  }
  return Status::OK();
}

Status KuduClient::TableExists(const string& table_name, bool* exists) {
  vector<string> tables;
  RETURN_NOT_OK(ListTables(&tables, table_name));
  for (const string& table : tables) {
    if (table == table_name) {
      *exists = true;
      return Status::OK();
    }
  }
  *exists = false;
  return Status::OK();
}

Status KuduClient::OpenTable(const string& table_name,
                             shared_ptr<KuduTable>* table) {
  KuduSchema schema;
  string table_id;
  int num_replicas;
  PartitionSchema partition_schema;
  MonoTime deadline = MonoTime::Now() + default_admin_operation_timeout();
  RETURN_NOT_OK(data_->GetTableSchema(this,
                                      table_name,
                                      deadline,
                                      &schema,
                                      &partition_schema,
                                      &table_id,
                                      &num_replicas));

  // TODO: in the future, probably will look up the table in some map to reuse
  // KuduTable instances.
  table->reset(new KuduTable(shared_from_this(),
                             table_name, table_id, num_replicas,
                             schema, partition_schema));
  return Status::OK();
}

shared_ptr<KuduSession> KuduClient::NewSession() {
  shared_ptr<KuduSession> ret(new KuduSession(shared_from_this()));
  ret->data_->Init(ret);
  return ret;
}

Status KuduClient::GetTablet(const string& tablet_id, KuduTablet** tablet) {
  GetTabletLocationsRequestPB req;
  GetTabletLocationsResponsePB resp;

  req.add_tablet_ids(tablet_id);
  MonoTime deadline = MonoTime::Now() + default_admin_operation_timeout();
  Status s =
      data_->SyncLeaderMasterRpc<GetTabletLocationsRequestPB, GetTabletLocationsResponsePB>(
          deadline,
          this,
          req,
          &resp,
          "GetTabletLocations",
          &MasterServiceProxy::GetTabletLocations,
          {});
  RETURN_NOT_OK(s);
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  if (resp.tablet_locations_size() != 1) {
    return Status::IllegalState(Substitute(
        "Expected only one tablet, but received $0",
        resp.tablet_locations_size()));
  }
  const TabletLocationsPB& t = resp.tablet_locations(0);

  vector<const KuduReplica*> replicas;
  ElementDeleter deleter(&replicas);
  for (const auto& r : t.replicas()) {
    const TSInfoPB& ts_info = r.ts_info();
    if (ts_info.rpc_addresses_size() == 0) {
      return Status::IllegalState(Substitute(
          "No RPC addresses found for tserver $0",
          ts_info.permanent_uuid()));
    }
    HostPort hp;
    RETURN_NOT_OK(HostPortFromPB(ts_info.rpc_addresses(0), &hp));
    unique_ptr<KuduTabletServer> ts(new KuduTabletServer);
    ts->data_ = new KuduTabletServer::Data(ts_info.permanent_uuid(), hp);

    bool is_leader = r.role() == consensus::RaftPeerPB::LEADER;
    unique_ptr<KuduReplica> replica(new KuduReplica);
    replica->data_ = new KuduReplica::Data(is_leader, std::move(ts));

    replicas.push_back(replica.release());
  }

  unique_ptr<KuduTablet> client_tablet(new KuduTablet);
  client_tablet->data_ = new KuduTablet::Data(tablet_id, std::move(replicas));
  replicas.clear();

  *tablet = client_tablet.release();
  return Status::OK();
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

const uint64_t KuduClient::kNoTimestamp = 0;

uint64_t KuduClient::GetLatestObservedTimestamp() const {
  return data_->GetLatestObservedTimestamp();
}

void KuduClient::SetLatestObservedTimestamp(uint64_t ht_timestamp) {
  data_->UpdateLatestObservedTimestamp(ht_timestamp);
}

Status KuduClient::ExportAuthenticationCredentials(string* authn_creds) const {
  AuthenticationCredentialsPB pb;

  boost::optional<security::SignedTokenPB> tok = data_->messenger_->authn_token();
  if (tok) {
    pb.mutable_authn_token()->CopyFrom(*tok);
  }

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
                                                        int32_t num_buckets, int32_t seed) {
  PartitionSchemaPB::HashBucketSchemaPB* bucket_schema =
    data_->partition_schema_.add_hash_bucket_schemas();
  for (const string& col_name : columns) {
    bucket_schema->add_columns()->set_name(col_name);
  }
  bucket_schema->set_num_buckets(num_buckets);
  bucket_schema->set_seed(seed);
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

KuduTableCreator& KuduTableCreator::split_rows(const vector<const KuduPartialRow*>& rows) {
  for (const KuduPartialRow* row : rows) {
    data_->range_partition_splits_.emplace_back(const_cast<KuduPartialRow*>(row));
  }
  return *this;
}

KuduTableCreator& KuduTableCreator::add_range_partition(KuduPartialRow* lower_bound,
                                                        KuduPartialRow* upper_bound,
                                                        RangePartitionBound lower_bound_type,
                                                        RangePartitionBound upper_bound_type) {
  data_->range_partition_bounds_.push_back({
      unique_ptr<KuduPartialRow>(lower_bound),
      unique_ptr<KuduPartialRow>(upper_bound),
      lower_bound_type,
      upper_bound_type,
  });
  return *this;
}

KuduTableCreator& KuduTableCreator::num_replicas(int num_replicas) {
  data_->num_replicas_ = num_replicas;
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
      data_->partition_schema_.hash_bucket_schemas().empty()) {
    return Status::InvalidArgument(
        "Table partitioning must be specified using "
        "add_hash_partitions or set_range_partition_columns");
  }

  // Build request.
  CreateTableRequestPB req;
  req.set_name(data_->table_name_);
  if (data_->num_replicas_ != boost::none) {
    req.set_num_replicas(data_->num_replicas_.get());
  }
  RETURN_NOT_OK_PREPEND(SchemaToPB(*data_->schema_->schema_, req.mutable_schema(),
                                   SCHEMA_PB_WITHOUT_WRITE_DEFAULT),
                        "Invalid schema");

  RowOperationsPBEncoder encoder(req.mutable_split_rows_range_bounds());

  for (const auto& row : data_->range_partition_splits_) {
    if (!row) {
      return Status::InvalidArgument("range split row must not be null");
    }
    encoder.Add(RowOperationsPB::SPLIT_ROW, *row);
  }

  for (const auto& bound : data_->range_partition_bounds_) {
    if (!bound.lower_bound || !bound.upper_bound) {
      return Status::InvalidArgument("range bounds must not be null");
    }

    RowOperationsPB_Type lower_bound_type =
      bound.lower_bound_type == KuduTableCreator::INCLUSIVE_BOUND ?
      RowOperationsPB::RANGE_LOWER_BOUND :
      RowOperationsPB::EXCLUSIVE_RANGE_LOWER_BOUND;

    RowOperationsPB_Type upper_bound_type =
      bound.upper_bound_type == KuduTableCreator::EXCLUSIVE_BOUND ?
      RowOperationsPB::RANGE_UPPER_BOUND :
      RowOperationsPB::INCLUSIVE_RANGE_UPPER_BOUND;

    encoder.Add(lower_bound_type, *bound.lower_bound);
    encoder.Add(upper_bound_type, *bound.upper_bound);
  }

  req.mutable_partition_schema()->CopyFrom(data_->partition_schema_);

  MonoTime deadline = MonoTime::Now();
  if (data_->timeout_.Initialized()) {
    deadline += data_->timeout_;
  } else {
    deadline += data_->client_->default_admin_operation_timeout();
  }

  RETURN_NOT_OK_PREPEND(data_->client_->data_->CreateTable(data_->client_,
                                                           req,
                                                           *data_->schema_,
                                                           deadline,
                                                           !data_->range_partition_bounds_.empty()),
                        Substitute("Error creating table $0 on the master",
                                   data_->table_name_));

  // Spin until the table is fully created, if requested.
  if (data_->wait_) {
    RETURN_NOT_OK(data_->client_->data_->WaitForCreateTableToFinish(data_->client_,
                                                                    data_->table_name_,
                                                                    deadline));
  }

  return Status::OK();
}

////////////////////////////////////////////////////////////
// KuduTable
////////////////////////////////////////////////////////////

KuduTable::KuduTable(const shared_ptr<KuduClient>& client,
                     const string& name,
                     const string& id,
                     int num_replicas,
                     const KuduSchema& schema,
                     const PartitionSchema& partition_schema)
  : data_(new KuduTable::Data(client, name, id, num_replicas,
                              schema, partition_schema)) {
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

int KuduTable::num_replicas() const {
  return data_->num_replicas_;
}

KuduInsert* KuduTable::NewInsert() {
  return new KuduInsert(shared_from_this());
}

KuduUpsert* KuduTable::NewUpsert() {
  return new KuduUpsert(shared_from_this());
}

KuduUpdate* KuduTable::NewUpdate() {
  return new KuduUpdate(shared_from_this());
}

KuduDelete* KuduTable::NewDelete() {
  return new KuduDelete(shared_from_this());
}

KuduClient* KuduTable::client() const {
  return data_->client_.get();
}

const PartitionSchema& KuduTable::partition_schema() const {
  return data_->partition_schema_;
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
  : data_(new KuduError::Data(gscoped_ptr<KuduWriteOperation>(failed_op),
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

KuduSession::~KuduSession() {
  WARN_NOT_OK(data_->Close(true), "Closed Session with pending operations.");
  delete data_;
}

Status KuduSession::Close() {
  return data_->Close(false);
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

Status KuduSession::Flush() {
  return data_->Flush();
}

void KuduSession::FlushAsync(KuduStatusCallback* user_callback) {
  data_->FlushAsync(user_callback);
}

bool KuduSession::HasPendingOperations() const {
  return data_->HasPendingOperations();
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

KuduColumnSpec* KuduTableAlterer::AddColumn(const string& name) {
  Data::Step s = { AlterTableRequestPB::ADD_COLUMN,
                   new KuduColumnSpec(name), nullptr, nullptr };
  data_->steps_.emplace_back(std::move(s));
  return s.spec;
}

KuduColumnSpec* KuduTableAlterer::AlterColumn(const string& name) {
  Data::Step s = { AlterTableRequestPB::ALTER_COLUMN,
                   new KuduColumnSpec(name), nullptr, nullptr };
  data_->steps_.emplace_back(std::move(s));
  return s.spec;
}

KuduTableAlterer* KuduTableAlterer::DropColumn(const string& name) {
  Data::Step s = { AlterTableRequestPB::DROP_COLUMN,
                   new KuduColumnSpec(name), nullptr, nullptr };
  data_->steps_.emplace_back(std::move(s));
  return this;
}

KuduTableAlterer* KuduTableAlterer::AddRangePartition(
    KuduPartialRow* lower_bound,
    KuduPartialRow* upper_bound,
    KuduTableCreator::RangePartitionBound lower_bound_type,
    KuduTableCreator::RangePartitionBound upper_bound_type) {

  if (lower_bound == nullptr || upper_bound == nullptr) {
    data_->status_ = Status::InvalidArgument("range partition bounds may not be null");
    return this;
  }
  if (!lower_bound->schema()->Equals(*upper_bound->schema())) {
    data_->status_ = Status::InvalidArgument("range partition bounds must have matching schemas");
    return this;
  }
  if (data_->schema_ == nullptr) {
    data_->schema_ = lower_bound->schema();
  } else if (!lower_bound->schema()->Equals(*data_->schema_)) {
    data_->status_ = Status::InvalidArgument("range partition bounds must have matching schemas");
    return this;
  }

  Data::Step s { AlterTableRequestPB::ADD_RANGE_PARTITION,
                 nullptr,
                 unique_ptr<KuduPartialRow>(lower_bound),
                 unique_ptr<KuduPartialRow>(upper_bound),
                 lower_bound_type,
                 upper_bound_type };
  data_->steps_.emplace_back(std::move(s));
  data_->has_alter_partitioning_steps = true;
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
  if (!lower_bound->schema()->Equals(*upper_bound->schema())) {
    data_->status_ = Status::InvalidArgument("range partition bounds must have matching schemas");
    return this;
  }
  if (data_->schema_ == nullptr) {
    data_->schema_ = lower_bound->schema();
  } else if (!lower_bound->schema()->Equals(*data_->schema_)) {
    data_->status_ = Status::InvalidArgument("range partition bounds must have matching schemas");
    return this;
  }

  Data::Step s { AlterTableRequestPB::DROP_RANGE_PARTITION,
                 nullptr,
                 unique_ptr<KuduPartialRow>(lower_bound),
                 unique_ptr<KuduPartialRow>(upper_bound),
                 lower_bound_type,
                 upper_bound_type };
  data_->steps_.emplace_back(std::move(s));
  data_->has_alter_partitioning_steps = true;
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

Status KuduTableAlterer::Alter() {
  AlterTableRequestPB req;
  RETURN_NOT_OK(data_->ToRequest(&req));

  MonoDelta timeout = data_->timeout_.Initialized() ?
    data_->timeout_ :
    data_->client_->default_admin_operation_timeout();
  MonoTime deadline = MonoTime::Now() + timeout;
  RETURN_NOT_OK(data_->client_->data_->AlterTable(data_->client_, req, deadline,
                                                  data_->has_alter_partitioning_steps));

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
    string alter_name = data_->rename_to_.get_value_or(data_->table_name_);
    RETURN_NOT_OK(data_->client_->data_->WaitForAlterTableToFinish(
        data_->client_, alter_name, deadline));
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
  if (data_->open_) {
    // Take ownership even if we return a bad status.
    delete pred;
    return Status::IllegalState("Predicate must be set before Open()");
  }
  return data_->mutable_configuration()->AddConjunctPredicate(pred);
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
  return data_->mutable_configuration()->AddLowerBoundPartitionKeyRaw(partition_key);
}

Status KuduScanner::AddExclusiveUpperBoundPartitionKeyRaw(const Slice& partition_key) {
  return data_->mutable_configuration()->AddUpperBoundPartitionKeyRaw(partition_key);
}

Status KuduScanner::SetCacheBlocks(bool cache_blocks) {
  if (data_->open_) {
    return Status::IllegalState("Block caching must be set before Open()");
  }
  return data_->mutable_configuration()->SetCacheBlocks(cache_blocks);
}

KuduSchema KuduScanner::GetProjectionSchema() const {
  return KuduSchema(*data_->configuration().projection());
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
    gscoped_ptr<CloseCallback> closer(new CloseCallback);
    closer->scanner_id = data_->next_req_.scanner_id();
    data_->PrepareRequest(KuduScanner::Data::CLOSE);
    data_->next_req_.set_close_scanner(true);
    closer->controller.set_timeout(data_->configuration().timeout());
    data_->proxy_->ScanAsync(data_->next_req_, &closer->response, &closer->controller,
                             boost::bind(&CloseCallback::Callback, closer.get()));
    ignore_result(closer.release());
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
  RETURN_NOT_OK(NextBatch(&data_->batch_for_old_api_));
  data_->batch_for_old_api_.data_->ExtractRows(rows);
  return Status::OK();
}

Status KuduScanner::NextBatch(KuduScanBatch* batch) {
  // TODO: do some double-buffering here -- when we return this batch
  // we should already have fired off the RPC for the next batch, but
  // need to do some swapping of the response objects around to avoid
  // stomping on the memory the user is looking at.
  CHECK(data_->open_);
  CHECK(data_->proxy_);

  batch->data_->Clear();

  if (data_->short_circuit_) {
    return Status::OK();
  }

  if (data_->data_in_open_) {
    // We have data from a previous scan.
    VLOG(2) << "Extracting data from " << data_->DebugString();
    data_->data_in_open_ = false;
    return batch->data_->Reset(&data_->controller_,
                               data_->configuration().projection(),
                               data_->configuration().client_projection(),
                                make_gscoped_ptr(data_->last_response_.release_data()));
  } else if (data_->last_response_.has_more_results()) {
    // More data is available in this tablet.
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
        return batch->data_->Reset(&data_->controller_,
                                   data_->configuration().projection(),
                                   data_->configuration().client_projection(),
                                   make_gscoped_ptr(data_->last_response_.release_data()));
      }

      data_->scan_attempts_++;

      // Error handling.
      set<string> blacklist;
      Status s = data_->HandleError(result, batch_deadline, &blacklist);
      if (!s.ok()) {
        LOG(WARNING) << "Scan at tablet server " << data_->ts_->ToString() << " of tablet "
                     << data_->DebugString() << " failed: " << result.status.ToString();
        return s;
      }

      if (data_->configuration().is_fault_tolerant()) {
        LOG(WARNING) << "Attempting to retry scan of tablet " << data_->DebugString()
                     << " elsewhere.";
        return data_->ReopenCurrentTablet(batch_deadline, &blacklist);
      }

      if (blacklist.empty()) {
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
    // No rows written, the next invocation will pick them up.
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
                                                    host_ports[0]);
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

Status KuduScanTokenBuilder::AddConjunctPredicate(KuduPredicate* pred) {
  return data_->mutable_configuration()->AddConjunctPredicate(pred);
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

} // namespace client
} // namespace kudu
