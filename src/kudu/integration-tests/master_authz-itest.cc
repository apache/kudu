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

#include <atomic>
#include <cstdlib>
#include <functional>
#include <initializer_list>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/common/common.pb.h"
#include "kudu/common/table_util.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/hms/hms_client.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/hms_itest-base.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/sentry_authz_provider-test-base.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/ranger/mini_ranger.h"
#include "kudu/ranger/ranger.pb.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/user_credentials.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/sentry/mini_sentry.h"
#include "kudu/sentry/sentry_client.h"
#include "kudu/sentry/sentry_policy_service_types.h"
#include "kudu/thrift/client.h"
#include "kudu/util/decimal_util.h"
#include "kudu/util/monotime.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using boost::make_optional;
using boost::none;
using boost::optional;
using kudu::client::KuduClient;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduScanToken;
using kudu::client::KuduScanTokenBuilder;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduTable;
using kudu::client::KuduTableAlterer;
using kudu::client::KuduTableCreator;
using kudu::client::sp::shared_ptr;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::hms::HmsClient;
using kudu::master::AlterRoleGrantPrivilege;
using kudu::master::CreateRoleAndAddToGroups;
using kudu::master::GetDatabasePrivilege;
using kudu::master::GetTableLocationsResponsePB;
using kudu::master::GetTabletLocationsResponsePB;
using kudu::master::GetTablePrivilege;
using kudu::master::MasterServiceProxy;
using kudu::master::ResetAuthzCacheRequestPB;
using kudu::master::ResetAuthzCacheResponsePB;
using kudu::master::VOTER_REPLICA;
using kudu::ranger::ActionPB;
using kudu::ranger::AuthorizationPolicy;
using kudu::ranger::MiniRanger;
using kudu::ranger::PolicyItem;
using kudu::rpc::RpcController;
using kudu::rpc::UserCredentials;
using kudu::sentry::SentryClient;
using sentry::TSentryGrantOption;
using sentry::TSentryPrivilege;
using std::atomic;
using std::function;
using std::move;
using std::ostream;
using std::string;
using std::thread;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace {
const char* const kAdminGroup = "admin";
const char* const kAdminUser = "test-admin";
const char* const kUserGroup = "user";
const char* const kTestUser = "test-user";
const char* const kImpalaUser = "impala";
const char* const kDevRole = "developer";
const char* const kAdminRole = "ad";
const char* const kDatabaseName = "db";
const char* const kTableName = "table";
const char* const kSecondTable = "second_table";
} // namespace

namespace kudu {

// Parameters for the operator functor (see below for OperationFunc).
struct OperationParams {
  // NOLINTNEXTLINE(google-explicit-constructor)
  OperationParams(string table_name,
                  string new_table_name = "")
      : table_name(std::move(table_name)),
        new_table_name(std::move(new_table_name)) {
  }
  const string table_name;
  const string new_table_name;
};

enum HarnessEnum {
  kSentry,
  kSentryWithCache,
  kRanger,
};
string HarnessEnumToString(HarnessEnum h) {
  switch (h) {
    case kSentry:
      return "SentryNoCache";
    case kSentryWithCache:
      return "SentryWithCache";
    case kRanger:
      return "Ranger";
  }
  return "";
}

// Parameters for the privilege functor (see below for PrivilegeFunc).
struct PrivilegeParams {
  // NOLINTNEXTLINE(google-explicit-constructor)
  PrivilegeParams(string db_name,
                  string table_name = "")
      : db_name(std::move(db_name)),
        table_name(std::move(table_name)) {
  }
  const string db_name;
  const string table_name;
};

class MasterAuthzITestHarness {
 public:
  virtual ~MasterAuthzITestHarness() {}

  static Status GetTableLocationsWithTableId(const string& table_name,
                                             const optional<const string&>& table_id,
                                             const unique_ptr<ExternalMiniCluster>& cluster) {
    const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
    std::shared_ptr<MasterServiceProxy> proxy = cluster->master_proxy();
    UserCredentials user_credentials;
    user_credentials.set_real_user(kTestUser);
    proxy->set_user_credentials(user_credentials);

    GetTableLocationsResponsePB table_locations;
    return itest::GetTableLocations(proxy, table_name, kTimeout, VOTER_REPLICA,
                                    table_id, &table_locations);
  }

  static Status IsCreateTableDone(const OperationParams& p,
                                  const shared_ptr<KuduClient>& client) {
    bool in_progress = false;
    return client->IsCreateTableInProgress(p.table_name, &in_progress);
  }

  static Status DropTable(const OperationParams& p,
                          const shared_ptr<KuduClient>& client) {
    return client->DeleteTable(p.table_name);
  }

  static Status AlterTable(const OperationParams& p,
                           const shared_ptr<KuduClient>& client) {
    unique_ptr<KuduTableAlterer> alterer(
        client->NewTableAlterer(p.table_name));
    return alterer->DropColumn("int32_val")->Alter();
  }

  static Status IsAlterTableDone(const OperationParams& p,
                                 const shared_ptr<KuduClient>& client) {
    bool in_progress = false;
    return client->IsAlterTableInProgress(p.table_name, &in_progress);
  }

  static Status RenameTable(const OperationParams& p,
                            const shared_ptr<KuduClient>& client) {
    unique_ptr<KuduTableAlterer> alterer(
        client->NewTableAlterer(p.table_name));
    return alterer->RenameTo(p.new_table_name)->Alter();
  }

  static Status GetTableSchema(const OperationParams& p,
                               const shared_ptr<KuduClient>& client) {
    KuduSchema schema;
    return client->GetTableSchema(p.table_name, &schema);
  }

  static Status GetTableLocations(const OperationParams& p,
                                  const unique_ptr<ExternalMiniCluster>& cluster) {
    return GetTableLocationsWithTableId(p.table_name, /*table_id=*/none, cluster);
  }

  static Status GetTabletLocations(const OperationParams& p,
                                   const unique_ptr<ExternalMiniCluster>& cluster) {
    // Log in as 'test-admin' to get the tablet ID.
    RETURN_NOT_OK(cluster->kdc()->Kinit(kAdminUser));
    shared_ptr<KuduClient> client;
    RETURN_NOT_OK(cluster->CreateClient(nullptr, &client));

    shared_ptr<KuduTable> kudu_table;
    RETURN_NOT_OK(client->OpenTable(p.table_name, &kudu_table));
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(kudu_table.get());
    RETURN_NOT_OK(builder.Build(&tokens));
    CHECK(!tokens.empty());
    const string& tablet_id = tokens[0]->tablet().id();

    // Log back as 'test-user'.
    RETURN_NOT_OK(cluster->kdc()->Kinit(kTestUser));

    static const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
    std::shared_ptr<MasterServiceProxy> proxy(cluster->master_proxy());
    UserCredentials user_credentials;
    user_credentials.set_real_user(kTestUser);
    proxy->set_user_credentials(user_credentials);

    GetTabletLocationsResponsePB tablet_locations;
    return itest::GetTabletLocations(proxy, tablet_id, kTimeout,
                                     VOTER_REPLICA, &tablet_locations);
  }

  static Status CreateKuduTable(const string& database_name,
                                const string& table_name,
                                const shared_ptr<client::KuduClient>& client,
                                MonoDelta timeout = {}) {
    // Get coverage of all column types.
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()
                      ->PrimaryKey()->Comment("The Primary Key");
    b.AddColumn("int8_val")->Type(KuduColumnSchema::INT8);
    b.AddColumn("int16_val")->Type(KuduColumnSchema::INT16);
    b.AddColumn("int32_val")->Type(KuduColumnSchema::INT32);
    b.AddColumn("int64_val")->Type(KuduColumnSchema::INT64);
    b.AddColumn("timestamp_val")->Type(KuduColumnSchema::UNIXTIME_MICROS);
    b.AddColumn("date_val")->Type(KuduColumnSchema::DATE);
    b.AddColumn("string_val")->Type(KuduColumnSchema::STRING);
    b.AddColumn("bool_val")->Type(KuduColumnSchema::BOOL);
    b.AddColumn("float_val")->Type(KuduColumnSchema::FLOAT);
    b.AddColumn("double_val")->Type(KuduColumnSchema::DOUBLE);
    b.AddColumn("binary_val")->Type(KuduColumnSchema::BINARY);
    b.AddColumn("decimal32_val")->Type(KuduColumnSchema::DECIMAL)
                                ->Precision(kMaxDecimal32Precision);
    b.AddColumn("decimal64_val")->Type(KuduColumnSchema::DECIMAL)
                                ->Precision(kMaxDecimal64Precision);
    b.AddColumn("decimal128_val")->Type(KuduColumnSchema::DECIMAL)
                                 ->Precision(kMaxDecimal128Precision);
    KuduSchema schema;
    RETURN_NOT_OK(b.Build(&schema));
    unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
    if (timeout.Initialized()) {
      // If specified, set the timeout for the operation.
      table_creator->timeout(timeout);
    }
    return table_creator->table_name(Substitute("$0.$1",
                                                database_name, table_name))
        .schema(&schema)
        .num_replicas(1)
        .set_range_partition_columns({ "key" })
        .Create();
  }

  static void CheckTable(const string& database_name,
                         const string& table_name,
                         const boost::optional<const string&>& /*user*/,
                         const unique_ptr<ExternalMiniCluster>& /*cluster*/,
                         const shared_ptr<KuduClient>& client,
                         const std::string& /*table_type*/ = hms::HmsClient::kManagedTable) {
    SCOPED_TRACE(Substitute("Checking table $0.$1", database_name, table_name));
    shared_ptr<KuduTable> table;
    ASSERT_OK(client->OpenTable(Substitute("$0.$1", database_name, table_name), &table));
  }

  // Creates db.table and db.second_table.
  virtual Status SetUpTables(const unique_ptr<ExternalMiniCluster>& cluster,
                             const shared_ptr<KuduClient>& client) {
    RETURN_NOT_OK(CreateKuduTable(kDatabaseName, kTableName, client));
    RETURN_NOT_OK(CreateKuduTable(kDatabaseName, kSecondTable, client));
    CheckTable(kDatabaseName, kTableName,
               make_optional<const string&>(kAdminUser), cluster, client);
    CheckTable(kDatabaseName, kSecondTable,
               make_optional<const string&>(kAdminUser), cluster, client);
    return Status::OK();
  }

  // Returns a set of opts appropriate for an authorization test.
  ExternalMiniClusterOptions GetClusterOpts() {
    ExternalMiniClusterOptions opts;
    // Always enable Kerberos, as Sentry/Ranger deployments do not make sense
    // in non-Kerberized environments.
    opts.enable_kerberos = true;
    // Add 'impala' as trusted user who may access the cluster without being
    // authorized.
    opts.extra_master_flags.emplace_back("--trusted_user_acl=impala");
    opts.extra_master_flags.emplace_back("--user_acl=test-user,impala");
    SetUpExternalMiniServiceOpts(&opts);
    return opts;
  }

  virtual Status GrantCreateTablePrivilege(const PrivilegeParams& p) = 0;
  virtual Status GrantDropTablePrivilege(const PrivilegeParams& p) = 0;
  virtual Status GrantAlterTablePrivilege(const PrivilegeParams& p) = 0;
  virtual Status GrantRenameTablePrivilege(const PrivilegeParams& p) = 0;
  virtual Status GrantGetMetadataTablePrivilege(const PrivilegeParams& p) = 0;
  virtual Status GrantGetMetadataDatabasePrivilege(const PrivilegeParams& p) = 0;
  virtual Status CreateTable(const OperationParams& p,
                             const shared_ptr<KuduClient>& client) = 0;
  virtual void CheckTableDoesNotExist(const string& database_name, const string& table_name,
                                      shared_ptr<KuduClient> client) = 0;

  // Start or stop the authorization services.
  virtual Status StartAuthzProvider(const unique_ptr<ExternalMiniCluster>& cluster) = 0;
  virtual Status StopAuthzProvider(const unique_ptr<ExternalMiniCluster>& cluster) = 0;

  // Stop authorization service clients.
  virtual void TearDown() = 0;

  // Adds to 'opts' any options specific to the authorization service.
  virtual void SetUpExternalMiniServiceOpts(ExternalMiniClusterOptions* opts) = 0;

  // Sets up credentials such that kAdminUser has access to everything in
  // kDatabaseName.
  virtual Status SetUpCredentials() = 0;

  // Sets things up so we can begin sending requests to the authorization
  // services (whether that setup is an actual SentryClient or the ability to
  // send curl requests to MiniRanger).
  virtual Status SetUpExternalServiceClients(const unique_ptr<ExternalMiniCluster>& cluster) = 0;
};

// Test Master authorization enforcement with Sentry and HMS
// integration enabled.
class SentryITestHarness : public MasterAuthzITestHarness,
                           public HmsITestHarness {
 public:
  using MasterAuthzITestHarness::CreateKuduTable;
  using HmsITestHarness::CheckTable;
  Status StopAuthzProvider(const unique_ptr<ExternalMiniCluster>& cluster) override {
    RETURN_NOT_OK(sentry_client_->Stop());
    RETURN_NOT_OK(cluster->sentry()->Stop());
    return Status::OK();
  }

  Status StartAuthzProvider(const unique_ptr<ExternalMiniCluster>& cluster) override {
    RETURN_NOT_OK(cluster->sentry()->Start());
    RETURN_NOT_OK(cluster->kdc()->Kinit("kudu"));
    RETURN_NOT_OK(sentry_client_->Start());
    return Status::OK();
  }

  void CheckTableDoesNotExist(const string& database_name, const string& table_name,
                              shared_ptr<KuduClient> client) override {
    return HmsITestHarness::CheckTableDoesNotExist(database_name, table_name, client);
  }

  Status GrantCreateTablePrivilege(const PrivilegeParams& p) override {
    return AlterRoleGrantPrivilege(sentry_client_.get(), kDevRole,
        GetDatabasePrivilege(p.db_name, "CREATE"));
  }

  Status GrantDropTablePrivilege(const PrivilegeParams& p) override {
    return AlterRoleGrantPrivilege(sentry_client_.get(), kDevRole,
        GetTablePrivilege(p.db_name, p.table_name, "DROP"));
  }

  Status GrantAlterTablePrivilege(const PrivilegeParams& p) override {
    return AlterRoleGrantPrivilege(sentry_client_.get(), kDevRole,
        GetTablePrivilege(p.db_name, p.table_name, "ALTER"));
  }

  Status GrantRenameTablePrivilege(const PrivilegeParams& p) override {
    RETURN_NOT_OK(AlterRoleGrantPrivilege(sentry_client_.get(), kDevRole,
        GetTablePrivilege(p.db_name, p.table_name, "ALL")));
    return AlterRoleGrantPrivilege(sentry_client_.get(), kDevRole,
        GetDatabasePrivilege(p.db_name, "CREATE"));
  }

  Status GrantGetMetadataTablePrivilege(const PrivilegeParams& p) override {
    return AlterRoleGrantPrivilege(sentry_client_.get(), kDevRole,
        GetTablePrivilege(p.db_name, p.table_name, "METADATA"));
  }

  Status GrantGetMetadataDatabasePrivilege(const PrivilegeParams& p) override {
    return AlterRoleGrantPrivilege(sentry_client_.get(), kDevRole,
        GetDatabasePrivilege(p.db_name, "METADATA"));
  }

  Status CreateTable(const OperationParams& p,
                     const shared_ptr<KuduClient>& client) override {
    Slice hms_database;
    Slice hms_table;
    RETURN_NOT_OK(ParseHiveTableIdentifier(p.table_name,
                                           &hms_database, &hms_table));
    return MasterAuthzITestHarness::CreateKuduTable(hms_database.ToString(),
                                                    hms_table.ToString(), client);
  }

  Status SetUpTables(const unique_ptr<ExternalMiniCluster>& cluster,
                     const shared_ptr<KuduClient>& client) override {
    // First create database 'db' in the HMS.
    RETURN_NOT_OK(CreateDatabase(kDatabaseName));
    // Then create Kudu tables 'table' and 'second_table', owned by user
    // 'test-admin'.
    return MasterAuthzITestHarness::SetUpTables(cluster, client);
  }

  void TearDown() override {
    if (sentry_client_) {
      ASSERT_OK(sentry_client_->Stop());
    }
    if (hms_client_) {
      ASSERT_OK(hms_client_->Stop());
    }
  }

  void SetUpExternalMiniServiceOpts(ExternalMiniClusterOptions* opts) override {
    opts->enable_sentry = true;
    // Configure the timeout to reduce the run time of tests that involve
    // re-connections.
    const string timeout = AllowSlowTests() ? "5" : "2";
    opts->hms_mode = HmsMode::ENABLE_METASTORE_INTEGRATION;
    opts->extra_master_flags.emplace_back(
       Substitute("$0=$1", "--sentry_service_send_timeout_seconds", timeout));
    opts->extra_master_flags.emplace_back(
       Substitute("$0=$1", "--sentry_service_recv_timeout_seconds", timeout));
    // NOTE: this can be overwritten if another value is added to the end.
    opts->extra_master_flags.emplace_back("--sentry_privileges_cache_capacity_mb=0");
  }

  Status SetUpExternalServiceClients(const unique_ptr<ExternalMiniCluster>& cluster) override {
    thrift::ClientOptions hms_opts;
    hms_opts.enable_kerberos = true;
    hms_opts.service_principal = "hive";
    RETURN_NOT_OK(HmsITestHarness::RestartHmsClient(cluster, hms_opts));

    thrift::ClientOptions sentry_opts;
    sentry_opts.enable_kerberos = true;
    sentry_opts.service_principal = "sentry";
    sentry_client_.reset(new SentryClient(cluster->sentry()->address(), sentry_opts));
    return sentry_client_->Start();
  }

  Status SetUpCredentials() override {
    // User to Role mapping:
    // 1. user -> developer,
    // 2. admin -> admin.
    RETURN_NOT_OK(CreateRoleAndAddToGroups(sentry_client_.get(), kDevRole, kUserGroup));
    RETURN_NOT_OK(CreateRoleAndAddToGroups(sentry_client_.get(), kAdminRole, kAdminGroup));

    // Grant privilege 'ALL' on database 'db' to role admin.
    TSentryPrivilege privilege = GetDatabasePrivilege(
        kDatabaseName, "ALL",
        TSentryGrantOption::DISABLED);
    return AlterRoleGrantPrivilege(sentry_client_.get(), kAdminRole, privilege);
  }

 protected:
  unique_ptr<SentryClient> sentry_client_;
};

class SentryWithCacheITestHarness : public SentryITestHarness {
 public:
  void SetUpExternalMiniServiceOpts(ExternalMiniClusterOptions* opts) override {
    NO_FATALS(SentryITestHarness::SetUpExternalMiniServiceOpts(opts));
    opts->extra_master_flags.emplace_back("--sentry_privileges_cache_capacity_mb=1");
  }
};

class RangerITestHarness : public MasterAuthzITestHarness {
 public:
  static constexpr int kSleepAfterNewPolicyMs = 1200;

  Status GrantCreateTablePrivilege(const PrivilegeParams& p) override {
    AuthorizationPolicy policy;
    policy.databases.emplace_back(p.db_name);
    // IsCreateTableDone() requires METADATA on the table level.
    policy.tables.emplace_back("*");
    policy.items.emplace_back(PolicyItem({kTestUser}, {ActionPB::CREATE}));
    RETURN_NOT_OK(ranger_->AddPolicy(move(policy)));
    SleepFor(MonoDelta::FromMilliseconds(kSleepAfterNewPolicyMs));

    return Status::OK();
  }

  Status GrantDropTablePrivilege(const PrivilegeParams& p) override {
    AuthorizationPolicy policy;
    policy.databases.emplace_back(p.db_name);
    policy.tables.emplace_back(p.table_name);
    policy.items.emplace_back(PolicyItem({kTestUser}, {ActionPB::DROP}));
    RETURN_NOT_OK(ranger_->AddPolicy(move(policy)));
    SleepFor(MonoDelta::FromMilliseconds(kSleepAfterNewPolicyMs));

    return Status::OK();
  }

  void CheckTableDoesNotExist(const string& database_name, const string& table_name,
                              shared_ptr<KuduClient> client) override {
    shared_ptr<KuduTable> table;
    Status s = client->OpenTable(Substitute("$0.$1", database_name, table_name), &table);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  }

  Status GrantAlterTablePrivilege(const PrivilegeParams& p) override {
    AuthorizationPolicy policy;
    policy.databases.emplace_back(p.db_name);
    policy.tables.emplace_back(p.table_name);
    policy.items.emplace_back(PolicyItem({kTestUser}, {ActionPB::ALTER}));
    RETURN_NOT_OK(ranger_->AddPolicy(move(policy)));
    SleepFor(MonoDelta::FromMilliseconds(kSleepAfterNewPolicyMs));
    return Status::OK();
  }

  Status GrantRenameTablePrivilege(const PrivilegeParams& p) override {
    AuthorizationPolicy policy_new_table;
    policy_new_table.databases.emplace_back(p.db_name);
    // IsCreateTableDone() requires METADATA on the table level.
    policy_new_table.tables.emplace_back("*");
    policy_new_table.items.emplace_back(PolicyItem({kTestUser}, {ActionPB::CREATE}));
    RETURN_NOT_OK(ranger_->AddPolicy(move(policy_new_table)));

    AuthorizationPolicy policy;
    policy.databases.emplace_back(p.db_name);
    policy.tables.emplace_back(p.table_name);
    policy.items.emplace_back(PolicyItem({kTestUser}, {ActionPB::ALL}));
    RETURN_NOT_OK(ranger_->AddPolicy(move(policy)));
    SleepFor(MonoDelta::FromMilliseconds(kSleepAfterNewPolicyMs));
    return Status::OK();
  }

  Status GrantGetMetadataDatabasePrivilege(const PrivilegeParams& p) override {
    AuthorizationPolicy policy;
    policy.databases.emplace_back(p.db_name);
    policy.items.emplace_back(PolicyItem({kTestUser}, {ActionPB::METADATA}));
    RETURN_NOT_OK(ranger_->AddPolicy(move(policy)));
    SleepFor(MonoDelta::FromMilliseconds(kSleepAfterNewPolicyMs));
    return Status::OK();
  }

  Status GrantGetMetadataTablePrivilege(const PrivilegeParams& p) override {
    AuthorizationPolicy policy;
    policy.databases.emplace_back(p.db_name);
    policy.tables.emplace_back(p.table_name);
    policy.items.emplace_back(PolicyItem({kTestUser}, {ActionPB::METADATA}));
    RETURN_NOT_OK(ranger_->AddPolicy(move(policy)));
    SleepFor(MonoDelta::FromMilliseconds(kSleepAfterNewPolicyMs));
    return Status::OK();
  }

  Status CreateTable(const OperationParams& p,
                     const shared_ptr<KuduClient>& client) override {
    string ranger_db;
    Slice ranger_table;
    RETURN_NOT_OK(ParseRangerTableIdentifier(p.table_name,
                                             &ranger_db, &ranger_table));
    return CreateKuduTable(ranger_db, ranger_table.ToString(), client);
  }

  Status StartAuthzProvider(const unique_ptr<ExternalMiniCluster>& cluster) override {
    return cluster->ranger()->Start();
  }

  Status StopAuthzProvider(const unique_ptr<ExternalMiniCluster>& cluster) override {
    return cluster->ranger()->Stop();
  }

  void TearDown() override {}

 protected:
  void SetUpExternalMiniServiceOpts(ExternalMiniClusterOptions* opts) override {
    opts->enable_ranger = true;
  }

  Status SetUpExternalServiceClients(const unique_ptr<ExternalMiniCluster>& cluster) override {
    ranger_ = cluster->ranger();
    return Status::OK();
  }

  Status SetUpCredentials() override {
    AuthorizationPolicy policy;
    policy.databases.emplace_back(kDatabaseName);
    policy.tables.emplace_back("*");
    policy.items.emplace_back(PolicyItem({kAdminUser}, {ActionPB::ALL}));
    RETURN_NOT_OK(ranger_->AddPolicy(move(policy)));
    SleepFor(MonoDelta::FromMilliseconds(kSleepAfterNewPolicyMs));
    return Status::OK();
  }

 private:
  // Points to the Ranger instance used by the harness. This can be used to set
  // policies.
  MiniRanger* ranger_;
};

// Test basic master authorization enforcement with Sentry and HMS integration
// enabled.
class MasterAuthzITestBase : public ExternalMiniClusterITestBase {
 public:
  void SetUp() override {
    NO_FATALS(ExternalMiniClusterITestBase::SetUp());
  }

  void SetUpCluster(HarnessEnum harness) {
    switch (harness) {
      case kSentry:
        harness_.reset(new SentryITestHarness());
        break;
      case kSentryWithCache:
        harness_.reset(new SentryWithCacheITestHarness());
        break;
      case kRanger:
        harness_.reset(new RangerITestHarness());
        break;
      default:
        LOG(FATAL) << "unknown harness";
    }
    ExternalMiniClusterOptions opts = harness_->GetClusterOpts();
    NO_FATALS(StartClusterWithOpts(std::move(opts)));

    // Create principals 'impala' and 'kudu'. Configure to use the latter.
    ASSERT_OK(cluster_->kdc()->CreateUserPrincipal(kImpalaUser));
    ASSERT_OK(cluster_->kdc()->CreateUserPrincipal("kudu"));
    ASSERT_OK(cluster_->kdc()->Kinit("kudu"));

    ASSERT_OK(harness_->SetUpExternalServiceClients(cluster_));
    ASSERT_OK(harness_->SetUpCredentials());
    ASSERT_OK(harness_->SetUpTables(cluster_, client_));

    // Log in as 'test-user' and reset the client to pick up the change in user.
    ASSERT_OK(cluster_->kdc()->Kinit(kTestUser));
    ASSERT_OK(cluster_->CreateClient(nullptr, &client_));
  }

  void TearDown() override {
    harness_->TearDown();
    ExternalMiniClusterITestBase::TearDown();
  }

  Status GetTableLocationsWithTableId(const string& table_name,
                                      const optional<const string&>& table_id) {
    return harness_->GetTableLocationsWithTableId(table_name, table_id, cluster_);
  }

  Status GrantCreateTablePrivilege(const PrivilegeParams& p) {
    return harness_->GrantCreateTablePrivilege(p);
  }

  Status GrantDropTablePrivilege(const PrivilegeParams& p) {
    return harness_->GrantDropTablePrivilege(p);
  }

  Status GrantAlterTablePrivilege(const PrivilegeParams& p) {
    return harness_->GrantAlterTablePrivilege(p);
  }

  Status GrantRenameTablePrivilege(const PrivilegeParams& p) {
    return harness_->GrantRenameTablePrivilege(p);
  }

  Status GrantGetMetadataTablePrivilege(const PrivilegeParams& p) {
    return harness_->GrantGetMetadataTablePrivilege(p);
  }

  Status GrantGetMetadataDatabasePrivilege(const PrivilegeParams& p) {
    return harness_->GrantGetMetadataDatabasePrivilege(p);
  }

  Status CreateTable(const OperationParams& p) {
    return harness_->CreateTable(p, client_);
  }

  Status IsCreateTableDone(const OperationParams& p) {
    return harness_->IsCreateTableDone(p, client_);
  }

  Status DropTable(const OperationParams& p) {
    return harness_->DropTable(p, client_);
  }

  Status AlterTable(const OperationParams& p) {
    return harness_->AlterTable(p, client_);
  }

  Status IsAlterTableDone(const OperationParams& p) {
    return harness_->IsAlterTableDone(p, client_);
  }

  Status RenameTable(const OperationParams& p) {
    return harness_->RenameTable(p, client_);
  }

  Status GetTableSchema(const OperationParams& p) {
    return harness_->GetTableSchema(p, client_);
  }

  Status GetTableLocations(const OperationParams& p) {
    return harness_->GetTableLocations(p, cluster_);
  }

  Status GetTabletLocations(const OperationParams& p) {
    return harness_->GetTabletLocations(p, cluster_);
  }

  Status CreateKuduTable(const std::string& database_name,
                         const std::string& table_name,
                         MonoDelta timeout = {}) {
    return harness_->CreateKuduTable(database_name, table_name, client_, timeout);
  }

  void CheckTable(const std::string& database_name,
                  const std::string& table_name,
                  const boost::optional<const std::string&>& user,
                  const std::string& table_type = hms::HmsClient::kManagedTable) {
    harness_->CheckTable(database_name, table_name, user, cluster_, client_, table_type);
  }

  void CheckTableDoesNotExist(const std::string& database_name,
                              const std::string& table_name) {
    harness_->CheckTableDoesNotExist(database_name, table_name, client_);
  }

  Status StartAuthzProvider() {
    return harness_->StartAuthzProvider(cluster_);
  }

  Status StopAuthzProvider() {
    return harness_->StopAuthzProvider(cluster_);
  }

 protected:
  std::unique_ptr<MasterAuthzITestHarness> harness_;
};

class MasterAuthzITest : public MasterAuthzITestBase,
                         public ::testing::WithParamInterface<HarnessEnum> {
 public:
  void SetUp() override {
    NO_FATALS(MasterAuthzITestBase::SetUp());
    NO_FATALS(SetUpCluster(GetParam()));
  }
};

INSTANTIATE_TEST_CASE_P(AuthzProviders, MasterAuthzITest,
    ::testing::Values(kSentry, kRanger),
    [] (const testing::TestParamInfo<MasterAuthzITest::ParamType>& info) {
      return HarnessEnumToString(info.param);
    });

TEST_P(MasterAuthzITest, TestCreateTableAuthorized) {
  ASSERT_OK(cluster_->kdc()->Kinit(kAdminUser));
  ASSERT_OK(cluster_->CreateClient(nullptr, &client_));

  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()
                    ->PrimaryKey();
  KuduSchema schema;
  ASSERT_OK(b.Build(&schema));
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(Substitute("$0.$1",
                                      kDatabaseName, "another_table"))
    .schema(&schema)
    .num_replicas(1)
    .set_range_partition_columns({"key"})
    .Create());
}

TEST_P(MasterAuthzITest, TestCreateTableUnauthorized) {
  ASSERT_OK(cluster_->kdc()->Kinit(kTestUser));
  ASSERT_OK(cluster_->CreateClient(nullptr, &client_));

  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()
                    ->PrimaryKey();
  KuduSchema schema;
  ASSERT_OK(b.Build(&schema));
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_TRUE(table_creator->table_name(Substitute("$0.$1",
                                        kDatabaseName, "another_table"))
    .schema(&schema)
    .num_replicas(1)
    .set_range_partition_columns({"key"})
    .Create().IsNotAuthorized());
}

// Test that the trusted user can access the cluster without being authorized.
TEST_P(MasterAuthzITest, TestTrustedUserAcl) {
  // Log in as 'impala' and reset the client to pick up the change in user.
  ASSERT_OK(this->cluster_->kdc()->Kinit(kImpalaUser));
  ASSERT_OK(this->cluster_->CreateClient(nullptr, &this->client_));

  vector<string> tables;
  ASSERT_OK(this->client_->ListTables(&tables));
  unordered_set<string> tables_set(tables.begin(), tables.end());
  ASSERT_EQ(unordered_set<string>({ Substitute("$0.$1", kDatabaseName, kTableName),
                                    Substitute("$0.$1", kDatabaseName, kSecondTable) }),
            tables_set);

  ASSERT_OK(this->CreateKuduTable(kDatabaseName, "new_table"));
  NO_FATALS(this->CheckTable(kDatabaseName, "new_table",
                             make_optional<const string&>(kImpalaUser)));
}

TEST_P(MasterAuthzITest, TestAuthzListTables) {
  // ListTables is not parameterized as other operations below, because non-authorized
  // tables will be filtered instead of returning NOT_AUTHORIZED error.
  const auto table_name = Substitute("$0.$1", kDatabaseName, kTableName);
  const auto sec_table_name = Substitute("$0.$1", kDatabaseName, kSecondTable);

  // Listing tables shows nothing without proper privileges.
  vector<string> tables;
  ASSERT_OK(this->client_->ListTables(&tables));
  ASSERT_TRUE(tables.empty());

  // Listing tables only shows the tables which user has proper privileges on.
  tables.clear();
  ASSERT_OK(this->GrantGetMetadataTablePrivilege({ kDatabaseName, kTableName }));
  ASSERT_OK(this->client_->ListTables(&tables));
  ASSERT_EQ(vector<string>({ table_name }), tables);

  tables.clear();
  ASSERT_OK(this->GrantGetMetadataTablePrivilege({ kDatabaseName, kSecondTable }));
  ASSERT_OK(this->client_->ListTables(&tables));
  unordered_set<string> tables_set(tables.begin(), tables.end());
  ASSERT_EQ(unordered_set<string>({ table_name, sec_table_name }), tables_set);
}

// When authorizing ListTables, if there is a concurrent rename, we may not end
// up showing the table.
TEST_P(MasterAuthzITest, TestAuthzListTablesConcurrentRename) {
  ASSERT_OK(this->cluster_->SetFlag(this->cluster_->master(),
      "catalog_manager_inject_latency_list_authz_ms", "3000"));;
  const auto table_name = Substitute("$0.$1", kDatabaseName, kTableName);
  const auto sec_table_name = Substitute("$0.$1", kDatabaseName, kSecondTable);
  ASSERT_OK(this->GrantGetMetadataTablePrivilege({ kDatabaseName, kTableName }));
  ASSERT_OK(this->GrantRenameTablePrivilege({ kDatabaseName, kTableName }));

  // List the tables while injecting latency.
  vector<string> tables;
  thread t([&] {
    ASSERT_OK(this->client_->ListTables(&tables));
  });

  // While that's happening, rename one of the tables.
  ASSERT_OK(this->RenameTable({ table_name, Substitute("$0.$1", kDatabaseName, "b") }));
  NO_FATALS(t.join());

  // We shouldn't see the renamed table.
  ASSERT_EQ(1, tables.size());
  ASSERT_EQ(sec_table_name, tables[0]);
}

// Test that when the client passes a table identifier with the table name
// and table ID refer to different tables, the client needs permission on
// both tables for returning TABLE_NOT_FOUND error to avoid leaking table
// existence.
TEST_P(MasterAuthzITest, TestMismatchedTable) {
  const auto table_name_a = Substitute("$0.$1", kDatabaseName, kTableName);
  const auto table_name_b = Substitute("$0.$1", kDatabaseName, kSecondTable);

  // Log in as 'test-admin' to get the tablet ID.
  ASSERT_OK(this->cluster_->kdc()->Kinit(kAdminUser));
  shared_ptr<KuduClient> client;
  ASSERT_OK(this->cluster_->CreateClient(nullptr, &client));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client->OpenTable(table_name_a, &table));
  optional<const string&> table_id_a = make_optional<const string&>(table->id());

  // Log back as 'test-user'.
  ASSERT_OK(this->cluster_->kdc()->Kinit(kTestUser));

  Status s = this->GetTableLocationsWithTableId(table_name_b, table_id_a);
  ASSERT_TRUE(s.IsNotAuthorized());
  ASSERT_STR_MATCHES(s.ToString(), "[Uu]nauthorized action");

  ASSERT_OK(this->GrantGetMetadataTablePrivilege({ kDatabaseName, kTableName }));
  s = this->GetTableLocationsWithTableId(table_name_b, table_id_a);
  ASSERT_TRUE(s.IsNotAuthorized());
  ASSERT_STR_MATCHES(s.ToString(), "[Uu]nauthorized action");

  ASSERT_OK(this->GrantGetMetadataTablePrivilege({ kDatabaseName, kSecondTable }));
  s = this->GetTableLocationsWithTableId(table_name_b, table_id_a);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_STR_CONTAINS(s.ToString(), "the table ID refers to a different table");
}

// Functor that performs a certain operation (e.g. Create, Rename) on a table
// given its name and its desired new name, if necessary (only used for Rename).
typedef function<Status(MasterAuthzITestBase*, const OperationParams&)> OperatorFunc;

// Functor that grants the required permission for an operation (e.g Create,
// Rename) on a table given the database the table belongs to and the name of
// the table, if applicable.
typedef function<Status(MasterAuthzITestBase*, const PrivilegeParams&)> PrivilegeFunc;

// A description of the operation function that describes a particular action
// on a table a user can perform, as well as the privilege granting function
// that grants the required permission to the user to perform the action.
struct AuthzFuncs {
  OperatorFunc do_action;
  PrivilegeFunc grant_privileges;
  string description;
};
ostream& operator <<(ostream& out, const AuthzFuncs& d) {
  out << d.description;
  return out;
}

// A description of an authorization process, including the protected resource (table),
// the operation function, as well as the privilege granting function.
struct AuthzDescriptor {
  AuthzFuncs funcs;
  string database;
  string table_name;
  string new_table_name;
};
ostream& operator <<(ostream& out, const AuthzDescriptor& d) {
  out << d.funcs.description;
  return out;
}

class TestAuthzTable :
    public MasterAuthzITestBase,
    public ::testing::WithParamInterface<std::tuple<HarnessEnum, AuthzDescriptor>> {
 public:
  void SetUp() override {
    NO_FATALS(MasterAuthzITestBase::SetUp());
    NO_FATALS(SetUpCluster(std::get<0>(GetParam())));
  }
};

TEST_P(TestAuthzTable, TestAuthorizeTable) {
  const AuthzDescriptor& desc = std::get<1>(GetParam());
  const auto table_name = Substitute("$0.$1", desc.database, desc.table_name);
  const auto new_table_name = Substitute("$0.$1",
                                         desc.database, desc.new_table_name);
  const OperationParams action_params = { table_name, new_table_name };
  const PrivilegeParams privilege_params = { desc.database, desc.table_name };

  // User 'test-user' attempts to operate on the table without proper privileges.
  Status s = desc.funcs.do_action(this, action_params);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  ASSERT_STR_MATCHES(s.ToString(), "[Uu]nauthorized action");

  // User 'test-user' can operate on the table after obtaining proper privileges.
  ASSERT_OK(desc.funcs.grant_privileges(this, privilege_params));
  ASSERT_OK(desc.funcs.do_action(this, action_params));

  // Ensure that operating on a table while the Sentry is unreachable fails.
  // No such guarantee exists for Ranger, which caches policies in its clients.
  if (std::get<0>(GetParam()) != kRanger) {
    ASSERT_OK(StopAuthzProvider());
    s = desc.funcs.do_action(this, action_params);
    ASSERT_FALSE(s.ok()) << s.ToString();
  }
}

static const AuthzDescriptor kAuthzCombinations[] = {
    {
      {
        &MasterAuthzITestBase::CreateTable,
        &MasterAuthzITestBase::GrantCreateTablePrivilege,
        "CreateTable",
      },
      kDatabaseName,
      "new_table",
      ""
    },
    {
      {
        &MasterAuthzITestBase::DropTable,
        &MasterAuthzITestBase::GrantDropTablePrivilege,
        "DropTable",
      },
      kDatabaseName,
      kTableName,
      ""
    },
    {
      {
        &MasterAuthzITestBase::AlterTable,
        &MasterAuthzITestBase::GrantAlterTablePrivilege,
        "AlterTable",
      },
      kDatabaseName,
      kTableName,
      ""
    },
    {
      {
        &MasterAuthzITestBase::RenameTable,
        &MasterAuthzITestBase::GrantRenameTablePrivilege,
        "RenameTable",
      },
      kDatabaseName,
      kTableName,
      "new_table"
    },
    {
      {
        &MasterAuthzITestBase::GetTableSchema,
        &MasterAuthzITestBase::GrantGetMetadataTablePrivilege,
        "GetTableSchema",
      },
      kDatabaseName,
      kTableName,
      ""
    },
    {
      {
        &MasterAuthzITestBase::GetTableLocations,
        &MasterAuthzITestBase::GrantGetMetadataTablePrivilege,
        "GetTableLocations",
      },
      kDatabaseName,
      kTableName,
      ""
    },
    {
      {
        &MasterAuthzITestBase::GetTabletLocations,
        &MasterAuthzITestBase::GrantGetMetadataTablePrivilege,
        "GetTabletLocations",
      },
      kDatabaseName,
      kTableName,
      ""
    },
    {
      {
        &MasterAuthzITestBase::IsCreateTableDone,
        &MasterAuthzITestBase::GrantGetMetadataTablePrivilege,
        "IsCreateTableDone",
      },
      kDatabaseName,
      kTableName,
      ""
    },
    {
      {
        &MasterAuthzITestBase::IsAlterTableDone,
        &MasterAuthzITestBase::GrantGetMetadataTablePrivilege,
        "IsAlterTableDone",
      },
      kDatabaseName,
      kTableName,
      ""
    },
};
INSTANTIATE_TEST_CASE_P(AuthzCombinations,
                        TestAuthzTable,
                        ::testing::Combine(
                            ::testing::Values(kSentry, kRanger),
                            ::testing::ValuesIn(kAuthzCombinations)),
                        [] (const testing::TestParamInfo<TestAuthzTable::ParamType>& info) {
                          return Substitute("$0_$1", HarnessEnumToString(std::get<0>(info.param)),
                                            std::get<1>(info.param).funcs.description);
                        });

class MasterSentryITest : public MasterAuthzITestBase {
 public:
  void SetUp() override {
    NO_FATALS(MasterAuthzITestBase::SetUp());
    NO_FATALS(SetUpCluster(kSentry));
  }
};

// Checks the user with table ownership automatically has ALL privilege on the
// table. User 'test-user' can delete the same table without specifically
// granting 'DROP ON TABLE'. Note that ownership population between the HMS and
// the Sentry service happens synchronously, therefore, the table deletion
// should succeed right after the table creation.
// NOTE: this behavior is specific to Sentry,  so we don't parameterize.
TEST_F(MasterSentryITest, TestTableOwnership) {
  ASSERT_OK(GrantCreateTablePrivilege({ kDatabaseName }));
  ASSERT_OK(CreateKuduTable(kDatabaseName, "new_table"));
  NO_FATALS(CheckTable(kDatabaseName, "new_table",
                       make_optional<const string&>(kTestUser)));

  // TODO(hao): test create a table with a different owner than the client’s username?
  ASSERT_OK(client_->DeleteTable(Substitute("$0.$1", kDatabaseName, "new_table")));
  NO_FATALS(CheckTableDoesNotExist(kDatabaseName, "new_table"));
}

// Checks Sentry privileges are synchronized upon table rename in the HMS.
TEST_F(MasterSentryITest, TestRenameTablePrivilegeTransfer) {
  ASSERT_OK(GrantRenameTablePrivilege({ kDatabaseName, kTableName }));
  ASSERT_OK(RenameTable({ Substitute("$0.$1", kDatabaseName, kTableName),
                          Substitute("$0.$1", kDatabaseName, "b") }));
  NO_FATALS(CheckTable(kDatabaseName, "b",
                       make_optional<const string&>(kAdminUser)));

  unique_ptr<KuduTableAlterer> alterer(client_->NewTableAlterer(
      Substitute("$0.$1", kDatabaseName, "b")));
  alterer->DropColumn("int16_val");

  // Note that unlike table creation, there could be a delay between the table renaming
  // in Kudu and the privilege renaming in Sentry. Because Kudu uses the transactional
  // listener of the HMS to get notification of table alteration events, while Sentry
  // uses post event listener (which is executed outside the HMS transaction). There
  // is a chance that Kudu already finish the table renaming but the privilege renaming
  // hasn't been reflected in the Sentry service.
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(alterer->Alter());
  });
  NO_FATALS(CheckTable(kDatabaseName, "b", make_optional<const string&>(kAdminUser)));
}

class AuthzErrorHandlingTest :
    public MasterAuthzITestBase,
    public ::testing::WithParamInterface<std::tuple<HarnessEnum, AuthzFuncs>> {
    // TODO(aserbin): update the test to introduce authz privilege caching
 public:
  void SetUp() override {
    NO_FATALS(MasterAuthzITestBase::SetUp());
    NO_FATALS(SetUpCluster(std::get<0>(GetParam())));
  }
};
TEST_P(AuthzErrorHandlingTest, TestNonExistentTable) {
  static constexpr const char* const kTableName = "non_existent";
  const AuthzFuncs& funcs = std::get<1>(GetParam());
  const auto table_name = Substitute("$0.$1", kDatabaseName, kTableName);
  const auto new_table_name = Substitute("$0.$1", kDatabaseName, "b");
  const OperationParams action_params = { table_name, new_table_name };
  const PrivilegeParams privilege_params = { kDatabaseName, kTableName };

  // Ensure that operating on a non-existent table without proper privileges gives
  // a NOT_AUTHORIZED error, instead of leaking the table existence by giving a
  // TABLE_NOT_FOUND error.
  Status s = funcs.do_action(this, action_params);
  ASSERT_TRUE(s.IsNotAuthorized());
  ASSERT_STR_MATCHES(s.ToString(), "[Uu]nauthorized action");

  // Ensure that operating on a non-existent table fails while Sentry is
  // unreachable. No such guarantee exists for Ranger.
  if (std::get<0>(GetParam()) != kRanger) {
    ASSERT_OK(StopAuthzProvider());
    s = funcs.do_action(this, action_params);
    ASSERT_TRUE(s.IsNetworkError()) << s.ToString();

    // Ensure that operating on a non-existent table with proper privileges gives a
    // TABLE_NOT_FOUND error.
    ASSERT_OK(StartAuthzProvider());
  }
  ASSERT_EVENTUALLY([&] {
    // SentryAuthzProvider throttles reconnections, so it's necessary to wait
    // out the backoff.
    ASSERT_OK(funcs.grant_privileges(this, privilege_params));
  });
  s = funcs.do_action(this, action_params);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
}

static const AuthzFuncs kAuthzFuncCombinations[] = {
    {
      &MasterAuthzITestBase::DropTable,
      &MasterAuthzITestBase::GrantDropTablePrivilege,
      "DropTable"
    },
    {
      &MasterAuthzITestBase::AlterTable,
      &MasterAuthzITestBase::GrantAlterTablePrivilege,
      "AlterTable"
    },
    {
      &MasterAuthzITestBase::RenameTable,
      &MasterAuthzITestBase::GrantRenameTablePrivilege,
      "RenameTable"
    },
    {
      &MasterAuthzITestBase::GetTableSchema,
      &MasterAuthzITestBase::GrantGetMetadataTablePrivilege,
      "GetTableSchema"
    },
    {
      &MasterAuthzITestBase::GetTableLocations,
      &MasterAuthzITestBase::GrantGetMetadataTablePrivilege,
      "GetTableLocations"
    },
    {
      &MasterAuthzITestBase::IsCreateTableDone,
      &MasterAuthzITestBase::GrantGetMetadataTablePrivilege,
      "IsCreateTableDone"
    },
    {
      &MasterAuthzITestBase::IsAlterTableDone,
      &MasterAuthzITestBase::GrantGetMetadataTablePrivilege,
      "IsAlterTableDone"
    },
};

INSTANTIATE_TEST_CASE_P(AuthzFuncCombinations,
                        AuthzErrorHandlingTest,
                        ::testing::Combine(
                            ::testing::Values(kSentry, kRanger),
                            ::testing::ValuesIn(kAuthzFuncCombinations)),
                        [] (const testing::TestParamInfo<AuthzErrorHandlingTest::ParamType>& info) {
                          return Substitute("$0_$1", HarnessEnumToString(std::get<0>(info.param)),
                                            std::get<1>(info.param).description);
                        });

// Class for test scenarios verifying functionality of managing AuthzProvider's
// privileges cache via Kudu RPC.
class SentryAuthzProviderCacheITest : public MasterAuthzITestBase {
 public:
  void SetUp() override {
    NO_FATALS(MasterAuthzITestBase::SetUp());
    NO_FATALS(SetUpCluster(kSentryWithCache));
  }

  Status ResetCache() {
    // ResetAuthzCache() RPC requires admin/superuser credentials, so this
    // method calls Kinit(kAdminUser) to authenticate appropriately. However,
    // it's necessary to return back the credentials of the regular user after
    // resetting the cache since the rest of the scenario is supposed to run
    // without superuser credentials.
    SCOPED_CLEANUP({
      WARN_NOT_OK(cluster_->kdc()->Kinit(kTestUser),
                  "could not restore Kerberos credentials");
    });
    RETURN_NOT_OK(cluster_->kdc()->Kinit(kAdminUser));
    std::shared_ptr<MasterServiceProxy> proxy = cluster_->master_proxy();
    UserCredentials user_credentials;
    user_credentials.set_real_user(kAdminUser);
    proxy->set_user_credentials(user_credentials);

    RpcController ctl;
    ResetAuthzCacheResponsePB res;
    RETURN_NOT_OK(proxy->ResetAuthzCache(
        ResetAuthzCacheRequestPB(), &res, &ctl));
    return res.has_error() ? StatusFromPB(res.error().status()) : Status::OK();
  }
};

// This test scenario uses AlterTable() to make sure AuthzProvider's cache
// empties upon successful ResetAuthzCache() RPC.
TEST_F(SentryAuthzProviderCacheITest, AlterTable) {
  const auto table_name = Substitute("$0.$1", kDatabaseName, kTableName);
  ASSERT_OK(GrantAlterTablePrivilege({ kDatabaseName, kTableName }));
  {
    unique_ptr<KuduTableAlterer> table_alterer(
        client_->NewTableAlterer(table_name)->DropColumn("int8_val"));
    auto s = table_alterer->Alter();
    ASSERT_TRUE(s.ok()) << s.ToString();
  }
  ASSERT_OK(StopAuthzProvider());
  {
    unique_ptr<KuduTableAlterer> table_alterer(
        client_->NewTableAlterer(table_name)->DropColumn("int16_val"));
    auto s = table_alterer->Alter();
    ASSERT_TRUE(s.ok()) << s.ToString();
  }
  ASSERT_OK(ResetCache());
  {
    // After resetting the cache, it should not be possible to perform another
    // ALTER TABLE operation: no entries are in the cache, so authz provider
    // needs to fetch information from Sentry directly.
    unique_ptr<KuduTableAlterer> table_alterer(
        client_->NewTableAlterer(table_name)->DropColumn("int32_val"));
    auto s = table_alterer->Alter();
    ASSERT_TRUE(s.IsNetworkError()) << s.ToString();
  }

  // Try to do the same after starting Sentry back. It should be a success.
  ASSERT_OK(StartAuthzProvider());
  {
    unique_ptr<KuduTableAlterer> table_alterer(
        client_->NewTableAlterer(table_name)->DropColumn("int32_val"));
    auto s = table_alterer->Alter();
    ASSERT_TRUE(s.ok()) << s.ToString();
  }
}

// This test scenario calls a couple of authz methods of SentryAuthzProvider
// while resetting its fetcher's cache in parallel using master's
// ResetAuthzCache() RPC.
TEST_F(SentryAuthzProviderCacheITest, ResetAuthzCacheConcurrentAlterTable) {
  constexpr const auto num_threads = 16;
  const auto run_interval = AllowSlowTests() ? MonoDelta::FromSeconds(8)
                                             : MonoDelta::FromSeconds(1);
  ASSERT_OK(GrantCreateTablePrivilege({ kDatabaseName }));
  for (auto idx = 0; idx < num_threads; ++idx) {
    ASSERT_OK(CreateTable(Substitute("$0.$1", kDatabaseName, idx)));
    ASSERT_OK(GrantAlterTablePrivilege({ kDatabaseName, Substitute("$0", idx) }));
  }

  vector<Status> threads_task_status(num_threads);
  {
    atomic<bool> stopped(false);
    vector<thread> threads;

    SCOPED_CLEANUP({
      stopped = true;
      for (auto& thread : threads) {
        thread.join();
      }
    });

    for (auto idx = 0; idx < num_threads; ++idx) {
      const auto thread_idx = idx;
      threads.emplace_back([&, thread_idx] () {
        const auto table_name = Substitute("$0.$1", kDatabaseName, thread_idx);

        while (!stopped) {
          SleepFor(MonoDelta::FromMicroseconds((rand() % 2 + 1) * thread_idx));
          {
            unique_ptr<KuduTableAlterer> table_alterer(
               client_->NewTableAlterer(table_name));
            table_alterer->DropColumn("int8_val");

            auto s = table_alterer->Alter();
            if (!s.ok()) {
              threads_task_status[thread_idx] = s;
              return;
            }
          }

          SleepFor(MonoDelta::FromMicroseconds((rand() % 3 + 1) * thread_idx));
          {
            unique_ptr<KuduTableAlterer> table_alterer(
                client_->NewTableAlterer(table_name));
            table_alterer->AddColumn("int8_val")->Type(KuduColumnSchema::INT8);
            auto s = table_alterer->Alter();
            if (!s.ok()) {
              threads_task_status[thread_idx] = s;
              return;
            }
          }
        }
      });
    }

    const auto time_beg = MonoTime::Now();
    const auto time_end = time_beg + run_interval;
    while (MonoTime::Now() < time_end) {
      SleepFor(MonoDelta::FromMilliseconds((rand() % 3)));
      ASSERT_OK(ResetCache());
    }
  }
  for (auto idx = 0; idx < threads_task_status.size(); ++idx) {
    SCOPED_TRACE(Substitute("results for thread $0", idx));
    const auto& s = threads_task_status[idx];
    EXPECT_TRUE(s.ok()) << s.ToString();
  }
}

// This test scenario documents an artifact of the Kudu+HMS+Sentry integration
// when authz cache is enabled (the cache is enabled by default). In essence,
// information on the ownership of a table created during a short period of
// Sentry's unavailability will not ever appear in Sentry. That might be
// misleading because CreateTable() reports a success to the client. The created
// table indeed exists and is fully functional otherwise, but the corresponding
// owner privilege record is absent in Sentry.
//
// TODO(aserbin): clarify why it works with HEAD of the master branches
//                of Sentry/Hive but fails with Sentry 2.1.0 and Hive 2.1.1.
TEST_F(SentryAuthzProviderCacheITest, DISABLED_CreateTables) {
  constexpr const char* const kGhostTables[] = { "t10", "t11" };

  // Grant CREATE TABLE and METADATA privileges on the database.
  ASSERT_OK(GrantCreateTablePrivilege({ kDatabaseName }));
  ASSERT_OK(GrantGetMetadataDatabasePrivilege({ kDatabaseName }));

  // Make sure it's possible to create a table in the database. This also
  // populates the privileges cache with information on the privileges
  // granted on the database.
  ASSERT_OK(CreateKuduTable(kDatabaseName, "t0"));

  // An attempt to open a not-yet-existing table will fetch the information
  // on the granted privileges on the table into the privileges cache.
  for (const auto& t : kGhostTables) {
    shared_ptr<KuduTable> kudu_table;
    const auto s = client_->OpenTable(Substitute("$0.$1", kDatabaseName, t),
                                      &kudu_table);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  }

  ASSERT_OK(StopAuthzProvider());

  // CreateTable() with operation timeout longer than HMS --> Sentry
  // communication timeout successfully completes. After failing to push
  // the information on the newly created table to Sentry due to the logic
  // implemented in the SentrySyncHMSNotificationsPostEventListener plugin,
  // HMS sends success response to Kudu master and Kudu successfully completes
  // the rest of the steps.
  ASSERT_OK(CreateKuduTable(kDatabaseName, kGhostTables[0]));

  // In this case, the timeout for the CreateTable RPC is set to be lower than
  // the HMS --> Sentry communication timeout (see corresponding parameters
  // of the MiniHms::EnableSentry() method). CreateTable() successfully passes
  // the authz phase since the information on privileges is cached and no
  // Sentry RPC calls are attempted. However, since Sentry is down,
  // CreateTable() takes a long time on the HMS's side and the client's
  // request times out, while the creation of the table continues in the
  // background.
  {
    const auto s = CreateKuduTable(kDatabaseName, kGhostTables[1],
                                   MonoDelta::FromSeconds(1));
    ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
  }

  // Before starting Sentry, make sure the abandoned request to create the
  // latter table succeeded even if CreateKuduTable() reported timeout.
  // This is to make sure HMS stopped trying to push notification
  // on table creation to Sentry anymore via the metastore plugin
  // SentrySyncHMSNotificationsPostEventListener.
  ASSERT_EVENTUALLY([&]{
    bool exists;
    ASSERT_OK(client_->TableExists(
        Substitute("$0.$1", kDatabaseName, kGhostTables[1]), &exists));
    ASSERT_TRUE(exists);
  });

  ASSERT_OK(ResetCache());

  // After resetting the cache, it should not be possible to create another
  // table: authz provider needs to fetch information on privileges directly
  // from Sentry, but it's still down.
  {
    const auto s = CreateKuduTable(kDatabaseName, "t2");
    ASSERT_TRUE(s.IsNetworkError()) << s.ToString();
  }

  ASSERT_OK(StartAuthzProvider());

  // Try to create the table after starting Sentry back: it should be a success.
  ASSERT_OK(CreateKuduTable(kDatabaseName, "t2"));

  // Once table has been created, it should be possible to perform DDL operation
  // on it since the user is the owner of the table.
  {
    unique_ptr<KuduTableAlterer> table_alterer(
        client_->NewTableAlterer(Substitute("$0.$1", kDatabaseName, "t2")));
    table_alterer->AddColumn("new_int8_columun")->Type(KuduColumnSchema::INT8);
    ASSERT_OK(table_alterer->Alter());
  }

  // Try to run DDL against the tables created during Sentry's downtime. These
  // should not be authorized since Sentry didn't received information on the
  // ownership of those tables from HMS during their creation and there isn't
  // any catch up for those events made after Sentry started.
  for (const auto& t : kGhostTables) {
    unique_ptr<KuduTableAlterer> table_alterer(
        client_->NewTableAlterer(Substitute("$0.$1", kDatabaseName, t)));
    table_alterer->AddColumn("new_int8_columun")->Type(KuduColumnSchema::INT8);
    auto s = table_alterer->Alter();
    ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

    // After granting the ALTER TABLE privilege the alteration should be
    // successful. One caveat: it's necessary to reset the cache since the cache
    // will not have the new record till the current entry hasn't yet expired.
    ASSERT_OK(GrantAlterTablePrivilege({ kDatabaseName, t }));
    ASSERT_OK(ResetCache());
    ASSERT_OK(table_alterer->Alter());
  }
}

// Basic test to verify access control and functionality of
// the ResetAuthzCache(); integration with Sentry is not enabled.
class AuthzCacheControlTest : public ExternalMiniClusterITestBase {
 public:
  void SetUp() override {
    ExternalMiniClusterITestBase::SetUp();
    ExternalMiniClusterOptions opts;
    opts.enable_kerberos = true;
    StartClusterWithOpts(opts);
  }

  // Utility method to call master's ResetAuthzCache RPC under the credentials
  // of the specified 'user'. The credentials should have been set appropriately
  // before calling this method (e.g., call kinit if necessary).
  Status ResetCache(const string& user,
                    RpcController* ctl,
                    ResetAuthzCacheResponsePB* resp) {
    RETURN_NOT_OK(cluster_->kdc()->Kinit(user));
    std::shared_ptr<MasterServiceProxy> proxy = cluster_->master_proxy();
    UserCredentials user_credentials;
    user_credentials.set_real_user(user);
    proxy->set_user_credentials(user_credentials);

    ResetAuthzCacheRequestPB req;
    return proxy->ResetAuthzCache(req, resp, ctl);
  }
};

TEST_F(AuthzCacheControlTest, ResetCacheNoSentryIntegration) {
  // Non-admin users (i.e. those not in --superuser_acl) are not allowed
  // to reset authz cache.
  for (const auto& user : { "test-user", "joe-interloper", }) {
    ASSERT_OK(cluster_->kdc()->Kinit(user));
    ResetAuthzCacheResponsePB resp;
    RpcController ctl;
    auto s = ResetCache(user, &ctl, &resp);
    ASSERT_TRUE(s.IsRemoteError()) << s.ToString();
    ASSERT_FALSE(resp.has_error());

    const auto* err_status = ctl.error_response();
    ASSERT_NE(nullptr, err_status);
    ASSERT_TRUE(err_status->has_code());
    ASSERT_EQ(rpc::ErrorStatusPB::ERROR_APPLICATION, err_status->code());
    ASSERT_STR_CONTAINS(err_status->message(),
                        "unauthorized access to method: ResetAuthzCache");
  }

  // The cache can be reset with credentials of a super-user. However, in
  // case if integration with Sentry is not enabled, the AuthzProvider
  // doesn't have any cache.
  {
    ResetAuthzCacheResponsePB resp;
    RpcController ctl;
    const auto s = ResetCache("test-admin", &ctl, &resp);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(nullptr, ctl.error_response());
    ASSERT_TRUE(resp.has_error()) << resp.error().DebugString();
    const auto app_s = StatusFromPB(resp.error().status());
    ASSERT_TRUE(app_s.IsNotSupported()) << app_s.ToString();
    ASSERT_STR_CONTAINS(app_s.ToString(),
                        "provider does not have privileges cache");
  }
}

// A test for the ValidateSentryServiceRpcAddresses group flag validator.
// The only existing test scenario covers only the negative case, while
// several other Sentry-related (and not) tests provide good coverage
// for all the positive cases.
class MasterSentryAndHmsFlagsTest : public KuduTest {
};

TEST_F(MasterSentryAndHmsFlagsTest, MasterRefuseToStart) {
  // The code below results in setting the --sentry_service_rpc_addresses flag
  // to the mini-sentry's RPC address, but leaving the --hive_metastore_uris
  // flag unset (i.e. its value is an empty string). Such a combination of flag
  // settings makes it impossible to start Kudu master.
  cluster::ExternalMiniClusterOptions opts;
  opts.enable_kerberos = true;
  opts.enable_sentry = true;
  opts.hms_mode = HmsMode::NONE;

  cluster::ExternalMiniCluster cluster(std::move(opts));
  const auto s = cluster.Start();
  const auto msg = s.ToString();
  ASSERT_TRUE(s.IsRuntimeError()) << msg;
  ASSERT_STR_CONTAINS(msg, "failed to start masters: Unable to start Master");
  ASSERT_STR_CONTAINS(msg, "kudu-master: process exited with non-zero status 1");
}

} // namespace kudu
