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

#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/common/table_util.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/hms/hms_client.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/ranger/mini_ranger.h"
#include "kudu/ranger/ranger.pb.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/user_credentials.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/transactions/txn_system_client.h"
#include "kudu/util/decimal_util.h"
#include "kudu/util/monotime.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

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
using kudu::master::GetTableLocationsResponsePB;
using kudu::master::GetTabletLocationsResponsePB;
using kudu::master::RefreshAuthzCacheRequestPB;
using kudu::master::RefreshAuthzCacheResponsePB;
using kudu::master::MasterServiceProxy;
using kudu::master::VOTER_REPLICA;
using kudu::ranger::ActionPB;
using kudu::ranger::AuthorizationPolicy;
using kudu::ranger::MiniRanger;
using kudu::ranger::PolicyItem;
using kudu::rpc::RpcController;
using kudu::rpc::UserCredentials;
using kudu::transactions::TxnSystemClient;
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
const char* const kAdminUser = "test-admin";
const char* const kTestUser = "test-user";
const char* const kSecondUser = "alice";
const char* const kImpalaUser = "impala";
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
  kRanger,
};

string HarnessEnumToString(HarnessEnum h) {
  switch (h) {
    case kRanger:
      return "Ranger";
  }
  return "";
}

// Parameters for the privilege functor (see below for PrivilegeFunc).
struct PrivilegeParams {
  // NOLINTNEXTLINE(google-explicit-constructor)
  PrivilegeParams(string db_name,
                  string table_name = "",
                  string user_name = kTestUser)
      : db_name(std::move(db_name)),
        table_name(std::move(table_name)),
        user_name(std::move(user_name)) {
  }
  const string db_name;
  const string table_name;
  const string user_name;
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

  static Status RefreshAuthzPolicies(const unique_ptr<ExternalMiniCluster>& cluster) {
    RefreshAuthzCacheRequestPB req;
    RefreshAuthzCacheResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    RETURN_NOT_OK(cluster->master_proxy()->RefreshAuthzCache(req, &resp, &rpc));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }
    return Status::OK();
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

  static Status ChangeOwner(const OperationParams& p,
                            const shared_ptr<KuduClient>& client) {
    unique_ptr<KuduTableAlterer> alterer(
        client->NewTableAlterer(p.table_name));

    return alterer->SetOwner(kTestUser)->Alter();
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
        .set_owner(kTestUser)
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
    static const string kUser = kAdminUser;
    RETURN_NOT_OK(CreateKuduTable(kDatabaseName, kTableName, client));
    RETURN_NOT_OK(CreateKuduTable(kDatabaseName, kSecondTable, client));
    CheckTable(kDatabaseName, kTableName, kUser, cluster, client);
    CheckTable(kDatabaseName, kSecondTable, kUser, cluster, client);
    return Status::OK();
  }

  // Returns a set of opts appropriate for an authorization test.
  ExternalMiniClusterOptions GetClusterOpts() {
    ExternalMiniClusterOptions opts;
    // Always enable Kerberos, as Authz deployments do not make sense
    // in non-Kerberized environments.
    opts.enable_kerberos = true;
    // Add 'impala' as trusted user who may access the cluster without being
    // authorized.
    opts.extra_master_flags.emplace_back("--trusted_user_acl=impala");
    opts.extra_master_flags.emplace_back("--user_acl=test-user,impala,alice");
    SetUpExternalMiniServiceOpts(&opts);
    return opts;
  }

  virtual Status GrantCreateTablePrivilege(const PrivilegeParams& p,
                                           const unique_ptr<ExternalMiniCluster>& cluster) = 0;
  virtual Status GrantDropTablePrivilege(const PrivilegeParams& p,
                                         const unique_ptr<ExternalMiniCluster>& cluster) = 0;
  virtual Status GrantAlterTablePrivilege(const PrivilegeParams& p,
                                          const unique_ptr<ExternalMiniCluster>& cluster) = 0;
  virtual Status GrantAlterWithGrantTablePrivilege(
      const PrivilegeParams& p,
      const unique_ptr<ExternalMiniCluster>& cluster) = 0;
  virtual Status GrantRenameTablePrivilege(const PrivilegeParams& p,
                                           const unique_ptr<ExternalMiniCluster>& cluster) = 0;
  virtual Status GrantGetMetadataTablePrivilege(const PrivilegeParams& p,
                                                const unique_ptr<ExternalMiniCluster>& cluster) = 0;
  virtual Status GrantGetMetadataDatabasePrivilege(const PrivilegeParams& p,
                                                   const unique_ptr<ExternalMiniCluster>& cluster)
    = 0;
  virtual Status GrantAllTablePrivilege(const PrivilegeParams& p,
                                        const unique_ptr<ExternalMiniCluster>& cluster) = 0;
  virtual Status GrantAllDatabasePrivilege(const PrivilegeParams& p,
                                           const unique_ptr<ExternalMiniCluster>& cluster) = 0;
  virtual Status GrantAllWithGrantTablePrivilege(const PrivilegeParams& p,
                                                 const unique_ptr<ExternalMiniCluster>& cluster)
    = 0;
  virtual Status GrantAllWithGrantDatabasePrivilege(
      const PrivilegeParams& p,
      const unique_ptr<ExternalMiniCluster>& cluster) = 0;
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
  // services.
  virtual Status SetUpExternalServiceClients(const unique_ptr<ExternalMiniCluster>& cluster) = 0;
};

class RangerITestHarness : public MasterAuthzITestHarness {
 public:
  static constexpr int kSleepAfterNewPolicyMs = 1400;

  Status GrantCreateTablePrivilege(const PrivilegeParams& p,
                                   const unique_ptr<ExternalMiniCluster>& cluster) override {
    AuthorizationPolicy policy;
    policy.databases.emplace_back(p.db_name);
    // IsCreateTableDone() requires METADATA on the table level.
    policy.tables.emplace_back("*");
    policy.items.emplace_back(PolicyItem({p.user_name}, {ActionPB::CREATE}, false));
    RETURN_NOT_OK(ranger_->AddPolicy(move(policy)));
    return RefreshAuthzPolicies(cluster);
  }

  Status GrantDropTablePrivilege(const PrivilegeParams& p,
                                 const unique_ptr<ExternalMiniCluster>& cluster) override {
    AuthorizationPolicy policy;
    policy.databases.emplace_back(p.db_name);
    policy.tables.emplace_back(p.table_name);
    policy.items.emplace_back(PolicyItem({p.user_name}, {ActionPB::DROP}, false));
    RETURN_NOT_OK(ranger_->AddPolicy(move(policy)));
    return RefreshAuthzPolicies(cluster);
  }

  void CheckTableDoesNotExist(const string& database_name, const string& table_name,
                              shared_ptr<KuduClient> client) override {
    shared_ptr<KuduTable> table;
    Status s = client->OpenTable(Substitute("$0.$1", database_name, table_name), &table);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  }

  Status GrantAlterTablePrivilege(const PrivilegeParams& p,
                                  const unique_ptr<ExternalMiniCluster>& cluster) override {
    AuthorizationPolicy policy;
    policy.databases.emplace_back(p.db_name);
    policy.tables.emplace_back(p.table_name);
    policy.items.emplace_back(PolicyItem({p.user_name}, {ActionPB::ALTER}, false));
    RETURN_NOT_OK(ranger_->AddPolicy(move(policy)));
    return RefreshAuthzPolicies(cluster);
  }

  Status GrantAlterWithGrantTablePrivilege(
      const PrivilegeParams& p,
      const unique_ptr<ExternalMiniCluster>& cluster) override {
    AuthorizationPolicy policy;
    policy.databases.emplace_back(p.db_name);
    policy.tables.emplace_back(p.table_name);
    policy.items.emplace_back(PolicyItem({p.user_name}, {ActionPB::ALTER}, true));
    RETURN_NOT_OK(ranger_->AddPolicy(move(policy)));
    return RefreshAuthzPolicies(cluster);
  }

  Status GrantRenameTablePrivilege(const PrivilegeParams& p,
                                   const unique_ptr<ExternalMiniCluster>& cluster) override {
    AuthorizationPolicy policy_new_table;
    policy_new_table.databases.emplace_back(p.db_name);
    // IsCreateTableDone() requires METADATA on the table level.
    policy_new_table.tables.emplace_back("*");
    policy_new_table.items.emplace_back(PolicyItem({p.user_name}, {ActionPB::CREATE}, false));
    RETURN_NOT_OK(ranger_->AddPolicy(move(policy_new_table)));

    AuthorizationPolicy policy;
    policy.databases.emplace_back(p.db_name);
    policy.tables.emplace_back(p.table_name);
    policy.items.emplace_back(PolicyItem({p.user_name}, {ActionPB::ALL}, false));
    RETURN_NOT_OK(ranger_->AddPolicy(move(policy)));
    return RefreshAuthzPolicies(cluster);
  }

  Status GrantGetMetadataDatabasePrivilege(const PrivilegeParams& p,
                                           const unique_ptr<ExternalMiniCluster>& cluster)
        override {
    AuthorizationPolicy policy;
    policy.databases.emplace_back(p.db_name);
    policy.items.emplace_back(PolicyItem({p.user_name}, {ActionPB::METADATA}, false));
    RETURN_NOT_OK(ranger_->AddPolicy(move(policy)));
    return RefreshAuthzPolicies(cluster);
  }

  Status GrantGetMetadataTablePrivilege(const PrivilegeParams& p,
                                        const unique_ptr<ExternalMiniCluster>& cluster) override {
    AuthorizationPolicy policy;
    policy.databases.emplace_back(p.db_name);
    policy.tables.emplace_back(p.table_name);
    policy.items.emplace_back(PolicyItem({p.user_name}, {ActionPB::METADATA}, false));
    RETURN_NOT_OK(ranger_->AddPolicy(move(policy)));
    return RefreshAuthzPolicies(cluster);
  }

  Status GrantAllTablePrivilege(const PrivilegeParams& p,
                                const unique_ptr<ExternalMiniCluster>& cluster) override {
    AuthorizationPolicy policy;
    policy.databases.emplace_back(p.db_name);
    policy.tables.emplace_back(p.table_name);
    policy.items.emplace_back(PolicyItem({p.user_name}, {ActionPB::ALL}, false));
    RETURN_NOT_OK(ranger_->AddPolicy(move(policy)));
    return RefreshAuthzPolicies(cluster);
  }

  Status GrantAllDatabasePrivilege(const PrivilegeParams& p,
                                   const unique_ptr<ExternalMiniCluster>& cluster) override {
    AuthorizationPolicy db_policy;
    db_policy.databases.emplace_back(p.db_name);
    db_policy.items.emplace_back(PolicyItem({p.user_name}, {ActionPB::ALL}, false));
    RETURN_NOT_OK(ranger_->AddPolicy(move(db_policy)));
    AuthorizationPolicy tbl_policy;
    tbl_policy.databases.emplace_back(p.db_name);
    tbl_policy.tables.emplace_back("*");
    tbl_policy.items.emplace_back(PolicyItem({p.user_name}, {ActionPB::ALL}, false));
    RETURN_NOT_OK(ranger_->AddPolicy(move(tbl_policy)));
    return RefreshAuthzPolicies(cluster);
  }

  Status GrantAllWithGrantTablePrivilege(const PrivilegeParams& p,
                                         const unique_ptr<ExternalMiniCluster>& cluster) override {
    AuthorizationPolicy policy;
    policy.databases.emplace_back(p.db_name);
    policy.tables.emplace_back(p.table_name);
    policy.items.emplace_back(PolicyItem({p.user_name}, {ActionPB::ALL}, true));
    RETURN_NOT_OK(ranger_->AddPolicy(move(policy)));
    return RefreshAuthzPolicies(cluster);
  }

  Status GrantAllWithGrantDatabasePrivilege(
      const PrivilegeParams& p,
      const unique_ptr<ExternalMiniCluster>& cluster) override {
    AuthorizationPolicy db_policy;
    db_policy.databases.emplace_back(p.db_name);
    db_policy.items.emplace_back(PolicyItem({p.user_name}, {ActionPB::ALL}, true));
    RETURN_NOT_OK(ranger_->AddPolicy(move(db_policy)));
    AuthorizationPolicy tbl_policy;
    tbl_policy.databases.emplace_back(p.db_name);
    tbl_policy.tables.emplace_back("*");
    tbl_policy.items.emplace_back(PolicyItem({p.user_name}, {ActionPB::ALL}, true));
    RETURN_NOT_OK(ranger_->AddPolicy(move(tbl_policy)));
    return RefreshAuthzPolicies(cluster);
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
    policy.items.emplace_back(PolicyItem({kAdminUser}, {ActionPB::ALL}, true));
    return ranger_->AddPolicy(move(policy));
  }

 private:
  // Points to the Ranger instance used by the harness. This can be used to set
  // policies.
  MiniRanger* ranger_;
};

// Test basic master authorization enforcement.
class MasterAuthzITestBase : public ExternalMiniClusterITestBase {
 public:
  void SetUp() override {
    NO_FATALS(ExternalMiniClusterITestBase::SetUp());
  }

  void SetUpCluster(HarnessEnum harness) {
    switch (harness) {
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
    ASSERT_OK(cluster_->kdc()->CreateUserPrincipal(kSecondUser));
    ASSERT_OK(cluster_->kdc()->Kinit("kudu"));

    ASSERT_OK(harness_->SetUpExternalServiceClients(cluster_));
    ASSERT_OK(harness_->SetUpCredentials());
    ASSERT_OK(harness_->RefreshAuthzPolicies(cluster_));
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
    return harness_->GrantCreateTablePrivilege(p, cluster_);
  }

  Status GrantDropTablePrivilege(const PrivilegeParams& p) {
    return harness_->GrantDropTablePrivilege(p, cluster_);
  }

  Status GrantAlterTablePrivilege(const PrivilegeParams& p) {
    return harness_->GrantAlterTablePrivilege(p, cluster_);
  }

  Status GrantRenameTablePrivilege(const PrivilegeParams& p) {
    return harness_->GrantRenameTablePrivilege(p, cluster_);
  }

  Status GrantGetMetadataTablePrivilege(const PrivilegeParams& p) {
    return harness_->GrantGetMetadataTablePrivilege(p, cluster_);
  }

  Status GrantAlterWithGrantTablePrivilege(const PrivilegeParams& p) {
    return harness_->GrantAlterWithGrantTablePrivilege(p, cluster_);
  }

  Status GrantAllTablePrivilege(const PrivilegeParams& p) {
    return harness_->GrantAllTablePrivilege(p, cluster_);
  }

  Status GrantAllDatabasePrivilege(const PrivilegeParams& p) {
    return harness_->GrantAllDatabasePrivilege(p, cluster_);
  }

  Status GrantAllWithGrantTablePrivilege(const PrivilegeParams& p) {
    return harness_->GrantAllWithGrantTablePrivilege(p, cluster_);
  }

  Status GrantAllWithGrantDatabasePrivilege(const PrivilegeParams& p) {
    return harness_->GrantAllWithGrantDatabasePrivilege(p, cluster_);
  }

  Status GrantGetMetadataDatabasePrivilege(const PrivilegeParams& p) {
    return harness_->GrantGetMetadataDatabasePrivilege(p, cluster_);
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

  Status ChangeOwner(const OperationParams& p) {
    return harness_->ChangeOwner(p, client_);
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

INSTANTIATE_TEST_SUITE_P(AuthzProviders, MasterAuthzITest,
    ::testing::Values(kRanger),
    [] (const testing::TestParamInfo<MasterAuthzITest::ParamType>& info) {
      return HarnessEnumToString(info.param);
    });

// Test that creation of the transaction status table foregoes fine-grained
// authorization.
TEST_P(MasterAuthzITest, TestCreateTransactionStatusTable) {
  // Create a transaction status table and add a range. Both requests should
  // succeed, despite no privileges being granted in Ranger.
  ASSERT_OK(this->cluster_->kdc()->Kinit(kTestUser));
  // Attempting to call system client DDL as a non-admin should result in a
  // NotAuthorized error.
  {
    unique_ptr<TxnSystemClient> non_admin_client;
    ASSERT_OK(TxnSystemClient::Create(cluster_->master_rpc_addrs(),
                                      cluster_->service_principal(),
                                      &non_admin_client));
    Status s = non_admin_client->CreateTxnStatusTable(100);
    ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
    s = non_admin_client->AddTxnStatusTableRange(100, 200);
    ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  }
  // But as service user, we should have no trouble making the calls.
  ASSERT_OK(this->cluster_->kdc()->Kinit(kAdminUser));
  unique_ptr<TxnSystemClient> txn_sys_client;
  ASSERT_OK(TxnSystemClient::Create(cluster_->master_rpc_addrs(),
                                    cluster_->service_principal(),
                                    &txn_sys_client));
  ASSERT_OK(txn_sys_client->CreateTxnStatusTable(100));
  ASSERT_OK(txn_sys_client->AddTxnStatusTableRange(100, 200));
}

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

TEST_P(MasterAuthzITest, TestCreateTableDifferentOwner) {
  ASSERT_OK(cluster_->kdc()->Kinit(kTestUser));
  ASSERT_OK(cluster_->CreateClient(nullptr, &client_));

  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()
                    ->PrimaryKey();
  KuduSchema schema;
  ASSERT_OK(b.Build(&schema));
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());

  const string kTableName = "another_table";
  PrivilegeParams p = { kDatabaseName, kTableName };
  this->GrantAllDatabasePrivilege(p);
  this->GrantAllTablePrivilege(p);
  this->GrantAllWithGrantTablePrivilege(p);
  ASSERT_TRUE(table_creator->table_name(Substitute("$0.$1", kDatabaseName,
                                                   kTableName))
    .schema(&schema)
    .num_replicas(1)
    .set_range_partition_columns({"key"})
    .set_owner("another_user")
    .Create().IsNotAuthorized());

  this->GrantAllWithGrantDatabasePrivilege(p);

  ASSERT_OK(table_creator->Create());
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

  // Create another table with a different owner
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()
                    ->PrimaryKey();
  KuduSchema schema;
  ASSERT_OK(b.Build(&schema));
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());

  const string kTableName = "another_table";
  ASSERT_OK(table_creator->table_name(Substitute("$0.$1", kDatabaseName,
                                                 kTableName))
    .schema(&schema)
    .num_replicas(1)
    .set_range_partition_columns({"key"})
    .set_owner("another_user")
    .Create());
  const string user = kImpalaUser;
  NO_FATALS(this->CheckTable(kDatabaseName, "new_table", user));
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
  ASSERT_OK(this->GrantGetMetadataTablePrivilege(
      {kDatabaseName, kSecondTable, "{OWNER}"}));
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

TEST_P(MasterAuthzITest, TestAuthzGiveAwayOwnership) {
  this->GrantAllWithGrantTablePrivilege({ kDatabaseName, kTableName, "{OWNER}"});

  // We need to grant metadata permissions to the user, otherwise the ownership
  // change would time out due to lack of privileges when checking the alter
  // table progress.
  this->GrantGetMetadataTablePrivilege({ kDatabaseName, kTableName, kTestUser });
  const string table_name = Substitute("$0.$1", kDatabaseName, kTableName);

  // Change table owner.
  {
    unique_ptr<KuduTableAlterer> alterer(
        this->client_->NewTableAlterer(table_name));
    ASSERT_OK(alterer->SetOwner(kSecondUser)->Alter());
  }

  // Attempt to drop a column after as a non-owner.
  {
    unique_ptr<KuduTableAlterer> alterer(
        this->client_->NewTableAlterer(table_name));
    Status s = alterer->DropColumn("int8_val")->Alter();
    ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  }

  // Login as the new owner, create a new client, and alter the table.
  {
    ASSERT_OK(this->cluster_->kdc()->Kinit(kSecondUser));
    ASSERT_OK(this->cluster_->CreateClient(nullptr, &this->client_));
    unique_ptr<KuduTableAlterer> alterer(
        this->client_->NewTableAlterer(table_name));
    ASSERT_OK(alterer->DropColumn("int8_val")->Alter());
  }
}

TEST_P(MasterAuthzITest, TestChangeOwnerWithoutDelegateAdmin) {
  this->GrantAllDatabasePrivilege({kDatabaseName, kTableName});
  const string table_name = Substitute("$0.$1", kDatabaseName, kTableName);

  unique_ptr<KuduTableAlterer> alterer(this->client_->NewTableAlterer(table_name));
  Status s = alterer->SetOwner(kSecondUser)->Alter();
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
}

TEST_P(MasterAuthzITest, TestChangeOwnerWithoutAll) {
  this->GrantAlterWithGrantTablePrivilege({kDatabaseName, kTableName});
  const string table_name = Substitute("$0.$1", kDatabaseName, kTableName);

  unique_ptr<KuduTableAlterer> alterer(this->client_->NewTableAlterer(table_name));
  Status s = alterer->SetOwner(kSecondUser)->Alter();
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
}

TEST_P(MasterAuthzITest, TestAlterAndChangeOwner) {
  this->GrantAlterTablePrivilege({kDatabaseName, kTableName});
  const string table_name = Substitute("$0.$1", kDatabaseName, kTableName);

  unique_ptr<KuduTableAlterer> alterer(this->client_->NewTableAlterer(table_name));
  alterer->SetOwner(kSecondUser)->DropColumn("int8_val");
  Status s = alterer->Alter();
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  this->GrantAllWithGrantTablePrivilege({kDatabaseName, kTableName});
  ASSERT_OK(alterer->Alter());
}

class MasterAuthzOwnerITest : public MasterAuthzITestBase,
                              public ::testing::WithParamInterface<std::tuple<HarnessEnum,
                                                                              std::string>> {
 public:
  void SetUp() override {
    NO_FATALS(MasterAuthzITestBase::SetUp());
    NO_FATALS(SetUpCluster(std::get<0>(GetParam())));
  }
};

INSTANTIATE_TEST_SUITE_P(
    AuthzProvidersWithOwner, MasterAuthzOwnerITest,
    ::testing::Combine(::testing::Values(kRanger),
                       ::testing::Values(kTestUser, "{OWNER}")),
    [](const testing::TestParamInfo<MasterAuthzOwnerITest::ParamType>& info) {
      string user = std::get<1>(info.param) == "{OWNER}" ? "owner" : "nonowner";
      return Substitute("$0_$1", HarnessEnumToString(std::get<0>(info.param)),
                        user);
    });

// Test that when the client passes a table identifier with the table name
// and table ID refer to different tables, the client needs permission on
// both tables for returning TABLE_NOT_FOUND error to avoid leaking table
// existence.
TEST_P(MasterAuthzOwnerITest, TestMismatchedTable) {
  const auto table_name_a = Substitute("$0.$1", kDatabaseName, kTableName);
  const auto table_name_b = Substitute("$0.$1", kDatabaseName, kSecondTable);
  const string& kUsername = std::get<1>(GetParam());

  // Log in as 'test-admin' to get the tablet ID.
  ASSERT_OK(this->cluster_->kdc()->Kinit(kAdminUser));
  shared_ptr<KuduClient> client;
  ASSERT_OK(this->cluster_->CreateClient(nullptr, &client));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client->OpenTable(table_name_a, &table));
  optional<const string&> table_id_a = table->id();

  // Log back as 'test-user'.
  ASSERT_OK(this->cluster_->kdc()->Kinit(kTestUser));

  Status s = this->GetTableLocationsWithTableId(table_name_b, table_id_a);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  ASSERT_STR_MATCHES(s.ToString(), "[Uu]nauthorized action");

  ASSERT_OK(this->GrantGetMetadataTablePrivilege({ kDatabaseName, kTableName, kUsername }));
  s = this->GetTableLocationsWithTableId(table_name_b, table_id_a);
  ASSERT_TRUE(s.IsNotAuthorized());
  ASSERT_STR_MATCHES(s.ToString(), "[Uu]nauthorized action");

  ASSERT_OK(this->GrantGetMetadataTablePrivilege({ kDatabaseName, kSecondTable, kUsername }));
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
    public ::testing::WithParamInterface<std::tuple<HarnessEnum, AuthzDescriptor, std::string>> {
 public:
  void SetUp() override {
    NO_FATALS(MasterAuthzITestBase::SetUp());
    NO_FATALS(SetUpCluster(std::get<0>(GetParam())));
  }
};

TEST_P(TestAuthzTable, TestAuthorizeTable) {
  const AuthzDescriptor& desc = std::get<1>(GetParam());
  const string& owner = std::get<2>(GetParam());
  if (desc.funcs.description == "CreateTable" && owner == "{OWNER}") {
    // This case doesn't make sense semantically as we don't have database
    // owners which would be required to create a table.
    return;
  }
  const auto table_name = Substitute("$0.$1", desc.database, desc.table_name);
  const auto new_table_name = Substitute("$0.$1",
                                         desc.database, desc.new_table_name);
  const OperationParams action_params = { table_name, new_table_name };
  const PrivilegeParams privilege_params = { desc.database, desc.table_name,
                                             std::get<2>(GetParam()) };

  // User 'test-user' attempts to operate on the table without proper privileges.
  Status s = desc.funcs.do_action(this, action_params);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  ASSERT_STR_MATCHES(s.ToString(), "[Uu]nauthorized action");

  // User 'test-user' can operate on the table after obtaining proper privileges.
  ASSERT_OK(desc.funcs.grant_privileges(this, privilege_params));
  ASSERT_OK(desc.funcs.do_action(this, action_params));

  // Ensure that operating on a table while the Authz service is unreachable fails.
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
    {
      {
        &MasterAuthzITestBase::ChangeOwner,
        &MasterAuthzITestBase::GrantAllWithGrantTablePrivilege,
        "ChangeOwner",
      },
      kDatabaseName,
      kTableName,
    },
};
INSTANTIATE_TEST_SUITE_P(
    AuthzCombinations, TestAuthzTable,
    ::testing::Combine(::testing::Values(kRanger),
                       ::testing::ValuesIn(kAuthzCombinations),
                       ::testing::Values(kTestUser, "{OWNER}")),
    [](const testing::TestParamInfo<TestAuthzTable::ParamType>& info) {
      string user = std::get<2>(info.param) == "{OWNER}" ? "owner" : "nonowner";
      return Substitute("$0_$1_$2",
                        HarnessEnumToString(std::get<0>(info.param)),
                        std::get<1>(info.param).funcs.description, user);
    });

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

  // Ensure that operating on a non-existent table fails while the Authz service is
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

INSTANTIATE_TEST_SUITE_P(AuthzFuncCombinations,
                         AuthzErrorHandlingTest,
                         ::testing::Combine(::testing::Values(kRanger),
                                            ::testing::ValuesIn(kAuthzFuncCombinations)),
                         [](const testing::TestParamInfo<AuthzErrorHandlingTest::ParamType>& info) {
                           return Substitute("$0_$1", HarnessEnumToString(std::get<0>(info.param)),
                                             std::get<1>(info.param).description);
                         });

} // namespace kudu
