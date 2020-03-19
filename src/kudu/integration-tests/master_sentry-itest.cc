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
#include "kudu/hms/mini_hms.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/hms_itest-base.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/sentry_authz_provider-test-base.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/user_credentials.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/sentry/mini_sentry.h"
#include "kudu/sentry/sentry_client.h"
#include "kudu/sentry/sentry_policy_service_types.h"
#include "kudu/thrift/client.h"
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
using kudu::client::KuduTable;
using kudu::client::KuduTableAlterer;
using kudu::client::sp::shared_ptr;
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
using kudu::rpc::RpcController;
using kudu::rpc::UserCredentials;
using kudu::sentry::SentryClient;
using sentry::TSentryGrantOption;
using sentry::TSentryPrivilege;
using std::atomic;
using std::function;
using std::ostream;
using std::string;
using std::thread;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strings::Substitute;

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

// Test Master authorization enforcement with Sentry and HMS
// integration enabled.
class SentryITestBase : public HmsITestBase {
 public:
  static const char* const kAdminGroup;
  static const char* const kAdminUser;
  static const char* const kUserGroup;
  static const char* const kTestUser;
  static const char* const kImpalaUser;
  static const char* const kDevRole;
  static const char* const kAdminRole;
  static const char* const kDatabaseName;
  static const char* const kTableName;
  static const char* const kSecondTable;

  Status StopSentry() {
    RETURN_NOT_OK(sentry_client_->Stop());
    RETURN_NOT_OK(cluster_->sentry()->Stop());
    return Status::OK();
  }

  Status StartSentry() {
    RETURN_NOT_OK(cluster_->sentry()->Start());
    RETURN_NOT_OK(cluster_->kdc()->Kinit("kudu"));
    RETURN_NOT_OK(sentry_client_->Start());
    return Status::OK();
  }

  Status GetTableLocationsWithTableId(const string& table_name,
                                      optional<const string&> table_id) {
    const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
    std::shared_ptr<MasterServiceProxy> proxy = cluster_->master_proxy();
    UserCredentials user_credentials;
    user_credentials.set_real_user(kTestUser);
    proxy->set_user_credentials(user_credentials);

    GetTableLocationsResponsePB table_locations;
    return itest::GetTableLocations(proxy, table_name, kTimeout, VOTER_REPLICA,
                                    table_id, &table_locations);
  }

  Status GrantCreateTablePrivilege(const PrivilegeParams& p) {
    return AlterRoleGrantPrivilege(sentry_client_.get(), kDevRole,
        GetDatabasePrivilege(p.db_name, "CREATE"));
  }

  Status GrantDropTablePrivilege(const PrivilegeParams& p) {
    return AlterRoleGrantPrivilege(sentry_client_.get(), kDevRole,
        GetTablePrivilege(p.db_name, p.table_name, "DROP"));
  }

  Status GrantAlterTablePrivilege(const PrivilegeParams& p) {
    return AlterRoleGrantPrivilege(sentry_client_.get(), kDevRole,
        GetTablePrivilege(p.db_name, p.table_name, "ALTER"));
  }

  Status GrantRenameTablePrivilege(const PrivilegeParams& p) {
    RETURN_NOT_OK(AlterRoleGrantPrivilege(sentry_client_.get(), kDevRole,
        GetTablePrivilege(p.db_name, p.table_name, "ALL")));
    return AlterRoleGrantPrivilege(sentry_client_.get(), kDevRole,
        GetDatabasePrivilege(p.db_name, "CREATE"));
  }

  Status GrantGetMetadataTablePrivilege(const PrivilegeParams& p) {
    return AlterRoleGrantPrivilege(sentry_client_.get(), kDevRole,
        GetTablePrivilege(p.db_name, p.table_name, "METADATA"));
  }

  Status CreateTable(const OperationParams& p) {
    Slice hms_database;
    Slice hms_table;
    RETURN_NOT_OK(ParseHiveTableIdentifier(p.table_name,
                                           &hms_database, &hms_table));
    return CreateKuduTable(hms_database.ToString(), hms_table.ToString());
  }

  Status IsCreateTableDone(const OperationParams& p) {
    bool in_progress = false;
    return client_->IsCreateTableInProgress(p.table_name, &in_progress);
  }

  Status DropTable(const OperationParams& p) {
    return client_->DeleteTable(p.table_name);
  }

  Status AlterTable(const OperationParams& p) {
    unique_ptr<KuduTableAlterer> alterer(
        client_->NewTableAlterer(p.table_name));
    return alterer->DropColumn("int32_val")->Alter();
  }

  Status IsAlterTableDone(const OperationParams& p) {
    bool in_progress = false;
    return client_->IsAlterTableInProgress(p.table_name, &in_progress);
  }

  Status RenameTable(const OperationParams& p) {
    unique_ptr<KuduTableAlterer> alterer(
        client_->NewTableAlterer(p.table_name));
    return alterer->RenameTo(p.new_table_name)->Alter();
  }

  Status GetTableSchema(const OperationParams& p) {
    KuduSchema schema;
    return client_->GetTableSchema(p.table_name, &schema);
  }

  Status GetTableLocations(const OperationParams& p) {
    return GetTableLocationsWithTableId(p.table_name, /*table_id=*/none);
  }

  Status GetTabletLocations(const OperationParams& p) {
    // Log in as 'test-admin' to get the tablet ID.
    RETURN_NOT_OK(cluster_->kdc()->Kinit(kAdminUser));
    shared_ptr<KuduClient> client;
    RETURN_NOT_OK(cluster_->CreateClient(nullptr, &client));

    shared_ptr<KuduTable> kudu_table;
    RETURN_NOT_OK(client->OpenTable(p.table_name, &kudu_table));
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(kudu_table.get());
    RETURN_NOT_OK(builder.Build(&tokens));
    CHECK(!tokens.empty());
    const string& tablet_id = tokens[0]->tablet().id();

    // Log back as 'test-user'.
    RETURN_NOT_OK(cluster_->kdc()->Kinit(kTestUser));

    static const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
    std::shared_ptr<MasterServiceProxy> proxy(cluster_->master_proxy());
    UserCredentials user_credentials;
    user_credentials.set_real_user(kTestUser);
    proxy->set_user_credentials(user_credentials);

    GetTabletLocationsResponsePB tablet_locations;
    return itest::GetTabletLocations(proxy, tablet_id, kTimeout,
                                     VOTER_REPLICA, &tablet_locations);
  }

  void SetUp() override {
    HmsITestBase::SetUp();

    cluster::ExternalMiniClusterOptions opts;
    // Always enable Kerberos, as sentry deployment does not make much sense
    // in a non-Kerberized environment.
    bool enable_kerberos = true;
    opts.hms_mode = HmsMode::ENABLE_METASTORE_INTEGRATION;
    opts.enable_sentry = true;
    opts.enable_kerberos = enable_kerberos;
    // Configure the timeout to reduce the run time of tests that involve
    // re-connections.
    const string timeout = AllowSlowTests() ? "5" : "2";
    opts.extra_master_flags.emplace_back(
       Substitute("$0=$1", "--sentry_service_send_timeout_seconds", timeout));
    opts.extra_master_flags.emplace_back(
       Substitute("$0=$1", "--sentry_service_recv_timeout_seconds", timeout));

    // Add 'impala' as trusted user who may access the cluster without being
    // authorized.
    opts.extra_master_flags.emplace_back("--trusted_user_acl=impala");
    opts.extra_master_flags.emplace_back("--user_acl=test-user,impala");

    // Enable/disable caching of authz privileges received from Sentry.
    opts.extra_master_flags.emplace_back(
        Substitute("--sentry_privileges_cache_capacity_mb=$0",
                   IsAuthzPrivilegeCacheEnabled() ? 1 : 0));

    StartClusterWithOpts(std::move(opts));

    // Create principals 'impala' and 'kudu'. Configure to use the latter.
    ASSERT_OK(cluster_->kdc()->CreateUserPrincipal(kImpalaUser));
    ASSERT_OK(cluster_->kdc()->CreateUserPrincipal("kudu"));
    ASSERT_OK(cluster_->kdc()->Kinit("kudu"));

    thrift::ClientOptions hms_opts;
    hms_opts.enable_kerberos = enable_kerberos;
    hms_opts.service_principal = "hive";
    hms_client_.reset(new HmsClient(cluster_->hms()->address(), hms_opts));
    ASSERT_OK(hms_client_->Start());

    thrift::ClientOptions sentry_opts;
    sentry_opts.enable_kerberos = enable_kerberos;
    sentry_opts.service_principal = "sentry";
    sentry_client_.reset(new SentryClient(cluster_->sentry()->address(), sentry_opts));
    ASSERT_OK(sentry_client_->Start());

    // User to Role mapping:
    // 1. user -> developer,
    // 2. admin -> admin.
    ASSERT_OK(CreateRoleAndAddToGroups(sentry_client_.get(), kDevRole, kUserGroup));
    ASSERT_OK(CreateRoleAndAddToGroups(sentry_client_.get(), kAdminRole, kAdminGroup));

    // Grant privilege 'ALL' on database 'db' to role admin.
    TSentryPrivilege privilege = GetDatabasePrivilege(
        kDatabaseName, "ALL",
        TSentryGrantOption::DISABLED);
    ASSERT_OK(AlterRoleGrantPrivilege(sentry_client_.get(),
                                              kAdminRole, privilege));

    // Create database 'db' in HMS and Kudu tables 'table' and 'second_table' which
    // owns by user 'test-admin'.
    ASSERT_OK(CreateDatabase(kDatabaseName));
    ASSERT_OK(CreateKuduTable(kDatabaseName, kTableName));
    ASSERT_OK(CreateKuduTable(kDatabaseName, kSecondTable));
    NO_FATALS(CheckTable(kDatabaseName, kTableName,
                         make_optional<const string&>(kAdminUser)));
    NO_FATALS(CheckTable(kDatabaseName, kSecondTable,
                         make_optional<const string&>(kAdminUser)));

    // Log in as 'test-user' and reset the client to pick up the change in user.
    ASSERT_OK(cluster_->kdc()->Kinit(kTestUser));
    ASSERT_OK(cluster_->CreateClient(nullptr, &client_));
  }

  void TearDown() override {
    if (sentry_client_) {
      ASSERT_OK(sentry_client_->Stop());
    }
    if (hms_client_) {
      ASSERT_OK(hms_client_->Stop());
    }
    HmsITestBase::TearDown();
  }

  virtual bool IsAuthzPrivilegeCacheEnabled() const {
    return false;
  }

 protected:
  unique_ptr<SentryClient> sentry_client_;
};

const char* const SentryITestBase::kAdminGroup = "admin";
const char* const SentryITestBase::kAdminUser = "test-admin";
const char* const SentryITestBase::kUserGroup = "user";
const char* const SentryITestBase::kTestUser = "test-user";
const char* const SentryITestBase::kImpalaUser = "impala";
const char* const SentryITestBase::kDevRole = "developer";
const char* const SentryITestBase::kAdminRole = "ad";
const char* const SentryITestBase::kDatabaseName = "db";
const char* const SentryITestBase::kTableName = "table";
const char* const SentryITestBase::kSecondTable = "second_table";

// Functor that performs a certain operation (e.g. Create, Rename) on a table
// given its name and its desired new name, if necessary (only used for Rename).
typedef function<Status(SentryITestBase*, const OperationParams&)> OperatorFunc;

// Functor that grants the required permission for an operation (e.g Create,
// Rename) on a table given the database the table belongs to and the name of
// the table, if applicable.
typedef function<Status(SentryITestBase*, const PrivilegeParams&)> PrivilegeFunc;

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

// Test basic master authorization enforcement with Sentry and HMS integration
// enabled.
class MasterSentryTest : public SentryITestBase {};

// Test that the trusted user can access the cluster without being authorized.
TEST_F(MasterSentryTest, TestTrustedUserAcl) {
  // Log in as 'impala' and reset the client to pick up the change in user.
  ASSERT_OK(cluster_->kdc()->Kinit(kImpalaUser));
  ASSERT_OK(cluster_->CreateClient(nullptr, &client_));

  vector<string> tables;
  ASSERT_OK(client_->ListTables(&tables));
  unordered_set<string> tables_set(tables.begin(), tables.end());
  ASSERT_EQ(unordered_set<string>({ Substitute("$0.$1", kDatabaseName, kTableName),
                                    Substitute("$0.$1", kDatabaseName, kSecondTable) }),
            tables_set);

  ASSERT_OK(CreateKuduTable(kDatabaseName, "new_table"));
  NO_FATALS(CheckTable(kDatabaseName, "new_table",
                       make_optional<const string&>(kImpalaUser)));
}

TEST_F(MasterSentryTest, TestAuthzListTables) {
  // ListTables is not parameterized as other operations below, because non-authorized
  // tables will be filtered instead of returning NOT_AUTHORIZED error.
  const auto table_name = Substitute("$0.$1", kDatabaseName, kTableName);
  const auto sec_table_name = Substitute("$0.$1", kDatabaseName, kSecondTable);

  // Listing tables shows nothing without proper privileges.
  vector<string> tables;
  ASSERT_OK(client_->ListTables(&tables));
  ASSERT_TRUE(tables.empty());

  // Listing tables only shows the tables which user has proper privileges on.
  tables.clear();
  ASSERT_OK(GrantGetMetadataTablePrivilege({ kDatabaseName, kTableName }));
  ASSERT_OK(client_->ListTables(&tables));
  ASSERT_EQ(vector<string>({ table_name }), tables);

  tables.clear();
  ASSERT_OK(GrantGetMetadataTablePrivilege({ kDatabaseName, kSecondTable }));
  ASSERT_OK(client_->ListTables(&tables));
  unordered_set<string> tables_set(tables.begin(), tables.end());
  ASSERT_EQ(unordered_set<string>({ table_name, sec_table_name }), tables_set);
}

// When authorizing ListTables, if there is a concurrent rename, we may not end
// up showing the table.
TEST_F(MasterSentryTest, TestAuthzListTablesConcurrentRename) {
  ASSERT_OK(cluster_->SetFlag(cluster_->master(),
      "catalog_manager_inject_latency_list_authz_ms", "3000"));;
  const auto table_name = Substitute("$0.$1", kDatabaseName, kTableName);
  const auto sec_table_name = Substitute("$0.$1", kDatabaseName, kSecondTable);
  ASSERT_OK(GrantGetMetadataTablePrivilege({ kDatabaseName, kTableName }));
  ASSERT_OK(GrantRenameTablePrivilege({ kDatabaseName, kTableName }));

  // List the tables while injecting latency.
  vector<string> tables;
  thread t([&] {
    ASSERT_OK(client_->ListTables(&tables));
  });

  // While that's happening, rename one of the tables.
  ASSERT_OK(RenameTable({ table_name, Substitute("$0.$1", kDatabaseName, "b") }));
  NO_FATALS(t.join());

  // We shouldn't see the renamed table.
  ASSERT_EQ(1, tables.size());
  ASSERT_EQ(sec_table_name, tables[0]);
}

TEST_F(MasterSentryTest, TestTableOwnership) {
  ASSERT_OK(GrantCreateTablePrivilege({ kDatabaseName }));
  ASSERT_OK(CreateKuduTable(kDatabaseName, "new_table"));
  NO_FATALS(CheckTable(kDatabaseName, "new_table",
                       make_optional<const string&>(kTestUser)));

  // Checks the user with table ownership automatically has ALL privilege on the
  // table. User 'test-user' can delete the same table without specifically granting
  // 'DROP ON TABLE'. Note that ownership population between the HMS and the Sentry
  // service happens synchronously, therefore, the table deletion should succeed
  // right after the table creation.
  // TODO(hao): test create a table with a different owner than the client’s username?
  ASSERT_OK(client_->DeleteTable(Substitute("$0.$1", kDatabaseName, "new_table")));
  NO_FATALS(CheckTableDoesNotExist(kDatabaseName, "new_table"));
}

// Checks Sentry privileges are synchronized upon table rename in the HMS.
TEST_F(MasterSentryTest, TestRenameTablePrivilegeTransfer) {
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

class TestAuthzTable :
    public MasterSentryTest,
    public ::testing::WithParamInterface<AuthzDescriptor> {
    // TODO(aserbin): update the test to introduce authz privilege caching
};

TEST_P(TestAuthzTable, TestAuthorizeTable) {
  const AuthzDescriptor& desc = GetParam();
  const auto table_name = Substitute("$0.$1", desc.database, desc.table_name);
  const auto new_table_name = Substitute("$0.$1",
                                         desc.database, desc.new_table_name);
  const OperationParams action_params = { table_name, new_table_name };
  const PrivilegeParams privilege_params = { desc.database, desc.table_name };

  // User 'test-user' attempts to operate on the table without proper privileges.
  Status s = desc.funcs.do_action(this, action_params);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "unauthorized action");

  // User 'test-user' can operate on the table after obtaining proper privileges.
  ASSERT_OK(desc.funcs.grant_privileges(this, privilege_params));
  ASSERT_OK(desc.funcs.do_action(this, action_params));

  // Ensure that operating on a table while the Sentry is unreachable fails.
  ASSERT_OK(StopSentry());
  s = desc.funcs.do_action(this, action_params);
  ASSERT_TRUE(s.IsNetworkError()) << s.ToString();
}

static const AuthzDescriptor kAuthzCombinations[] = {
    {
      {
        &SentryITestBase::CreateTable,
        &SentryITestBase::GrantCreateTablePrivilege,
        "CreateTable",
      },
      SentryITestBase::kDatabaseName,
      "new_table",
      ""
    },
    {
      {
        &SentryITestBase::DropTable,
        &SentryITestBase::GrantDropTablePrivilege,
        "DropTable",
      },
      SentryITestBase::kDatabaseName,
      SentryITestBase::kTableName,
      ""
    },
    {
      {
        &SentryITestBase::AlterTable,
        &SentryITestBase::GrantAlterTablePrivilege,
        "AlterTable",
      },
      SentryITestBase::kDatabaseName,
      SentryITestBase::kTableName,
      ""
    },
    {
      {
        &SentryITestBase::RenameTable,
        &SentryITestBase::GrantRenameTablePrivilege,
        "RenameTable",
      },
      SentryITestBase::kDatabaseName,
      SentryITestBase::kTableName,
      "new_table"
    },
    {
      {
        &SentryITestBase::GetTableSchema,
        &SentryITestBase::GrantGetMetadataTablePrivilege,
        "GetTableSchema",
      },
      SentryITestBase::kDatabaseName,
      SentryITestBase::kTableName,
      ""
    },
    {
      {
        &SentryITestBase::GetTableLocations,
        &SentryITestBase::GrantGetMetadataTablePrivilege,
        "GetTableLocations",
      },
      SentryITestBase::kDatabaseName,
      SentryITestBase::kTableName,
      ""
    },
    {
      {
        &SentryITestBase::GetTabletLocations,
        &SentryITestBase::GrantGetMetadataTablePrivilege,
        "GetTabletLocations",
      },
      SentryITestBase::kDatabaseName,
      SentryITestBase::kTableName,
      ""
    },
    {
      {
        &SentryITestBase::IsCreateTableDone,
        &SentryITestBase::GrantGetMetadataTablePrivilege,
        "IsCreateTableDone",
      },
      SentryITestBase::kDatabaseName,
      SentryITestBase::kTableName,
      ""
    },
    {
      {
        &SentryITestBase::IsAlterTableDone,
        &SentryITestBase::GrantGetMetadataTablePrivilege,
        "IsAlterTableDone",
      },
      SentryITestBase::kDatabaseName,
      SentryITestBase::kTableName,
      ""
    },
};
INSTANTIATE_TEST_CASE_P(AuthzCombinations,
                        TestAuthzTable,
                        ::testing::ValuesIn(kAuthzCombinations));

// Test that when the client passes a table identifier with the table name
// and table ID refer to different tables, the client needs permission on
// both tables for returning TABLE_NOT_FOUND error to avoid leaking table
// existence.
TEST_F(MasterSentryTest, TestMismatchedTable) {
  const auto table_name_a = Substitute("$0.$1", kDatabaseName, kTableName);
  const auto table_name_b = Substitute("$0.$1", kDatabaseName, kSecondTable);

  // Log in as 'test-admin' to get the tablet ID.
  ASSERT_OK(cluster_->kdc()->Kinit(kAdminUser));
  shared_ptr<KuduClient> client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &client));
  shared_ptr<KuduTable> table;
  ASSERT_OK(client->OpenTable(table_name_a, &table));
  optional<const string&> table_id_a = make_optional<const string&>(table->id());

  // Log back as 'test-user'.
  ASSERT_OK(cluster_->kdc()->Kinit(kTestUser));

  Status s = GetTableLocationsWithTableId(table_name_b, table_id_a);
  ASSERT_TRUE(s.IsNotAuthorized());
  ASSERT_STR_CONTAINS(s.ToString(), "unauthorized action");

  ASSERT_OK(GrantGetMetadataTablePrivilege({ kDatabaseName, kTableName }));
  s = GetTableLocationsWithTableId(table_name_b, table_id_a);
  ASSERT_TRUE(s.IsNotAuthorized());
  ASSERT_STR_CONTAINS(s.ToString(), "unauthorized action");

  ASSERT_OK(GrantGetMetadataTablePrivilege({ kDatabaseName, kSecondTable }));
  s = GetTableLocationsWithTableId(table_name_b, table_id_a);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_STR_CONTAINS(s.ToString(), "the table ID refers to a different table");
}

class AuthzErrorHandlingTest :
    public SentryITestBase,
    public ::testing::WithParamInterface<AuthzFuncs> {
    // TODO(aserbin): update the test to introduce authz privilege caching
};
TEST_P(AuthzErrorHandlingTest, TestNonExistentTable) {
  static constexpr const char* const kTableName = "non_existent";
  const AuthzFuncs& funcs = GetParam();
  const auto table_name = Substitute("$0.$1", kDatabaseName, kTableName);
  const auto new_table_name = Substitute("$0.$1", kDatabaseName, "b");
  const OperationParams action_params = { table_name, new_table_name };
  const PrivilegeParams privilege_params = { kDatabaseName, kTableName };

  // Ensure that operating on a non-existent table without proper privileges gives
  // a NOT_AUTHORIZED error, instead of leaking the table existence by giving a
  // TABLE_NOT_FOUND error.
  Status s = funcs.do_action(this, action_params);
  ASSERT_TRUE(s.IsNotAuthorized());
  ASSERT_STR_CONTAINS(s.ToString(), "unauthorized action");

  // Ensure that operating on a non-existent table fails
  // while Sentry is unreachable.
  ASSERT_OK(StopSentry());
  s = funcs.do_action(this, action_params);
  ASSERT_TRUE(s.IsNetworkError()) << s.ToString();

  // Ensure that operating on a non-existent table with proper privileges gives a
  // TABLE_NOT_FOUND error.
  ASSERT_OK(StartSentry());
  ASSERT_EVENTUALLY([&] {
    // SentryAuthzProvider throttles reconnections, so it's necessary
    // to wait out the backoff.
    ASSERT_OK(funcs.grant_privileges(this, privilege_params));
  });
  s = funcs.do_action(this, action_params);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
}

static const AuthzFuncs kAuthzFuncCombinations[] = {
    {
      &SentryITestBase::DropTable,
      &SentryITestBase::GrantDropTablePrivilege,
      "DropTable"
    },
    {
      &SentryITestBase::AlterTable,
      &SentryITestBase::GrantAlterTablePrivilege,
      "AlterTable"
    },
    {
      &SentryITestBase::RenameTable,
      &SentryITestBase::GrantRenameTablePrivilege,
      "RenameTable"
    },
    {
      &SentryITestBase::GetTableSchema,
      &SentryITestBase::GrantGetMetadataTablePrivilege,
      "GetTableSchema"
    },
    {
      &SentryITestBase::GetTableLocations,
      &SentryITestBase::GrantGetMetadataTablePrivilege,
      "GetTableLocations"
    },
    {
      &SentryITestBase::IsCreateTableDone,
      &SentryITestBase::GrantGetMetadataTablePrivilege,
      "IsCreateTableDone"
    },
    {
      &SentryITestBase::IsAlterTableDone,
      &SentryITestBase::GrantGetMetadataTablePrivilege,
      "IsAlterTableDone"
    },
};

INSTANTIATE_TEST_CASE_P(AuthzFuncCombinations,
                        AuthzErrorHandlingTest,
                        ::testing::ValuesIn(kAuthzFuncCombinations));

// Class for test scenarios verifying functionality of managing AuthzProvider's
// privileges cache via Kudu RPC.
class SentryAuthzProviderCacheITest : public SentryITestBase {
 public:
  bool IsAuthzPrivilegeCacheEnabled() const override {
    return true;
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
  ASSERT_OK(StopSentry());
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
  ASSERT_OK(StartSentry());
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
  ASSERT_OK(AlterRoleGrantPrivilege(
      sentry_client_.get(), kDevRole,
      GetDatabasePrivilege(kDatabaseName, "METADATA")));

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

  ASSERT_OK(StopSentry());

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

  ASSERT_OK(StartSentry());

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
    cluster::ExternalMiniClusterOptions opts;
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
