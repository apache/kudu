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
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/table_util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/hms/hms_client.h"
#include "kudu/hms/mini_hms.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/hms_itest-base.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/sentry_authz_provider-test-base.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/rpc/user_credentials.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/sentry/mini_sentry.h"
#include "kudu/sentry/sentry_client.h"
#include "kudu/sentry/sentry_policy_service_types.h"
#include "kudu/thrift/client.h"
#include "kudu/util/monotime.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using ::sentry::TSentryGrantOption;
using ::sentry::TSentryPrivilege;
using boost::make_optional;
using boost::none;
using boost::optional;
using kudu::client::KuduClient;
using kudu::client::KuduScanToken;
using kudu::client::KuduScanTokenBuilder;
using kudu::client::KuduSchema;
using kudu::client::KuduTable;
using kudu::client::KuduTableAlterer;
using kudu::client::sp::shared_ptr;
using kudu::hms::HmsClient;
using kudu::master::GetTableLocationsResponsePB;
using kudu::master::MasterServiceProxy;
using kudu::master::TabletLocationsPB;
using kudu::rpc::UserCredentials;
using kudu::sentry::SentryClient;
using std::function;
using std::ostream;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {

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
    return itest::GetTableLocations(proxy, table_name, kTimeout, master::VOTER_REPLICA,
                                    table_id, &table_locations);
  }

  Status GrantCreateTablePrivilege(const string& database_name,
                                   const string& /*table_name*/) {
    TSentryPrivilege privilege = master::GetDatabasePrivilege(
        database_name, "CREATE",
        TSentryGrantOption::DISABLED);
    return master::AlterRoleGrantPrivilege(sentry_client_.get(), kDevRole, privilege);
  }

  Status GrantDropTablePrivilege(const string& database_name,
                                 const string& table_name) {
    TSentryPrivilege privilege = master::GetTablePrivilege(
        database_name, table_name, "DROP",
        TSentryGrantOption::DISABLED);
    return master::AlterRoleGrantPrivilege(sentry_client_.get(), kDevRole, privilege);
  }

  Status GrantAlterTablePrivilege(const string& database_name,
                                  const string& table_name) {
    TSentryPrivilege privilege = master::GetTablePrivilege(
        database_name, table_name, "ALTER",
        TSentryGrantOption::DISABLED);
    return master::AlterRoleGrantPrivilege(sentry_client_.get(), kDevRole, privilege);
  }

  Status GrantRenameTablePrivilege(const string& database_name,
                                   const string& table_name) {
    TSentryPrivilege privilege = master::GetTablePrivilege(
        database_name, table_name, "ALL",
        TSentryGrantOption::DISABLED);
    RETURN_NOT_OK(master::AlterRoleGrantPrivilege(sentry_client_.get(), kDevRole, privilege));
    privilege = master::GetDatabasePrivilege(
        database_name, "CREATE",
        TSentryGrantOption::DISABLED);
    return master::AlterRoleGrantPrivilege(sentry_client_.get(), kDevRole, privilege);
  }

  Status GrantGetMetadataTablePrivilege(const string& database_name,
                                        const string& table_name) {
    TSentryPrivilege privilege = master::GetTablePrivilege(
        database_name, table_name, "METADATA",
        TSentryGrantOption::DISABLED);
    return master::AlterRoleGrantPrivilege(sentry_client_.get(), kDevRole, privilege);
  }

  Status CreateTable(const string& table,
                     const string& /*new_table*/) {
    Slice hms_database;
    Slice hms_table;
    RETURN_NOT_OK(ParseHiveTableIdentifier(table, &hms_database, &hms_table));
    return CreateKuduTable(hms_database.ToString(), hms_table.ToString());
  }

  Status IsCreateTableDone(const string& table,
                           const string& /*new_table*/) {
    bool in_progress = false;
    return client_->IsCreateTableInProgress(table, &in_progress);
  }

  Status DeleteTable(const string& table,
                     const string& /*new_table*/) {
    return client_->DeleteTable(table);
  }

  Status AlterTable(const string& table,
                    const string& /*new_table*/) {
    unique_ptr<KuduTableAlterer> table_alterer;
    table_alterer.reset(client_->NewTableAlterer(table)
                               ->DropColumn("int32_val"));
    return table_alterer->Alter();
  }

  Status IsAlterTableDone(const string& table,
                          const string& /*new_table*/) {
    bool in_progress = false;
    return client_->IsAlterTableInProgress(table, &in_progress);
  }

  Status RenameTable(const string& table,
                     const string& new_table) {
    unique_ptr<KuduTableAlterer> table_alterer;
    table_alterer.reset(client_->NewTableAlterer(table)
                               ->RenameTo(new_table));
    return table_alterer->Alter();
  }

  Status GetTableSchema(const string& table,
                        const string& /*new_table*/) {
    KuduSchema schema;
    return client_->GetTableSchema(table, &schema);
  }

  Status GetTableLocations(const string& table,
                           const string& /*new_table*/) {
    return GetTableLocationsWithTableId(table, /*table_id=*/none);
  }

  Status GetTabletLocations(const string& table,
                            const string& /*new_table*/) {
    // Log in as 'test-admin' to get the tablet ID.
    RETURN_NOT_OK(cluster_->kdc()->Kinit(kAdminUser));
    shared_ptr<KuduClient> client;
    RETURN_NOT_OK(cluster_->CreateClient(nullptr, &client));

    shared_ptr<KuduTable> kudu_table;
    RETURN_NOT_OK(client->OpenTable(table, &kudu_table));
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(kudu_table.get());
    RETURN_NOT_OK(builder.Build(&tokens));
    const string tablet_id = tokens[0]->tablet().id();

    // Log back as 'test-user'.
    RETURN_NOT_OK(cluster_->kdc()->Kinit(kTestUser));

    const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
    std::shared_ptr<MasterServiceProxy> proxy = cluster_->master_proxy();
    UserCredentials user_credentials;
    user_credentials.set_real_user(kTestUser);
    proxy->set_user_credentials(user_credentials);

    TabletLocationsPB tablet_locations;
    return itest::GetTabletLocations(proxy, tablet_id, kTimeout,
                                     master::VOTER_REPLICA, &tablet_locations);
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
    ASSERT_OK(master::CreateRoleAndAddToGroups(sentry_client_.get(), kDevRole, kUserGroup));
    ASSERT_OK(master::CreateRoleAndAddToGroups(sentry_client_.get(), kAdminRole, kAdminGroup));

    // Grant privilege 'ALL' on database 'db' to role admin.
    TSentryPrivilege privilege = master::GetDatabasePrivilege(
        kDatabaseName, "ALL",
        TSentryGrantOption::DISABLED);
    ASSERT_OK(master::AlterRoleGrantPrivilege(sentry_client_.get(),
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

// Functor that performs a certain operation (e.g. Create, Rename) on a table given
// its name and its desired new name (only used for Rename).
typedef function<Status(SentryITestBase*, const string&, const string&)> OperatorFunc;

// Functor that grants the required permission for an operation (e.g Create, Rename)
// on a table given the database the table belongs to and the name of the table.
typedef function<Status(SentryITestBase*, const string&, const string&)> PrivilegeFunc;

// A description of the operation function that describes a particular action on a table
// a user can perform, as well as the privilege granting function that grants the required
// permission to the user to able to perform the action.
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
  ASSERT_OK(GrantGetMetadataTablePrivilege(kDatabaseName, kTableName));
  ASSERT_OK(client_->ListTables(&tables));
  ASSERT_EQ(vector<string>({ table_name }), tables);

  tables.clear();
  ASSERT_OK(GrantGetMetadataTablePrivilege(kDatabaseName, kSecondTable));
  ASSERT_OK(client_->ListTables(&tables));
  unordered_set<string> tables_set(tables.begin(), tables.end());
  ASSERT_EQ(unordered_set<string>({ table_name, sec_table_name }), tables_set);
}

TEST_F(MasterSentryTest, TestTableOwnership) {
  ASSERT_OK(GrantCreateTablePrivilege(kDatabaseName, "new_table"));
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
  ASSERT_OK(GrantRenameTablePrivilege(kDatabaseName, kTableName));
  ASSERT_OK(RenameTable(Substitute("$0.$1", kDatabaseName, kTableName),
                        Substitute("$0.$1", kDatabaseName, "b")));
  NO_FATALS(CheckTable(kDatabaseName, "b", make_optional<const string &>(kAdminUser)));

  unique_ptr<KuduTableAlterer> table_alterer;
  table_alterer.reset(client_->NewTableAlterer(Substitute("$0.$1", kDatabaseName, "b"))
                             ->DropColumn("int16_val"));

  // Note that unlike table creation, there could be a delay between the table renaming
  // in Kudu and the privilege renaming in Sentry. Because Kudu uses the transactional
  // listener of the HMS to get notification of table alteration events, while Sentry
  // uses post event listener (which is executed outside the HMS transaction). There
  // is a chance that Kudu already finish the table renaming but the privilege renaming
  // hasn't been reflected in the Sentry service.
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(table_alterer->Alter());
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
  const auto new_table_name = Substitute("$0.$1", desc.database, desc.new_table_name);

  // User 'test-user' attempts to operate on the table without proper privileges.
  Status s = desc.funcs.do_action(this, table_name, new_table_name);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "unauthorized action");

  // User 'test-user' can operate on the table after obtaining proper privileges.
  ASSERT_OK(desc.funcs.grant_privileges(this, desc.database, desc.table_name));
  ASSERT_OK(desc.funcs.do_action(this, table_name, new_table_name));

  // Ensure that operating on a table while the Sentry is unreachable fails.
  ASSERT_OK(StopSentry());
  s = desc.funcs.do_action(this, table_name, new_table_name);
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
        &SentryITestBase::DeleteTable,
        &SentryITestBase::GrantDropTablePrivilege,
        "DeleteTable",
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

  ASSERT_OK(GrantGetMetadataTablePrivilege(kDatabaseName, kTableName));
  s = GetTableLocationsWithTableId(table_name_b, table_id_a);
  ASSERT_TRUE(s.IsNotAuthorized());
  ASSERT_STR_CONTAINS(s.ToString(), "unauthorized action");

  ASSERT_OK(GrantGetMetadataTablePrivilege(kDatabaseName, kSecondTable));
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
  const AuthzFuncs& funcs = GetParam();
  const auto table_name = Substitute("$0.$1", kDatabaseName, "non_existent");
  const auto new_table_name = Substitute("$0.$1", kDatabaseName, "b");

  // Ensure that operating on a non-existent table without proper privileges gives
  // a NOT_AUTHORIZED error, instead of leaking the table existence by giving a
  // TABLE_NOT_FOUND error.
  Status s = funcs.do_action(this, table_name, new_table_name);
  ASSERT_TRUE(s.IsNotAuthorized());
  ASSERT_STR_CONTAINS(s.ToString(), "unauthorized action");

  // Ensure that operating on a non-existent table fails
  // while Sentry is unreachable.
  ASSERT_OK(StopSentry());
  s = funcs.do_action(this, table_name, new_table_name);
  ASSERT_TRUE(s.IsNetworkError()) << s.ToString();

  // Ensure that operating on a non-existent table with proper privileges gives a
  // TABLE_NOT_FOUND error.
  ASSERT_OK(StartSentry());
  ASSERT_EVENTUALLY([&] {
    // SentryAuthzProvider throttles reconnections, so it's necessary
    // to wait out the backoff.
    ASSERT_OK(funcs.grant_privileges(this, kDatabaseName, "non_existent"));
  });
  s = funcs.do_action(this, table_name, new_table_name);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
}

static const AuthzFuncs kAuthzFuncCombinations[] = {
    {
      &SentryITestBase::DeleteTable,
      &SentryITestBase::GrantDropTablePrivilege,
      "DeleteTable"
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
} // namespace kudu
