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

#include "kudu/master/sentry_authz_provider.h"

#include <memory>
#include <set>
#include <string>
#include <vector>

#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/sentry/mini_sentry.h"
#include "kudu/sentry/sentry-test-base.h"
#include "kudu/sentry/sentry_action.h"
#include "kudu/sentry/sentry_authorizable_scope.h"
#include "kudu/sentry/sentry_client.h"
#include "kudu/sentry/sentry_policy_service_types.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int32(sentry_service_recv_timeout_seconds);
DECLARE_int32(sentry_service_send_timeout_seconds);
DECLARE_string(sentry_service_rpc_addresses);
DECLARE_string(sentry_service_security_mode);
DECLARE_string(server_name);

using sentry::TAlterSentryRoleAddGroupsRequest;
using sentry::TAlterSentryRoleAddGroupsResponse;
using sentry::TAlterSentryRoleGrantPrivilegeRequest;
using sentry::TAlterSentryRoleGrantPrivilegeResponse;
using sentry::TCreateSentryRoleRequest;
using sentry::TDropSentryRoleRequest;
using sentry::TSentryGrantOption;
using sentry::TSentryGroup;
using sentry::TSentryPrivilege;
using std::tuple;
using std::unique_ptr;
using std::set;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {

using sentry::SentryAction;
using sentry::SentryTestBase;
using sentry::SentryAuthorizableScope;

namespace master {

class SentryAuthzProviderTest : public SentryTestBase {
 public:
  const char* const kAdminUser = "test-admin";
  const char* const kTestUser = "test-user";
  const char* const kUserGroup = "user";
  const char* const kRoleName = "developer";

  void SetUp() override {
    SentryTestBase::SetUp();

    // Configure the SentryAuthzProvider flags.
    FLAGS_sentry_service_security_mode = KerberosEnabled() ? "kerberos" : "none";
    FLAGS_sentry_service_rpc_addresses = sentry_->address().ToString();
    sentry_authz_provider_.reset(new SentryAuthzProvider());
    ASSERT_OK(sentry_authz_provider_->Start());
  }

  Status StopSentry() {
    RETURN_NOT_OK(sentry_client_->Stop());
    RETURN_NOT_OK(sentry_->Stop());
    return Status::OK();
  }

  Status StartSentry() {
    RETURN_NOT_OK(sentry_->Start());
    RETURN_NOT_OK(sentry_client_->Start());
    return Status::OK();
  }

  Status DropRole(const string& role_name) {
    TDropSentryRoleRequest role_req;
    role_req.__set_requestorUserName(kAdminUser);
    role_req.__set_roleName(role_name);
    return sentry_client_->DropRole(role_req);
  }

  Status CreateRoleAndAddToGroups(const string& role_name, const string& group_name) {
    TCreateSentryRoleRequest role_req;
    role_req.__set_requestorUserName(kAdminUser);
    role_req.__set_roleName(role_name);
    RETURN_NOT_OK(sentry_client_->CreateRole(role_req));

    TSentryGroup group;
    group.groupName = group_name;
    set<TSentryGroup> groups;
    groups.insert(group);
    TAlterSentryRoleAddGroupsRequest group_request;
    TAlterSentryRoleAddGroupsResponse group_response;
    group_request.__set_requestorUserName(kAdminUser);
    group_request.__set_roleName(role_name);
    group_request.__set_groups(groups);
    return sentry_client_->AlterRoleAddGroups(group_request, &group_response);
  }

  Status AlterRoleGrantPrivilege(const string& role_name, const TSentryPrivilege& privilege) {
    TAlterSentryRoleGrantPrivilegeRequest privilege_request;
    TAlterSentryRoleGrantPrivilegeResponse privilege_response;
    privilege_request.__set_requestorUserName(kAdminUser);
    privilege_request.__set_roleName(role_name);
    privilege_request.__set_privilege(privilege);
    return sentry_client_->AlterRoleGrantPrivilege(privilege_request, &privilege_response);
  }

  // Returns a server level TSentryPrivilege with the server name, action
  // and grant option.
  TSentryPrivilege GetServerPrivilege(
      const string& action,
      TSentryGrantOption::type grant_option = TSentryGrantOption::DISABLED) {
    TSentryPrivilege privilege;
    privilege.__set_privilegeScope("SERVER");
    privilege.__set_serverName(FLAGS_server_name);
    privilege.__set_action(action);
    privilege.__set_grantOption(grant_option);
    return privilege;
  }

  // Returns a database level TSentryPrivilege with the given database name, action
  // and grant option.
  TSentryPrivilege GetDatabasePrivilege(
      const string& db_name,
      const string& action,
      const TSentryGrantOption::type& grant_option = TSentryGrantOption::DISABLED) {
    TSentryPrivilege privilege = GetServerPrivilege(action, grant_option);
    privilege.__set_privilegeScope("DATABASE");
    privilege.__set_dbName(db_name);
    return privilege;
  }

  // Returns a table level TSentryPrivilege with the given table name, database name,
  // action and grant option.
  TSentryPrivilege GetTablePrivilege(
      const string& db_name,
      const string& table_name,
      const string& action,
      const TSentryGrantOption::type& grant_option = TSentryGrantOption::DISABLED) {
    TSentryPrivilege privilege = GetDatabasePrivilege(db_name, action, grant_option);
    privilege.__set_privilegeScope("TABLE");
    privilege.__set_tableName(table_name);
    return privilege;
  }

  // Returns a column level TSentryPrivilege with the given column name, table name,
  // database name, action and grant option.
  TSentryPrivilege GetColumnPrivilege(
      const string& db_name,
      const string& table_name,
      const string& column_name,
      const string& action,
      const TSentryGrantOption::type& grant_option = TSentryGrantOption::DISABLED) {
    TSentryPrivilege privilege = GetTablePrivilege(db_name, table_name,
                                                   action, grant_option);
    privilege.__set_privilegeScope("COLUMN");
    privilege.__set_columnName(column_name);
    return privilege;
  }

 protected:
  unique_ptr<SentryAuthzProvider> sentry_authz_provider_;
};

// Tests to ensure SentryAuthzProvider enforces access control on tables as expected.
// Parameterized by whether Kerberos should be enabled.
class TestTableAuthorization : public SentryAuthzProviderTest,
                               public ::testing::WithParamInterface<bool> {
 public:
  bool KerberosEnabled() const {
    return GetParam();
  }
};

INSTANTIATE_TEST_CASE_P(KerberosEnabled, TestTableAuthorization, ::testing::Bool());

TEST_P(TestTableAuthorization, TestAuthorizeCreateTable) {
  // Don't authorize create table on a non-existent user.
  Status s = sentry_authz_provider_->AuthorizeCreateTable("db.table",
                                                          "non-existent-user",
                                                          "non-existent-user");
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Don't authorize create table on a user without any privileges.
  s = sentry_authz_provider_->AuthorizeCreateTable("db.table", kTestUser, kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Don't authorize create table on a user without required privileges.
  ASSERT_OK(CreateRoleAndAddToGroups(kRoleName, kUserGroup));
  TSentryPrivilege privilege = GetDatabasePrivilege("db", "DROP");
  ASSERT_OK(AlterRoleGrantPrivilege(kRoleName, privilege));
  s = sentry_authz_provider_->AuthorizeCreateTable("db.table", kTestUser, kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Authorize create table on a user with proper privileges.
  privilege = GetDatabasePrivilege("db", "CREATE");
  ASSERT_OK(AlterRoleGrantPrivilege(kRoleName, privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeCreateTable("db.table", kTestUser, kTestUser));

  // Table creation with a different owner than the user
  // requires the creating user have 'ALL on DATABASE' with grant.
  s = sentry_authz_provider_->AuthorizeCreateTable("db.table", kTestUser, "diff-user");
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  privilege = GetDatabasePrivilege("db", "ALL");
  s = sentry_authz_provider_->AuthorizeCreateTable("db.table", kTestUser, "diff-user");
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  privilege = GetDatabasePrivilege("db", "ALL", TSentryGrantOption::ENABLED);
  ASSERT_OK(AlterRoleGrantPrivilege(kRoleName, privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeCreateTable("db.table", kTestUser, "diff-user"));
}

TEST_P(TestTableAuthorization, TestAuthorizeDropTable) {
  // Don't authorize delete table on a user without required privileges.
  ASSERT_OK(CreateRoleAndAddToGroups(kRoleName, kUserGroup));
  TSentryPrivilege privilege = GetDatabasePrivilege("db", "SELECT");
  ASSERT_OK(AlterRoleGrantPrivilege(kRoleName, privilege));
  Status s = sentry_authz_provider_->AuthorizeDropTable("db.table", kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Authorize delete table on a user with proper privileges.
  privilege = GetDatabasePrivilege("db", "DROP");
  ASSERT_OK(AlterRoleGrantPrivilege(kRoleName, privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeDropTable("db.table", kTestUser));
}

TEST_P(TestTableAuthorization, TestAuthorizeAlterTable) {
  // Don't authorize alter table on a user without required privileges.
  ASSERT_OK(CreateRoleAndAddToGroups(kRoleName, kUserGroup));
  TSentryPrivilege db_privilege = GetDatabasePrivilege("db", "SELECT");
  ASSERT_OK(AlterRoleGrantPrivilege(kRoleName, db_privilege));
  Status s = sentry_authz_provider_->AuthorizeAlterTable("db.table", "db.table", kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Authorize alter table without rename on a user with proper privileges.
  db_privilege = GetDatabasePrivilege("db", "ALTER");
  ASSERT_OK(AlterRoleGrantPrivilege(kRoleName, db_privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeAlterTable("db.table", "db.table", kTestUser));

  // Table alteration with rename requires 'ALL ON TABLE <old-table>' and
  // 'CREATE ON DATABASE <new-database>'
  s = sentry_authz_provider_->AuthorizeAlterTable("db.table", "new_db.new_table", kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Authorize alter table without rename on a user with proper privileges.
  db_privilege = GetDatabasePrivilege("new_db", "CREATE");
  ASSERT_OK(AlterRoleGrantPrivilege(kRoleName, db_privilege));
  TSentryPrivilege table_privilege = GetTablePrivilege("db", "table", "ALL");
  ASSERT_OK(AlterRoleGrantPrivilege(kRoleName, table_privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeAlterTable("db.table",
                                                        "new_db.new_table",
                                                        kTestUser));
}

TEST_P(TestTableAuthorization, TestAuthorizeGetTableMetadata) {
  // Don't authorize delete table on a user without required privileges.
  ASSERT_OK(CreateRoleAndAddToGroups(kRoleName, kUserGroup));
  Status s = sentry_authz_provider_->AuthorizeGetTableMetadata("db.table", kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Authorize delete table on a user with proper privileges.
  TSentryPrivilege privilege = GetDatabasePrivilege("db", "SELECT");
  ASSERT_OK(AlterRoleGrantPrivilege(kRoleName, privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeGetTableMetadata("db.table", kTestUser));
}

// Checks that the SentryAuthzProvider handles reconnecting to Sentry after a connection failure,
// or service being too busy.
TEST_P(TestTableAuthorization, TestReconnect) {

  // Restart SentryAuthzProvider with configured timeout to reduce the run time of this test.
  NO_FATALS(sentry_authz_provider_->Stop());
  FLAGS_sentry_service_security_mode = KerberosEnabled() ? "kerberos" : "none";
  FLAGS_sentry_service_rpc_addresses = sentry_->address().ToString();
  FLAGS_sentry_service_send_timeout_seconds = AllowSlowTests() ? 5 : 2;
  FLAGS_sentry_service_recv_timeout_seconds = AllowSlowTests() ? 5 : 2;
  sentry_authz_provider_.reset(new SentryAuthzProvider());
  ASSERT_OK(sentry_authz_provider_->Start());

  ASSERT_OK(CreateRoleAndAddToGroups(kRoleName, kUserGroup));
  TSentryPrivilege privilege = GetDatabasePrivilege("db", "METADATA");
  ASSERT_OK(AlterRoleGrantPrivilege(kRoleName, privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeGetTableMetadata("db.table", kTestUser));

  // Shutdown Sentry and try a few operations.
  ASSERT_OK(StopSentry());

  Status s = sentry_authz_provider_->AuthorizeDropTable("db.table", kTestUser);
  EXPECT_TRUE(s.IsNetworkError()) << s.ToString();

  s = sentry_authz_provider_->AuthorizeCreateTable("db.table", kTestUser, "diff-user");
  EXPECT_TRUE(s.IsNetworkError()) << s.ToString();

  // Start Sentry back up and ensure that the same operations succeed.
  ASSERT_OK(StartSentry());
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(sentry_authz_provider_->AuthorizeGetTableMetadata(
        "db.table", kTestUser));
  });

  privilege = GetDatabasePrivilege("db", "DROP");
  ASSERT_OK(AlterRoleGrantPrivilege(kRoleName, privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeDropTable("db.table", kTestUser));

  // Pause Sentry and try a few operations.
  ASSERT_OK(sentry_->Pause());

  s = sentry_authz_provider_->AuthorizeDropTable("db.table", kTestUser);
  EXPECT_TRUE(s.IsTimedOut()) << s.ToString();

  s = sentry_authz_provider_->AuthorizeGetTableMetadata("db.table", kTestUser);
  EXPECT_TRUE(s.IsTimedOut()) << s.ToString();

  // Resume Sentry and ensure that the same operations succeed.
  ASSERT_OK(sentry_->Resume());
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(sentry_authz_provider_->AuthorizeDropTable(
        "db.table", kTestUser));
  });
}

TEST_P(TestTableAuthorization, TestInvalidAction) {
  ASSERT_OK(CreateRoleAndAddToGroups(kRoleName, kUserGroup));
  TSentryPrivilege privilege = GetDatabasePrivilege("db", "invalid");
  ASSERT_OK(AlterRoleGrantPrivilege(kRoleName, privilege));
  // User has privileges with invalid action cannot operate on the table.
  Status s = sentry_authz_provider_->AuthorizeCreateTable("DB.table", kTestUser, kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
}

TEST_P(TestTableAuthorization, TestInvalidAuthzScope) {
  ASSERT_OK(CreateRoleAndAddToGroups(kRoleName, kUserGroup));
  TSentryPrivilege privilege = GetDatabasePrivilege("db", "ALL");
  privilege.__set_privilegeScope("invalid");
  ASSERT_OK(AlterRoleGrantPrivilege(kRoleName, privilege));
  // User has privileges with invalid authorizable scope cannot operate
  // on the table.
  Status s = sentry_authz_provider_->AuthorizeCreateTable("DB.table", kTestUser, kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
}

// Ensures Sentry privileges are case insensitive.
TEST_P(TestTableAuthorization, TestPrivilegeCaseSensitivity) {
  ASSERT_OK(CreateRoleAndAddToGroups(kRoleName, kUserGroup));
  TSentryPrivilege privilege = GetDatabasePrivilege("db", "create");
  ASSERT_OK(AlterRoleGrantPrivilege(kRoleName, privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeCreateTable("DB.table", kTestUser, kTestUser));
}

// Test to ensure the authorization hierarchy rule of SentryAuthzProvider
// works as expected.
class TestAuthzHierarchy : public SentryAuthzProviderTest,
                           public ::testing::WithParamInterface<
                               tuple<bool, SentryAuthorizableScope::Scope>> {
 public:
  bool KerberosEnabled() const {
    return std::get<0>(GetParam());
  }
};

TEST_P(TestAuthzHierarchy, TestAuthorizableScope) {
  SentryAuthorizableScope::Scope scope = std::get<1>(GetParam());
  const string action = "ALL";
  const string db = "database";
  const string tbl = "table";
  const string col = "col";
  vector<TSentryPrivilege> lower_hierarchy_privs;
  vector<TSentryPrivilege> higher_hierarchy_privs;
  const TSentryPrivilege column_priv = GetColumnPrivilege(db, tbl, col, action);
  const TSentryPrivilege table_priv = GetTablePrivilege(db, tbl, action);
  const TSentryPrivilege db_priv = GetDatabasePrivilege(db, action);
  const TSentryPrivilege server_priv = GetServerPrivilege(action);

  switch (scope) {
    case SentryAuthorizableScope::Scope::TABLE:
      higher_hierarchy_privs.emplace_back(table_priv);
      FALLTHROUGH_INTENDED;
    case SentryAuthorizableScope::Scope::DATABASE:
      higher_hierarchy_privs.emplace_back(db_priv);
      FALLTHROUGH_INTENDED;
    case SentryAuthorizableScope::Scope::SERVER:
      higher_hierarchy_privs.emplace_back(server_priv);
      break;
    default:
      break;
  }

  switch (scope) {
    case SentryAuthorizableScope::Scope::SERVER:
      lower_hierarchy_privs.emplace_back(db_priv);
      FALLTHROUGH_INTENDED;
    case SentryAuthorizableScope::Scope::DATABASE:
      lower_hierarchy_privs.emplace_back(table_priv);
      FALLTHROUGH_INTENDED;
    case SentryAuthorizableScope::Scope::TABLE:
      lower_hierarchy_privs.emplace_back(column_priv);
      break;
    default:
      break;
  }

  // Privilege with higher scope on the hierarchy can imply privileges
  // with lower scope on the hierarchy.
  for (const auto& privilege : higher_hierarchy_privs) {
    ASSERT_OK(CreateRoleAndAddToGroups(kRoleName, kUserGroup));
    ASSERT_OK(AlterRoleGrantPrivilege(kRoleName, privilege));
    ASSERT_OK(sentry_authz_provider_->Authorize(scope, SentryAction::Action::ALL,
                                                Substitute("$0.$1", db, tbl), kTestUser));
    ASSERT_OK(DropRole(kRoleName));
  }

  // Privilege with lower scope on the hierarchy cannot imply privileges
  // with higher scope on the hierarchy.
  for (const auto& privilege : lower_hierarchy_privs) {
    ASSERT_OK(CreateRoleAndAddToGroups(kRoleName, kUserGroup));
    ASSERT_OK(AlterRoleGrantPrivilege(kRoleName, privilege));
    Status s = sentry_authz_provider_->Authorize(scope, SentryAction::Action::ALL,
                                                 Substitute("$0.$1", db, tbl), kTestUser);
    ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
    ASSERT_OK(DropRole(kRoleName));
  }
}

INSTANTIATE_TEST_CASE_P(AuthzCombinations, TestAuthzHierarchy,
    ::testing::Combine(::testing::Bool(),
                       // Scope::COLUMN is excluded since column scope for table
                       // authorizable doesn't make sense.
                       ::testing::Values(SentryAuthorizableScope::Scope::SERVER,
                                         SentryAuthorizableScope::Scope::DATABASE,
                                         SentryAuthorizableScope::Scope::TABLE)));

} // namespace master
} // namespace kudu
