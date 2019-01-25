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

#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "kudu/sentry/mini_sentry.h"
#include "kudu/sentry/sentry-test-base.h"
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
using sentry::TSentryGrantOption;
using sentry::TSentryGroup;
using sentry::TSentryPrivilege;
using std::unique_ptr;
using std::set;
using std::string;

namespace kudu {

using sentry::SentryTestBase;

namespace master {

// Class for SentryAuthzProvider tests. Parameterized by whether
// Kerberos should be enabled.
class SentryAuthzProviderTest : public SentryTestBase {
 public:
  const char* const kAdminUser = "test-admin";
  const char* const kUserGroup = "user";
  const char* const kTestUser = "test-user";

  void SetUp() override {
    SentryTestBase::SetUp();

    // Configure the SentryAuthzProvider flags.
    FLAGS_sentry_service_security_mode = kerberos_enabled_ ? "kerberos" : "none";
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

  // Returns a database level TSentryPrivilege with the given database name, action
  // and grant option.
  TSentryPrivilege GetDatabasePrivilege(const string& db_name,
                                        const string& action,
                                        const TSentryGrantOption::type& grant_option) {
    TSentryPrivilege privilege;
    privilege.__set_privilegeScope("DATABASE");
    privilege.__set_serverName(FLAGS_server_name);
    privilege.__set_dbName(db_name);
    privilege.__set_action(action);
    privilege.__set_grantOption(grant_option);
    return privilege;
  }

  // Returns a table level TSentryPrivilege with the given table name, database name,
  // action and grant option.
  TSentryPrivilege GetTablePrivilege(const string& db_name,
                                     const string& table_name,
                                     const string& action,
                                     const TSentryGrantOption::type& grant_option) {
    TSentryPrivilege privilege = GetDatabasePrivilege(db_name, action, grant_option);
    privilege.__set_tableName(table_name);
    return privilege;
  }

 protected:
  unique_ptr<SentryAuthzProvider> sentry_authz_provider_;
};

INSTANTIATE_TEST_CASE_P(KerberosEnabled, SentryAuthzProviderTest, ::testing::Bool());

TEST_P(SentryAuthzProviderTest, TestAuthorizeCreateTable) {
  // Don't authorize create table on a non-existent user.
  Status s = sentry_authz_provider_->AuthorizeCreateTable("db.table",
                                                          "non-existent-user",
                                                          "non-existent-user");
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Don't authorize create table on a user without any privileges.
  s = sentry_authz_provider_->AuthorizeCreateTable("db.table", kTestUser, kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Don't authorize create table on a user without required privileges.
  const string role_name = "developer";
  ASSERT_OK(CreateRoleAndAddToGroups(role_name, kUserGroup));
  TSentryPrivilege privilege = GetDatabasePrivilege("db", "DROP", TSentryGrantOption::DISABLED);
  ASSERT_OK(AlterRoleGrantPrivilege(role_name, privilege));
  s = sentry_authz_provider_->AuthorizeCreateTable("db.table", kTestUser, kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Authorize create table on a user with proper privileges.
  privilege = GetDatabasePrivilege("db", "CREATE", TSentryGrantOption::DISABLED);
  ASSERT_OK(AlterRoleGrantPrivilege(role_name, privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeCreateTable("db.table", kTestUser, kTestUser));

  // Table creation with a different owner than the user
  // requires the creating user have 'ALL on DATABASE' with grant.
  s = sentry_authz_provider_->AuthorizeCreateTable("db.table", kTestUser, "diff-user");
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  privilege = GetDatabasePrivilege("db", "ALL", TSentryGrantOption::ENABLED);
  ASSERT_OK(AlterRoleGrantPrivilege(role_name, privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeCreateTable("db.table", kTestUser, "diff-user"));
}

TEST_P(SentryAuthzProviderTest, TestAuthorizeDropTable) {
  // Don't authorize delete table on a user without required privileges.
  const string role_name = "developer";
  ASSERT_OK(CreateRoleAndAddToGroups(role_name, kUserGroup));
  TSentryPrivilege privilege = GetDatabasePrivilege("db", "SELECT", TSentryGrantOption::DISABLED);
  ASSERT_OK(AlterRoleGrantPrivilege(role_name, privilege));
  Status s = sentry_authz_provider_->AuthorizeDropTable("db.table", kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Authorize delete table on a user with proper privileges.
  privilege = GetDatabasePrivilege("db", "DROP", TSentryGrantOption::DISABLED);
  ASSERT_OK(AlterRoleGrantPrivilege(role_name, privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeDropTable("db.table", kTestUser));
}

TEST_P(SentryAuthzProviderTest, TestAuthorizeAlterTable) {
  // Don't authorize alter table on a user without required privileges.
  const string role_name = "developer";
  ASSERT_OK(CreateRoleAndAddToGroups(role_name, kUserGroup));
  TSentryPrivilege db_privilege = GetDatabasePrivilege("db", "SELECT",
                                                       TSentryGrantOption::DISABLED);
  ASSERT_OK(AlterRoleGrantPrivilege(role_name, db_privilege));
  Status s = sentry_authz_provider_->AuthorizeAlterTable("db.table", "db.table", kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Authorize alter table without rename on a user with proper privileges.
  db_privilege = GetDatabasePrivilege("db", "ALTER", TSentryGrantOption::DISABLED);
  ASSERT_OK(AlterRoleGrantPrivilege(role_name, db_privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeAlterTable("db.table", "db.table", kTestUser));

  // Table alteration with rename requires 'ALL ON TABLE <old-table>' and
  // 'CREATE ON DATABASE <new-database>'
  s = sentry_authz_provider_->AuthorizeAlterTable("db.table", "new_db.new_table", kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Authorize alter table without rename on a user with proper privileges.
  db_privilege = GetDatabasePrivilege("new_db", "CREATE", TSentryGrantOption::DISABLED);
  ASSERT_OK(AlterRoleGrantPrivilege(role_name, db_privilege));
  TSentryPrivilege table_privilege = GetTablePrivilege("db", "table", "ALL",
                                                       TSentryGrantOption::DISABLED);
  ASSERT_OK(AlterRoleGrantPrivilege(role_name, table_privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeAlterTable("db.table",
                                                        "new_db.new_table",
                                                        kTestUser));
}

TEST_P(SentryAuthzProviderTest, TestAuthorizeGetTableMetadata) {
  // Don't authorize delete table on a user without required privileges.
  const string role_name = "developer";
  ASSERT_OK(CreateRoleAndAddToGroups(role_name, kUserGroup));
  Status s = sentry_authz_provider_->AuthorizeGetTableMetadata("db.table", kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Authorize delete table on a user with proper privileges.
  TSentryPrivilege privilege = GetDatabasePrivilege("db", "SELECT", TSentryGrantOption::DISABLED);
  ASSERT_OK(AlterRoleGrantPrivilege(role_name, privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeGetTableMetadata("db.table", kTestUser));
}

// Checks that the SentryAuthzProvider handles reconnecting to Sentry after a connection failure,
// or service being too busy.
TEST_P(SentryAuthzProviderTest, TestReconnect) {

  // Restart SentryAuthzProvider with configured timeout to reduce the run time of this test.
  NO_FATALS(sentry_authz_provider_->Stop());
  FLAGS_sentry_service_security_mode = kerberos_enabled_ ? "kerberos" : "none";
  FLAGS_sentry_service_rpc_addresses = sentry_->address().ToString();
  FLAGS_sentry_service_send_timeout_seconds = AllowSlowTests() ? 5 : 2;
  FLAGS_sentry_service_recv_timeout_seconds = AllowSlowTests() ? 5 : 2;
  sentry_authz_provider_.reset(new SentryAuthzProvider());
  ASSERT_OK(sentry_authz_provider_->Start());

  const string role_name = "developer";
  ASSERT_OK(CreateRoleAndAddToGroups(role_name, kUserGroup));
  TSentryPrivilege privilege = GetDatabasePrivilege("db", "METADATA",
                                                    TSentryGrantOption::DISABLED);
  ASSERT_OK(AlterRoleGrantPrivilege(role_name, privilege));
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

  privilege = GetDatabasePrivilege("db", "DROP", TSentryGrantOption::DISABLED);
  ASSERT_OK(AlterRoleGrantPrivilege(role_name, privilege));
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

} // namespace master
} // namespace kudu
