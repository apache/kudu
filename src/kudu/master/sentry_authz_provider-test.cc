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

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/sentry_authz_provider-test-base.h"
#include "kudu/sentry/mini_sentry.h"
#include "kudu/sentry/sentry-test-base.h"
#include "kudu/sentry/sentry_action.h"
#include "kudu/sentry/sentry_authorizable_scope.h"
#include "kudu/sentry/sentry_client.h"
#include "kudu/sentry/sentry_policy_service_types.h"
#include "kudu/util/hdr_histogram.h"
#include "kudu/util/metrics.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int32(sentry_service_recv_timeout_seconds);
DECLARE_int32(sentry_service_send_timeout_seconds);
DECLARE_string(sentry_service_rpc_addresses);
DECLARE_string(server_name);
DECLARE_string(trusted_user_acl);

METRIC_DECLARE_counter(sentry_client_tasks_successful);
METRIC_DECLARE_counter(sentry_client_tasks_failed_fatal);
METRIC_DECLARE_counter(sentry_client_tasks_failed_nonfatal);
METRIC_DECLARE_counter(sentry_client_reconnections_succeeded);
METRIC_DECLARE_counter(sentry_client_reconnections_failed);
METRIC_DECLARE_histogram(sentry_client_task_execution_time_us);

using kudu::sentry::AuthorizableScopesSet;
using kudu::sentry::SentryAction;
using kudu::sentry::SentryActionsSet;
using kudu::sentry::SentryTestBase;
using kudu::sentry::SentryAuthorizableScope;
using sentry::TSentryAuthorizable;
using sentry::TSentryGrantOption;
using sentry::TSentryPrivilege;
using std::string;
using std::tuple;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace master {

TEST(SentryAuthzProviderStaticTest, TestTrustedUserAcl) {
  FLAGS_trusted_user_acl = "impala,hive,hdfs";
  SentryAuthzProvider authz_provider;
  ASSERT_TRUE(authz_provider.IsTrustedUser("impala"));
  ASSERT_TRUE(authz_provider.IsTrustedUser("hive"));
  ASSERT_TRUE(authz_provider.IsTrustedUser("hdfs"));
  ASSERT_FALSE(authz_provider.IsTrustedUser("untrusted"));
}

// Basic unit test for validations on ill-formed privileges.
TEST(SentryAuthzProviderStaticTest, TestPrivilegesWellFormed) {
  const string kDb = "db";
  const string kTable = "table";
  TSentryAuthorizable requested_authorizable;
  requested_authorizable.__set_server(FLAGS_server_name);
  requested_authorizable.__set_db(kDb);
  requested_authorizable.__set_table(kTable);
  TSentryPrivilege real_privilege = GetTablePrivilege(kDb, kTable, "ALL");
  {
    // Privilege with a bogus action set.
    TSentryPrivilege privilege = real_privilege;
    privilege.__set_action("NotAnAction");
    ASSERT_FALSE(SentryAuthzProvider::SentryPrivilegeIsWellFormed(
        privilege, requested_authorizable, /*scope=*/nullptr, /*action=*/nullptr));
  }
  {
    // Privilege with a bogus authorizable scope set.
    TSentryPrivilege privilege = real_privilege;
    privilege.__set_privilegeScope("NotAnAuthorizableScope");
    ASSERT_FALSE(SentryAuthzProvider::SentryPrivilegeIsWellFormed(
        privilege, requested_authorizable, /*scope=*/nullptr, /*action=*/nullptr));
  }
  {
    // Privilege with a valid, but unexpected scope for the set fields.
    TSentryPrivilege privilege = real_privilege;
    privilege.__set_privilegeScope("COLUMN");
    ASSERT_FALSE(SentryAuthzProvider::SentryPrivilegeIsWellFormed(
        privilege, requested_authorizable, /*scope=*/nullptr, /*action=*/nullptr));
  }
  {
    // Privilege with a messed up scope field at a higher scope than that
    // requested.
    TSentryPrivilege privilege = real_privilege;
    privilege.__set_dbName("NotTheActualDb");
    ASSERT_FALSE(SentryAuthzProvider::SentryPrivilegeIsWellFormed(
        privilege, requested_authorizable, /*scope=*/nullptr, /*action=*/nullptr));
  }
  {
    // Privilege with an field set that isn't meant to be set at its scope.
    TSentryPrivilege privilege = real_privilege;
    privilege.__set_columnName("SomeColumn");
    ASSERT_FALSE(SentryAuthzProvider::SentryPrivilegeIsWellFormed(
        privilege, requested_authorizable, /*scope=*/nullptr, /*action=*/nullptr));
  }
  {
    // Privilege with a missing field for its scope.
    TSentryPrivilege privilege = real_privilege;
    privilege.__isset.tableName = false;
    ASSERT_FALSE(SentryAuthzProvider::SentryPrivilegeIsWellFormed(
        privilege, requested_authorizable, /*scope=*/nullptr, /*action=*/nullptr));
  }
  {
    // Finally, the correct table-level privilege.
    SentryAuthorizableScope::Scope granted_scope;
    SentryAction::Action granted_action;
    real_privilege.printTo(LOG(INFO));
    ASSERT_TRUE(SentryAuthzProvider::SentryPrivilegeIsWellFormed(
        real_privilege, requested_authorizable, &granted_scope, &granted_action));
    ASSERT_EQ(SentryAuthorizableScope::TABLE, granted_scope);
    ASSERT_EQ(SentryAction::ALL, granted_action);
  }
}

class SentryAuthzProviderTest : public SentryTestBase {
 public:
  const char* const kTestUser = "test-user";
  const char* const kUserGroup = "user";
  const char* const kRoleName = "developer";

  void SetUp() override {
    SentryTestBase::SetUp();

    metric_entity_ = METRIC_ENTITY_server.Instantiate(
        &metric_registry_, "sentry_auth_provider-test");

    // Configure the SentryAuthzProvider flags.
    FLAGS_sentry_service_rpc_addresses = sentry_->address().ToString();
    sentry_authz_provider_.reset(new SentryAuthzProvider(metric_entity_));
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

  bool KerberosEnabled() const override {
    return true;
  }

#define GET_GAUGE_READINGS(func_name, counter_name_suffix) \
  int64_t func_name() { \
    scoped_refptr<Counter> gauge(metric_entity_->FindOrCreateCounter( \
        &METRIC_sentry_client_##counter_name_suffix)); \
    CHECK(gauge); \
    return gauge->value(); \
  }
  GET_GAUGE_READINGS(GetTasksSuccessful, tasks_successful)
  GET_GAUGE_READINGS(GetTasksFailedFatal, tasks_failed_fatal)
  GET_GAUGE_READINGS(GetTasksFailedNonFatal, tasks_failed_nonfatal)
  GET_GAUGE_READINGS(GetReconnectionsSucceeded, reconnections_succeeded)
  GET_GAUGE_READINGS(GetReconnectionsFailed, reconnections_failed)
#undef GET_GAUGE_READINGS

 protected:
  MetricRegistry metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
  unique_ptr<SentryAuthzProvider> sentry_authz_provider_;
};

namespace {

// Indicates different invalid privilege response types to be injected.
enum class InvalidPrivilege {
  // No error is injected.
  NONE,

  // The action string is set to something other than the expected action.
  INCORRECT_ACTION,

  // The scope string is set to something other than the expected scope.
  INCORRECT_SCOPE,

  // The 'serverName' field is set to something other than the authorizable's
  // server name. Why just the server? This guarantees that a request for any
  // authorizable scope will ignore such invalid privileges. E.g. say we
  // instead granted an incorrect 'tableName'; assuming the dbName were still
  // correct, a request at the database scope would correctly _not_ ignore the
  // privilege. So to ensure that these InvalidPrivileges always yield
  // privileges that are ignored, we exclusively butcher the 'server' field.
  INCORRECT_SERVER,

  // One of the scope fields (e.g. serverName, dbName, etc.) is unexpectedly
  // missing or unexpectedly set. Note: Sentry servers don't allow an empty
  // 'server' scope; if erasing the 'server' field, we'll instead set it to
  // something other than the expected server.
  FLIPPED_FIELD,
};

const SentryActionsSet kAllActions({
  SentryAction::ALL,
  SentryAction::METADATA,
  SentryAction::SELECT,
  SentryAction::INSERT,
  SentryAction::UPDATE,
  SentryAction::DELETE,
  SentryAction::ALTER,
  SentryAction::CREATE,
  SentryAction::DROP,
  SentryAction::OWNER,
});

constexpr const char* kDb = "db";
constexpr const char* kTable = "table";
constexpr const char* kColumn = "column";

} // anonymous namespace

class SentryAuthzProviderFilterResponsesTest :
    public SentryAuthzProviderTest,
    public ::testing::WithParamInterface<SentryAuthorizableScope::Scope> {
 public:
  SentryAuthzProviderFilterResponsesTest()
      : prng_(SeedRandom()) {}

  void SetUp() override {
    SentryAuthzProviderTest::SetUp();
    ASSERT_OK(CreateRoleAndAddToGroups(sentry_client_.get(), kRoleName, kUserGroup));
    full_authorizable_.server = FLAGS_server_name;
    full_authorizable_.db = kDb;
    full_authorizable_.table = kTable;
    full_authorizable_.column = kColumn;
  }

  // Creates a Sentry privilege for the user based on the given action,
  // the given scope, and the given authorizable that has all scope fields set.
  // With all of the scope fields set in the authorizable, and a given scope,
  // we can return an appropriate privilege for it, with tweaks indicated by
  // 'invalid_privilege' to make the privilege invalid if desired.
  TSentryPrivilege CreatePrivilege(const TSentryAuthorizable& full_authorizable,
                                   const SentryAuthorizableScope& scope, const SentryAction& action,
                                   InvalidPrivilege invalid_privilege = InvalidPrivilege::NONE) {
    DCHECK(!full_authorizable.server.empty() && !full_authorizable.db.empty() &&
          !full_authorizable.table.empty() && !full_authorizable.column.empty());
    TSentryPrivilege privilege;
    privilege.__set_action(invalid_privilege == InvalidPrivilege::INCORRECT_ACTION ?
                           "foobar" : ActionToString(action.action()));
    privilege.__set_privilegeScope(invalid_privilege == InvalidPrivilege::INCORRECT_SCOPE ?
                                   "foobar" : ScopeToString(scope.scope()));

    // Select a scope at which we'll mess up the privilege request's field.
    AuthorizableScopesSet nonempty_fields =
        SentryAuthzProvider::ExpectedNonEmptyFields(scope.scope());
    if (invalid_privilege == InvalidPrivilege::FLIPPED_FIELD) {
      static const AuthorizableScopesSet kMessUpCandidates = {
        SentryAuthorizableScope::SERVER,
        SentryAuthorizableScope::DATABASE,
        SentryAuthorizableScope::TABLE,
        SentryAuthorizableScope::COLUMN,
      };
      SentryAuthorizableScope::Scope field_to_mess_up =
          SelectRandomElement<AuthorizableScopesSet, SentryAuthorizableScope::Scope, Random>(
              kMessUpCandidates, &prng_);
      if (ContainsKey(nonempty_fields, field_to_mess_up)) {
        // Since Sentry servers don't allow empty 'server' fields in requests,
        // rather flipping the empty status of the field, inject an incorrect
        // value for the field.
        if (field_to_mess_up == SentryAuthorizableScope::SERVER) {
          invalid_privilege = InvalidPrivilege::INCORRECT_SERVER;
        } else {
          nonempty_fields.erase(field_to_mess_up);
        }
      } else {
        InsertOrDie(&nonempty_fields, field_to_mess_up);
      }
    }

    // Fill in any fields we may need.
    for (const auto& field : nonempty_fields) {
      switch (field) {
        case SentryAuthorizableScope::SERVER:
          privilege.__set_serverName(invalid_privilege == InvalidPrivilege::INCORRECT_SERVER ?
                                     "foobar" : full_authorizable.server);
          break;
        case SentryAuthorizableScope::DATABASE:
          privilege.__set_dbName(full_authorizable.db);
          break;
        case SentryAuthorizableScope::TABLE:
          privilege.__set_tableName(full_authorizable.table);
          break;
        case SentryAuthorizableScope::COLUMN:
          privilege.__set_columnName(full_authorizable.column);
          break;
        default:
          LOG(FATAL) << "not a valid scope field: " << field;
      }
    }
    return privilege;
  }
 protected:
  // Authorizable that has all scope fields set; useful for generating
  // privilege requests.
  TSentryAuthorizable full_authorizable_;

 private:
  mutable Random prng_;
};

// Attempst to grant privileges for various actions on a single scope of an
// authorizable, injecting various invalid privileges, and checking that Kudu
// ignores them.
TEST_P(SentryAuthzProviderFilterResponsesTest, TestFilterInvalidResponses) {
  const string& table_ident = Substitute("$0.$1", full_authorizable_.db, full_authorizable_.table);
  static constexpr InvalidPrivilege kInvalidPrivileges[] = {
      InvalidPrivilege::INCORRECT_ACTION,
      InvalidPrivilege::INCORRECT_SCOPE,
      InvalidPrivilege::INCORRECT_SERVER,
      InvalidPrivilege::FLIPPED_FIELD,
  };
  SentryAuthorizableScope granted_scope(GetParam());
  for (const auto& action : kAllActions) {
    for (const auto& ip : kInvalidPrivileges) {
      TSentryPrivilege privilege = CreatePrivilege(full_authorizable_, granted_scope,
                                                   SentryAction(action), ip);
      ASSERT_OK(AlterRoleGrantPrivilege(sentry_client_.get(), kRoleName, privilege));
    }
  }
  for (const auto& requested_scope : { SentryAuthorizableScope::SERVER,
                                       SentryAuthorizableScope::DATABASE,
                                       SentryAuthorizableScope::TABLE }) {
    SentryPrivilegesBranch privileges;
    ASSERT_OK(sentry_authz_provider_->GetSentryPrivileges(
        requested_scope, table_ident, kTestUser, &privileges));
    // Kudu should ignore all of the invalid privileges.
    ASSERT_TRUE(privileges.privileges.empty());
  }
}

// Grants privileges for various actions on a single scope of an authorizable.
TEST_P(SentryAuthzProviderFilterResponsesTest, TestFilterValidResponses) {
  const string& table_ident = Substitute("$0.$1", full_authorizable_.db, full_authorizable_.table);
  SentryAuthorizableScope granted_scope(GetParam());
  // Send valid requests and verify that we can get it back through the
  // SentryAuthzProvider.
  for (const auto& action : kAllActions) {
    TSentryPrivilege privilege = CreatePrivilege(full_authorizable_, granted_scope,
                                                 SentryAction(action));
    ASSERT_OK(AlterRoleGrantPrivilege(sentry_client_.get(), kRoleName, privilege));
  }
  for (const auto& requested_scope : { SentryAuthorizableScope::SERVER,
                                       SentryAuthorizableScope::DATABASE,
                                       SentryAuthorizableScope::TABLE }) {
    SentryPrivilegesBranch privileges;
    ASSERT_OK(sentry_authz_provider_->GetSentryPrivileges(
        requested_scope, table_ident, kTestUser, &privileges));
    ASSERT_EQ(1, privileges.privileges.size());
    const auto& authorizable_privileges = privileges.privileges[0];
    ASSERT_EQ(GetParam(), authorizable_privileges.scope)
        << ScopeToString(authorizable_privileges.scope);
    ASSERT_FALSE(authorizable_privileges.granted_privileges.empty());
  }
}

INSTANTIATE_TEST_CASE_P(GrantedScopes, SentryAuthzProviderFilterResponsesTest,
                        ::testing::Values(SentryAuthorizableScope::SERVER,
                                          SentryAuthorizableScope::DATABASE,
                                          SentryAuthorizableScope::TABLE,
                                          SentryAuthorizableScope::COLUMN));



// Test to create tables requiring ALL ON DATABASE with the grant option. This
// is parameterized on the ALL scope and OWNER actions, which behave
// identically.
class CreateTableAuthorizationTest : public SentryAuthzProviderTest,
                                     public ::testing::WithParamInterface<string> {};

TEST_P(CreateTableAuthorizationTest, TestAuthorizeCreateTable) {
  // Don't authorize create table on a non-existent user.
  Status s = sentry_authz_provider_->AuthorizeCreateTable("db.table",
                                                          "non-existent-user",
                                                          "non-existent-user");
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Don't authorize create table on a user without any privileges.
  s = sentry_authz_provider_->AuthorizeCreateTable("db.table", kTestUser, kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Don't authorize create table on a user without required privileges.
  ASSERT_OK(CreateRoleAndAddToGroups(sentry_client_.get(), kRoleName, kUserGroup));
  TSentryPrivilege privilege = GetDatabasePrivilege("db", "DROP");
  ASSERT_OK(AlterRoleGrantPrivilege(sentry_client_.get(), kRoleName, privilege));
  s = sentry_authz_provider_->AuthorizeCreateTable("db.table", kTestUser, kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Authorize create table on a user with proper privileges.
  privilege = GetDatabasePrivilege("db", "CREATE");
  ASSERT_OK(AlterRoleGrantPrivilege(sentry_client_.get(), kRoleName, privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeCreateTable("db.table", kTestUser, kTestUser));

  // Table creation with a different owner than the user
  // requires the creating user have 'ALL on DATABASE' with grant.
  s = sentry_authz_provider_->AuthorizeCreateTable("db.table", kTestUser, "diff-user");
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  const auto& all = GetParam();
  privilege = GetDatabasePrivilege("db", all);
  s = sentry_authz_provider_->AuthorizeCreateTable("db.table", kTestUser, "diff-user");
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  privilege = GetDatabasePrivilege("db", all, TSentryGrantOption::ENABLED);
  ASSERT_OK(AlterRoleGrantPrivilege(sentry_client_.get(), kRoleName, privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeCreateTable("db.table", kTestUser, "diff-user"));
}

INSTANTIATE_TEST_CASE_P(AllOrOwner, CreateTableAuthorizationTest,
    ::testing::Values("ALL", "OWNER"));

TEST_F(SentryAuthzProviderTest, TestAuthorizeDropTable) {
  // Don't authorize delete table on a user without required privileges.
  ASSERT_OK(CreateRoleAndAddToGroups(sentry_client_.get(), kRoleName, kUserGroup));
  TSentryPrivilege privilege = GetDatabasePrivilege("db", "SELECT");
  ASSERT_OK(AlterRoleGrantPrivilege(sentry_client_.get(), kRoleName, privilege));
  Status s = sentry_authz_provider_->AuthorizeDropTable("db.table", kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Authorize delete table on a user with proper privileges.
  privilege = GetDatabasePrivilege("db", "DROP");
  ASSERT_OK(AlterRoleGrantPrivilege(sentry_client_.get(), kRoleName, privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeDropTable("db.table", kTestUser));
}

TEST_F(SentryAuthzProviderTest, TestAuthorizeAlterTable) {
  // Don't authorize alter table on a user without required privileges.
  ASSERT_OK(CreateRoleAndAddToGroups(sentry_client_.get(), kRoleName, kUserGroup));
  TSentryPrivilege db_privilege = GetDatabasePrivilege("db", "SELECT");
  ASSERT_OK(AlterRoleGrantPrivilege(sentry_client_.get(), kRoleName, db_privilege));
  Status s = sentry_authz_provider_->AuthorizeAlterTable("db.table", "db.table", kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Authorize alter table without rename on a user with proper privileges.
  db_privilege = GetDatabasePrivilege("db", "ALTER");
  ASSERT_OK(AlterRoleGrantPrivilege(sentry_client_.get(), kRoleName, db_privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeAlterTable("db.table", "db.table", kTestUser));

  // Table alteration with rename requires 'ALL ON TABLE <old-table>' and
  // 'CREATE ON DATABASE <new-database>'
  s = sentry_authz_provider_->AuthorizeAlterTable("db.table", "new_db.new_table", kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Authorize alter table without rename on a user with proper privileges.
  db_privilege = GetDatabasePrivilege("new_db", "CREATE");
  ASSERT_OK(AlterRoleGrantPrivilege(sentry_client_.get(), kRoleName, db_privilege));
  TSentryPrivilege table_privilege = GetTablePrivilege("db", "table", "ALL");
  ASSERT_OK(AlterRoleGrantPrivilege(sentry_client_.get(), kRoleName, table_privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeAlterTable("db.table",
                                                        "new_db.new_table",
                                                        kTestUser));
}

TEST_F(SentryAuthzProviderTest, TestAuthorizeGetTableMetadata) {
  // Don't authorize delete table on a user without required privileges.
  ASSERT_OK(CreateRoleAndAddToGroups(sentry_client_.get(), kRoleName, kUserGroup));
  Status s = sentry_authz_provider_->AuthorizeGetTableMetadata("db.table", kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Authorize delete table on a user with proper privileges.
  TSentryPrivilege privilege = GetDatabasePrivilege("db", "SELECT");
  ASSERT_OK(AlterRoleGrantPrivilege(sentry_client_.get(), kRoleName, privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeGetTableMetadata("db.table", kTestUser));
}

// Checks that the SentryAuthzProvider handles reconnecting to Sentry after a connection failure,
// or service being too busy.
TEST_F(SentryAuthzProviderTest, TestReconnect) {

  // Restart SentryAuthzProvider with configured timeout to reduce the run time of this test.
  NO_FATALS(sentry_authz_provider_->Stop());
  FLAGS_sentry_service_rpc_addresses = sentry_->address().ToString();
  FLAGS_sentry_service_send_timeout_seconds = AllowSlowTests() ? 5 : 2;
  FLAGS_sentry_service_recv_timeout_seconds = AllowSlowTests() ? 5 : 2;
  sentry_authz_provider_.reset(new SentryAuthzProvider());
  ASSERT_OK(sentry_authz_provider_->Start());

  ASSERT_OK(CreateRoleAndAddToGroups(sentry_client_.get(), kRoleName, kUserGroup));
  TSentryPrivilege privilege = GetDatabasePrivilege("db", "METADATA");
  ASSERT_OK(AlterRoleGrantPrivilege(sentry_client_.get(), kRoleName, privilege));
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
  ASSERT_OK(AlterRoleGrantPrivilege(sentry_client_.get(), kRoleName, privilege));
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

TEST_F(SentryAuthzProviderTest, TestInvalidAction) {
  ASSERT_OK(CreateRoleAndAddToGroups(sentry_client_.get(), kRoleName, kUserGroup));
  TSentryPrivilege privilege = GetDatabasePrivilege("db", "invalid");
  ASSERT_OK(AlterRoleGrantPrivilege(sentry_client_.get(), kRoleName, privilege));
  // User has privileges with invalid action cannot operate on the table.
  Status s = sentry_authz_provider_->AuthorizeCreateTable("DB.table", kTestUser, kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
}

TEST_F(SentryAuthzProviderTest, TestInvalidAuthzScope) {
  ASSERT_OK(CreateRoleAndAddToGroups(sentry_client_.get(), kRoleName, kUserGroup));
  TSentryPrivilege privilege = GetDatabasePrivilege("db", "ALL");
  privilege.__set_privilegeScope("invalid");
  ASSERT_OK(AlterRoleGrantPrivilege(sentry_client_.get(), kRoleName, privilege));
  // User has privileges with invalid authorizable scope cannot operate
  // on the table.
  Status s = sentry_authz_provider_->AuthorizeCreateTable("DB.table", kTestUser, kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
}

// Ensures Sentry privileges are case insensitive.
TEST_F(SentryAuthzProviderTest, TestPrivilegeCaseSensitivity) {
  ASSERT_OK(CreateRoleAndAddToGroups(sentry_client_.get(), kRoleName, kUserGroup));
  TSentryPrivilege privilege = GetDatabasePrivilege("db", "create");
  ASSERT_OK(AlterRoleGrantPrivilege(sentry_client_.get(), kRoleName, privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeCreateTable("DB.table", kTestUser, kTestUser));
}

// Test to ensure the authorization hierarchy rule of SentryAuthzProvider
// works as expected.
class TestAuthzHierarchy : public SentryAuthzProviderTest,
                           public ::testing::WithParamInterface<SentryAuthorizableScope::Scope> {};

TEST_P(TestAuthzHierarchy, TestAuthorizableScope) {
  SentryAuthorizableScope::Scope scope = GetParam();
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
    ASSERT_OK(CreateRoleAndAddToGroups(sentry_client_.get(), kRoleName, kUserGroup));
    ASSERT_OK(AlterRoleGrantPrivilege(sentry_client_.get(), kRoleName, privilege));
    ASSERT_OK(sentry_authz_provider_->Authorize(scope, SentryAction::Action::ALL,
                                                Substitute("$0.$1", db, tbl), kTestUser));
    ASSERT_OK(DropRole(sentry_client_.get(), kRoleName));
  }

  // Privilege with lower scope on the hierarchy cannot imply privileges
  // with higher scope on the hierarchy.
  for (const auto& privilege : lower_hierarchy_privs) {
    ASSERT_OK(CreateRoleAndAddToGroups(sentry_client_.get(), kRoleName, kUserGroup));
    ASSERT_OK(AlterRoleGrantPrivilege(sentry_client_.get(), kRoleName, privilege));
    Status s = sentry_authz_provider_->Authorize(scope, SentryAction::Action::ALL,
                                                 Substitute("$0.$1", db, tbl), kTestUser);
    ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
    ASSERT_OK(DropRole(sentry_client_.get(), kRoleName));
  }
}

INSTANTIATE_TEST_CASE_P(AuthzCombinations, TestAuthzHierarchy,
                       // Scope::COLUMN is excluded since column scope for table
                       // authorizable doesn't make sense.
                       ::testing::Values(SentryAuthorizableScope::Scope::SERVER,
                                         SentryAuthorizableScope::Scope::DATABASE,
                                         SentryAuthorizableScope::Scope::TABLE));

// Test to verify the functionality of metrics in HA Sentry client used in
// SentryAuthzProvider to communicate with Sentry.
class TestSentryClientMetrics : public SentryAuthzProviderTest {
 public:
  bool KerberosEnabled() const {
    return false;
  }
};

TEST_F(TestSentryClientMetrics, Basic) {
  ASSERT_EQ(0, GetTasksSuccessful());
  ASSERT_EQ(0, GetTasksFailedFatal());
  ASSERT_EQ(0, GetTasksFailedNonFatal());
  ASSERT_EQ(0, GetReconnectionsSucceeded());
  ASSERT_EQ(0, GetReconnectionsFailed());

  Status s = sentry_authz_provider_->AuthorizeCreateTable("db.table",
                                                          kTestUser, kTestUser);
  // The call should be counted as successful, and the client should connect
  // to Sentry (counted as reconnect).
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  ASSERT_EQ(1, GetTasksSuccessful());
  ASSERT_EQ(0, GetTasksFailedFatal());
  ASSERT_EQ(0, GetTasksFailedNonFatal());
  ASSERT_EQ(1, GetReconnectionsSucceeded());
  ASSERT_EQ(0, GetReconnectionsFailed());

  // Stop Sentry, and try the same call again. There should be a fatal error
  // reported, and then there should be failed reconnection attempts.
  ASSERT_OK(StopSentry());
  s = sentry_authz_provider_->AuthorizeCreateTable("db.table",
                                                   kTestUser, kTestUser);
  ASSERT_TRUE(s.IsNetworkError()) << s.ToString();
  ASSERT_EQ(1, GetTasksSuccessful());
  ASSERT_EQ(1, GetTasksFailedFatal());
  ASSERT_EQ(0, GetTasksFailedNonFatal());
  ASSERT_LE(1, GetReconnectionsFailed());
  ASSERT_EQ(1, GetReconnectionsSucceeded());

  ASSERT_OK(StartSentry());
  s = sentry_authz_provider_->AuthorizeCreateTable("db.table",
                                                   kTestUser, kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  ASSERT_EQ(2, GetTasksSuccessful());
  ASSERT_EQ(1, GetTasksFailedFatal());
  ASSERT_EQ(0, GetTasksFailedNonFatal());
  ASSERT_EQ(2, GetReconnectionsSucceeded());

  // NotAuthorized() from Sentry itself considered as a fatal error.
  // TODO(KUDU-2769): clarify whether it is a bug in HaClient or Sentry itself?
  s = sentry_authz_provider_->AuthorizeCreateTable("db.table",
                                                   "nobody", "nobody");
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  ASSERT_EQ(2, GetTasksSuccessful());
  ASSERT_EQ(2, GetTasksFailedFatal());
  ASSERT_EQ(0, GetTasksFailedNonFatal());
  // Once a new fatal error is registered, the client should reconnect.
  ASSERT_EQ(3, GetReconnectionsSucceeded());

  // Shorten the default timeout parameters: make timeout interval shorter.
  NO_FATALS(sentry_authz_provider_->Stop());
  FLAGS_sentry_service_rpc_addresses = sentry_->address().ToString();
  FLAGS_sentry_service_send_timeout_seconds = 2;
  FLAGS_sentry_service_recv_timeout_seconds = 2;
  sentry_authz_provider_.reset(new SentryAuthzProvider(metric_entity_));
  ASSERT_OK(sentry_authz_provider_->Start());

  ASSERT_OK(CreateRoleAndAddToGroups(sentry_client_.get(), kRoleName, kUserGroup));
  const auto privilege = GetDatabasePrivilege("db", "create");
  ASSERT_OK(AlterRoleGrantPrivilege(sentry_client_.get(), kRoleName, privilege));

  // Pause Sentry and try to send an RPC, expecting it to time out.
  ASSERT_OK(sentry_->Pause());
  s = sentry_authz_provider_->AuthorizeCreateTable(
      "db.table", kTestUser, kTestUser);
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_OK(sentry_->Resume());

  scoped_refptr<Histogram> hist(metric_entity_->FindOrCreateHistogram(
      &METRIC_sentry_client_task_execution_time_us));
  ASSERT_LT(0, hist->histogram()->MinValue());
  ASSERT_LT(2000000, hist->histogram()->MaxValue());
  ASSERT_LE(5, hist->histogram()->TotalCount());
  ASSERT_LT(2000000, hist->histogram()->TotalSum());
}

} // namespace master
} // namespace kudu
