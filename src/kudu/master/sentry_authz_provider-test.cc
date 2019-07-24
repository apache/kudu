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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/port.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/integral_types.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/master/sentry_authz_provider-test-base.h"
#include "kudu/master/sentry_privileges_fetcher.h"
#include "kudu/security/token.pb.h"
#include "kudu/sentry/mini_sentry.h"
#include "kudu/sentry/sentry-test-base.h"
#include "kudu/sentry/sentry_action.h"
#include "kudu/sentry/sentry_authorizable_scope.h"
#include "kudu/sentry/sentry_client.h"
#include "kudu/sentry/sentry_policy_service_types.h"
#include "kudu/util/barrier.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/hdr_histogram.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/ttl_cache.h"

DEFINE_int32(num_table_privileges, 100,
    "Number of table privileges to use in testing");
TAG_FLAG(num_table_privileges, hidden);

DEFINE_int32(num_databases, 10,
    "Number of databases to use in testing");
TAG_FLAG(num_databases, hidden);

DEFINE_int32(num_tables_per_db, 10,
    "Number of tables to use per database to use in testing");
TAG_FLAG(num_tables_per_db, hidden);

DEFINE_bool(has_db_privileges, true,
    "Whether the user should have db-level privileges in testing");
TAG_FLAG(has_db_privileges, hidden);

DECLARE_bool(sentry_require_db_privileges_for_list_tables);
DECLARE_int32(sentry_service_recv_timeout_seconds);
DECLARE_int32(sentry_service_send_timeout_seconds);
DECLARE_uint32(sentry_privileges_cache_capacity_mb);
DECLARE_string(sentry_service_rpc_addresses);
DECLARE_string(server_name);
DECLARE_string(trusted_user_acl);

METRIC_DECLARE_counter(sentry_client_tasks_successful);
METRIC_DECLARE_counter(sentry_client_tasks_failed_fatal);
METRIC_DECLARE_counter(sentry_client_tasks_failed_nonfatal);
METRIC_DECLARE_counter(sentry_client_reconnections_succeeded);
METRIC_DECLARE_counter(sentry_client_reconnections_failed);
METRIC_DECLARE_histogram(sentry_client_task_execution_time_us);

METRIC_DECLARE_counter(sentry_privileges_cache_evictions);
METRIC_DECLARE_counter(sentry_privileges_cache_evictions_expired);
METRIC_DECLARE_counter(sentry_privileges_cache_hits);
METRIC_DECLARE_counter(sentry_privileges_cache_hits_expired);
METRIC_DECLARE_counter(sentry_privileges_cache_inserts);
METRIC_DECLARE_counter(sentry_privileges_cache_lookups);
METRIC_DECLARE_counter(sentry_privileges_cache_misses);
METRIC_DECLARE_gauge_uint64(sentry_privileges_cache_memory_usage);

using kudu::pb_util::SecureDebugString;
using kudu::security::ColumnPrivilegePB;
using kudu::security::TablePrivilegePB;
using kudu::sentry::AuthorizableScopesSet;
using kudu::sentry::SentryAction;
using kudu::sentry::SentryActionsSet;
using kudu::sentry::SentryTestBase;
using kudu::sentry::SentryAuthorizableScope;
using google::protobuf::util::MessageDifferencer;
using sentry::TSentryAuthorizable;
using sentry::TSentryGrantOption;
using sentry::TSentryPrivilege;
using std::string;
using std::thread;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
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
TEST(SentryPrivilegesFetcherStaticTest, TestPrivilegesWellFormed) {
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
    ASSERT_FALSE(SentryPrivilegesFetcher::SentryPrivilegeIsWellFormed(
        privilege, requested_authorizable, /*scope=*/nullptr, /*action=*/nullptr));
  }
  {
    // Privilege with a bogus authorizable scope set.
    TSentryPrivilege privilege = real_privilege;
    privilege.__set_privilegeScope("NotAnAuthorizableScope");
    ASSERT_FALSE(SentryPrivilegesFetcher::SentryPrivilegeIsWellFormed(
        privilege, requested_authorizable, /*scope=*/nullptr, /*action=*/nullptr));
  }
  {
    // Privilege with a valid, but unexpected scope for the set fields.
    TSentryPrivilege privilege = real_privilege;
    privilege.__set_privilegeScope("COLUMN");
    ASSERT_FALSE(SentryPrivilegesFetcher::SentryPrivilegeIsWellFormed(
        privilege, requested_authorizable, /*scope=*/nullptr, /*action=*/nullptr));
  }
  {
    // Privilege with a messed up scope field at a higher scope than that
    // requested.
    TSentryPrivilege privilege = real_privilege;
    privilege.__set_dbName("NotTheActualDb");
    ASSERT_FALSE(SentryPrivilegesFetcher::SentryPrivilegeIsWellFormed(
        privilege, requested_authorizable, /*scope=*/nullptr, /*action=*/nullptr));
  }
  {
    // Privilege with an field set that isn't meant to be set at its scope.
    TSentryPrivilege privilege = real_privilege;
    privilege.__set_columnName("SomeColumn");
    ASSERT_FALSE(SentryPrivilegesFetcher::SentryPrivilegeIsWellFormed(
        privilege, requested_authorizable, /*scope=*/nullptr, /*action=*/nullptr));
  }
  {
    // Privilege with a missing field for its scope.
    TSentryPrivilege privilege = real_privilege;
    privilege.__isset.tableName = false;
    ASSERT_FALSE(SentryPrivilegesFetcher::SentryPrivilegeIsWellFormed(
        privilege, requested_authorizable, /*scope=*/nullptr, /*action=*/nullptr));
  }
  {
    // Finally, the correct table-level privilege.
    SentryAuthorizableScope::Scope granted_scope;
    SentryAction::Action granted_action;
    real_privilege.printTo(LOG(INFO));
    ASSERT_TRUE(SentryPrivilegesFetcher::SentryPrivilegeIsWellFormed(
        real_privilege, requested_authorizable, &granted_scope, &granted_action));
    ASSERT_EQ(SentryAuthorizableScope::TABLE, granted_scope);
    ASSERT_EQ(SentryAction::ALL, granted_action);
  }
}

class SentryAuthzProviderTest : public SentryTestBase {
 public:
  static const char* const kTestUser;
  static const char* const kTrustedUser;
  static const char* const kUserGroup;
  static const char* const kRoleName;

  void SetUp() override {
    SentryTestBase::SetUp();

    metric_entity_ = METRIC_ENTITY_server.Instantiate(
        &metric_registry_, "sentry_auth_provider-test");

    // Configure the SentryAuthzProvider flags.
    FLAGS_sentry_privileges_cache_capacity_mb = CachingEnabled() ? 1 : 0;
    FLAGS_sentry_service_rpc_addresses = sentry_->address().ToString();
    FLAGS_trusted_user_acl = kTrustedUser;
    sentry_authz_provider_.reset(new SentryAuthzProvider(metric_entity_));
    ASSERT_OK(sentry_authz_provider_->Start());
  }

  bool KerberosEnabled() const override {
    // The returned value corresponds to the actual setting of the
    // --sentry_service_security_mode flag; now it's "kerberos" by default.
    return true;
  }

  virtual bool CachingEnabled() const {
    return true;
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

  Status DropRole() {
    RETURN_NOT_OK(kudu::master::DropRole(sentry_client_.get(), kRoleName));
    return sentry_authz_provider_->fetcher_.ResetCache();
  }

  Status CreateRoleAndAddToGroups() {
    RETURN_NOT_OK(kudu::master::CreateRoleAndAddToGroups(
        sentry_client_.get(), kRoleName, kUserGroup));
    return sentry_authz_provider_->fetcher_.ResetCache();
  }

  Status AlterRoleGrantPrivilege(const TSentryPrivilege& privilege) {
    RETURN_NOT_OK(kudu::master::AlterRoleGrantPrivilege(
        sentry_client_.get(), kRoleName, privilege));
    return sentry_authz_provider_->fetcher_.ResetCache();
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

#define GET_GAUGE_READINGS(func_name, counter_name_suffix) \
  int64_t func_name() { \
    scoped_refptr<Counter> gauge(metric_entity_->FindOrCreateCounter( \
        &METRIC_sentry_privileges_##counter_name_suffix)); \
    CHECK(gauge); \
    return gauge->value(); \
  }
  GET_GAUGE_READINGS(GetCacheEvictions, cache_evictions)
  GET_GAUGE_READINGS(GetCacheEvictionsExpired, cache_evictions_expired)
  GET_GAUGE_READINGS(GetCacheHitsExpired, cache_hits_expired)
  GET_GAUGE_READINGS(GetCacheHits, cache_hits)
  GET_GAUGE_READINGS(GetCacheInserts, cache_inserts)
  GET_GAUGE_READINGS(GetCacheLookups, cache_lookups)
  GET_GAUGE_READINGS(GetCacheMisses, cache_misses)
#undef GET_GAUGE_READINGS

  int64_t GetCacheUsage() {
    scoped_refptr<AtomicGauge<uint64>> gauge(metric_entity_->FindOrCreateGauge(
        &METRIC_sentry_privileges_cache_memory_usage, static_cast<uint64>(0)));
    CHECK(gauge);
    return gauge->value();
  }

 protected:
  MetricRegistry metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
  unique_ptr<SentryAuthzProvider> sentry_authz_provider_;
};

const char* const SentryAuthzProviderTest::kTestUser = "test-user";
const char* const SentryAuthzProviderTest::kTrustedUser = "trusted-user";
const char* const SentryAuthzProviderTest::kUserGroup = "user";
const char* const SentryAuthzProviderTest::kRoleName = "developer";

namespace {

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

} // anonymous namespace

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

constexpr const char* kDb = "db";
constexpr const char* kTable = "table";
constexpr const char* kColumn = "column";

} // anonymous namespace

// Benchmark to test the time it takes to evaluate privileges when requesting
// privileges for braod authorization scopes (e.g. SERVER, DATABASE).
TEST_F(SentryAuthzProviderTest, BroadAuthzScopeBenchmark) {
  const char* kLongDb = "DbWithLongName";
  const char* kLongTable = "TableWithLongName";
  ASSERT_OK(CreateRoleAndAddToGroups());

  // Create a database with a bunch tables in it.
  int kNumTables = FLAGS_num_table_privileges;
  for (int i = 0; i < kNumTables; i++) {
    KLOG_EVERY_N_SECS(INFO, 3) << Substitute("num tables granted: $0", i);
    ASSERT_OK(AlterRoleGrantPrivilege(
        GetTablePrivilege(kLongDb, Substitute("$0_$1", kLongTable, i), "OWNER")));
  }

  // Time how long it takes to get the database privileges via authorizing a
  // create table request.
  Status s;
  LOG_TIMING(INFO, "Getting database privileges") {
    s = sentry_authz_provider_->AuthorizeCreateTable(
        Substitute("$0.$1_$2", kLongDb, kLongTable, 0) , kTestUser, kTestUser);
  }
  ASSERT_TRUE(s.IsNotAuthorized());
}

// Benchmark to test the time it takes to evaluate privileges when listing
// tables.
TEST_F(SentryAuthzProviderTest, ListTablesBenchmark) {
  ASSERT_OK(CreateRoleAndAddToGroups());
  unordered_set<string> tables;
  for (int d = 0; d < FLAGS_num_databases; d++) {
    const string db_name = Substitute("$0_$1", kDb, d);
    // Regardless of whether the user has database-level privileges on the
    // database, make sure there's at least one privilege for a database to
    // keep the benchmark consistent when toggling this flag.
    const string dummy_name = Substitute("$0_$1", "foo", d);
    ASSERT_OK(AlterRoleGrantPrivilege(
        GetDatabasePrivilege(FLAGS_has_db_privileges ? db_name : dummy_name, "METADATA")));
    for (int t = 0; t < FLAGS_num_tables_per_db; t++) {
      const string table_name = Substitute("$0_$1", kTable, t);
      const string table_ident = Substitute("$0.$1", db_name, table_name);

      KLOG_EVERY_N_SECS(INFO, 3) << "Granted privilege on table: " << table_ident;
      ASSERT_OK(AlterRoleGrantPrivilege(GetTablePrivilege(db_name, table_name, "METADATA")));
      EmplaceOrDie(&tables, table_ident);
    }
  }
  bool checked_table_names = false;
  LOG_TIMING(INFO, "Listing tables") {
    ASSERT_OK(sentry_authz_provider_->AuthorizeListTables(
        kTestUser, &tables, &checked_table_names));
  }
  ASSERT_TRUE(checked_table_names);
  ASSERT_EQ(FLAGS_num_databases * FLAGS_num_tables_per_db, tables.size());
}

TEST_F(SentryAuthzProviderTest, TestListTables) {
  ASSERT_OK(CreateRoleAndAddToGroups());
  const int kNumDbs = 2;
  const int kNumTablesPerDb = 5;
  const int kNumNonHiveTables = 3;
  unordered_set<string> tables;
  for (int d = 0; d < kNumDbs; d++) {
    const string db_name = Substitute("$0_$1", kDb, d);
    for (int t = 0; t < kNumTablesPerDb; t++) {
      const string table_name = Substitute("$0_$1", kTable, t);
      // To test the absence of privileges, only grant privileges on one table
      // per database.
      if (t == 0) {
        ASSERT_OK(AlterRoleGrantPrivilege(GetTablePrivilege(db_name, table_name, "METADATA")));
      }
      EmplaceOrDie(&tables, Substitute("$0.$1", db_name, table_name));
    }
  }
  // Add some tables that don't conform to Hive's naming convention.
  for (int i = 0; i < kNumNonHiveTables; i++) {
    EmplaceOrDie(&tables, Substitute("badname_$0!", i));
  }
  bool checked_table_names = false;
  // List tables as a trusted user. All tables, including non-Hive-conformant
  // ones, should be visible.
  ASSERT_OK(sentry_authz_provider_->AuthorizeListTables(
      kTrustedUser, &tables, &checked_table_names));
  ASSERT_FALSE(checked_table_names);
  ASSERT_EQ(kNumDbs * kNumTablesPerDb + kNumNonHiveTables, tables.size());

  // Now try as a regular user. Only the tables with Hive-conformant names that
  // the user has privileges on should be visible.
  ASSERT_OK(sentry_authz_provider_->AuthorizeListTables(kTestUser, &tables, &checked_table_names));
  ASSERT_TRUE(checked_table_names);
  ASSERT_EQ(kNumDbs, tables.size());

  // When requires database level privileges for list tables, user shouldn't see
  // any tables with only table level privileges.
  FLAGS_sentry_require_db_privileges_for_list_tables = true;
  ASSERT_OK(sentry_authz_provider_->AuthorizeListTables(kTestUser, &tables, &checked_table_names));
  ASSERT_TRUE(checked_table_names);
  ASSERT_EQ(0, tables.size());
}

class SentryAuthzProviderFilterPrivilegesTest : public SentryAuthzProviderTest {
 public:
  SentryAuthzProviderFilterPrivilegesTest()
      : prng_(SeedRandom()) {
  }

  void SetUp() override {
    SentryAuthzProviderTest::SetUp();
    ASSERT_OK(CreateRoleAndAddToGroups());
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
        SentryPrivilegesFetcher::ExpectedNonEmptyFields(scope.scope());
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

TEST_F(SentryAuthzProviderFilterPrivilegesTest, TestTablePrivilegePBParsing) {
  constexpr int kNumColumns = 10;
  SchemaBuilder schema_builder;
  schema_builder.AddKeyColumn("col0", DataType::INT32);
  vector<string> column_names = { "col0" };
  for (int i = 1; i < kNumColumns; i++) {
    const string col = Substitute("col$0", i);
    schema_builder.AddColumn(ColumnSchema(col, DataType::INT32),
                             /*is_key=*/false);
    column_names.emplace_back(col);
  }
  SchemaPB schema_pb;
  ASSERT_OK(SchemaToPB(schema_builder.Build(), &schema_pb));
  unordered_map<string, ColumnId> col_name_to_id;
  for (const auto& col_pb : schema_pb.columns()) {
    EmplaceOrDie(&col_name_to_id, col_pb.name(), ColumnId(col_pb.id()));
  }

  // First, grant some privileges at the table authorizable scope or higher.
  Random prng(SeedRandom());
  vector<SentryAuthorizableScope::Scope> scope_to_grant_that_implies_table =
      SelectRandomSubset<vector<SentryAuthorizableScope::Scope>,
          SentryAuthorizableScope::Scope, Random>({ SentryAuthorizableScope::SERVER,
                                                    SentryAuthorizableScope::DATABASE,
                                                    SentryAuthorizableScope::TABLE }, 0, &prng);
  unordered_map<SentryAuthorizableScope::Scope, SentryActionsSet, std::hash<int>>
      granted_privileges;
  SentryActionsSet table_privileges;
  TSentryAuthorizable table_authorizable;
  table_authorizable.__set_server(FLAGS_server_name);
  table_authorizable.__set_db(kDb);
  table_authorizable.__set_table(kTable);
  table_authorizable.__set_column(column_names[0]);
  for (const auto& granted_scope : scope_to_grant_that_implies_table) {
    for (const auto& action : SelectRandomSubset<SentryActionsSet, SentryAction::Action, Random>(
        kAllActions, 0, &prng)) {
      // Grant the privilege to the user.
      TSentryPrivilege table_privilege = CreatePrivilege(table_authorizable,
          SentryAuthorizableScope(granted_scope), SentryAction(action));
      ASSERT_OK(AlterRoleGrantPrivilege(table_privilege));

      // All of the privileges imply the table-level action.
      InsertIfNotPresent(&table_privileges, action);
    }
  }

  // Grant some privileges at the column scope.
  vector<string> columns_to_grant =
      SelectRandomSubset<vector<string>, string, Random>(column_names, 0, &prng);
  unordered_set<ColumnId> scannable_columns;
  for (const auto& column_name : columns_to_grant) {
    for (const auto& action : SelectRandomSubset<SentryActionsSet, SentryAction::Action, Random>(
        kAllActions, 0, &prng)) {
      // Grant the privilege to the user.
      TSentryPrivilege column_privilege =
          GetColumnPrivilege(kDb, kTable, column_name, ActionToString(action));
      ASSERT_OK(AlterRoleGrantPrivilege(column_privilege));

      if (SentryAction(action).Implies(SentryAction(SentryAction::SELECT))) {
        InsertIfNotPresent(&scannable_columns, FindOrDie(col_name_to_id, column_name));
      }
    }
  }

  // Make sure that any implied privileges make their way to the token.
  const string kTableId = "table-id";
  TablePrivilegePB expected_pb;
  expected_pb.set_table_id(kTableId);
  for (const auto& granted_table_action : table_privileges) {
    if (SentryAction(granted_table_action).Implies(SentryAction(SentryAction::INSERT))) {
      expected_pb.set_insert_privilege(true);
    }
    if (SentryAction(granted_table_action).Implies(SentryAction(SentryAction::UPDATE))) {
      expected_pb.set_update_privilege(true);
    }
    if (SentryAction(granted_table_action).Implies(SentryAction(SentryAction::DELETE))) {
      expected_pb.set_delete_privilege(true);
    }
    if (SentryAction(granted_table_action).Implies(SentryAction(SentryAction::SELECT))) {
      expected_pb.set_scan_privilege(true);
    }
  }

  // If any of the table-level privileges imply privileges on scan, we
  // shouldn't expect per-column scan privileges. Otherwise, we should expect
  // the columns privileges that implied SELECT to have scan privileges.
  if (!expected_pb.scan_privilege()) {
    ColumnPrivilegePB scan_col_privilege;
    scan_col_privilege.set_scan_privilege(true);
    for (const auto& id : scannable_columns) {
      InsertIfNotPresent(expected_pb.mutable_column_privileges(), id, scan_col_privilege);
    }
  }
  // Validate the privileges went through.
  TablePrivilegePB privilege_pb;
  privilege_pb.set_table_id(kTableId);
  ASSERT_OK(sentry_authz_provider_->FillTablePrivilegePB(Substitute("$0.$1", kDb, kTable),
                                                         kTestUser, schema_pb, &privilege_pb));
  ASSERT_TRUE(MessageDifferencer::Equals(expected_pb, privilege_pb))
      << Substitute("$0 vs $1", SecureDebugString(expected_pb), SecureDebugString(privilege_pb));
}

// Parameterized on the scope at which the privilege will be granted.
class SentryAuthzProviderFilterPrivilegesScopeTest :
    public SentryAuthzProviderFilterPrivilegesTest,
    public ::testing::WithParamInterface<SentryAuthorizableScope::Scope> {};

// Attempt to grant privileges for various actions on a single scope of an
// authorizable, injecting various invalid privileges, and checking that Kudu
// ignores them.
TEST_P(SentryAuthzProviderFilterPrivilegesScopeTest, TestFilterInvalidResponses) {
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
      ASSERT_OK(AlterRoleGrantPrivilege(privilege));
    }
  }
  for (const auto& requested_scope : { SentryAuthorizableScope::SERVER,
                                       SentryAuthorizableScope::DATABASE,
                                       SentryAuthorizableScope::TABLE }) {
    SentryPrivilegesBranch privileges_info;
    ASSERT_OK(sentry_authz_provider_->fetcher_.GetSentryPrivileges(
        requested_scope, table_ident, kTestUser,
        SentryCaching::ALL, &privileges_info));
    // Kudu should ignore all of the invalid privileges.
    ASSERT_TRUE(privileges_info.privileges().empty());
  }
}

// Grants privileges for various actions on a single scope of an authorizable.
TEST_P(SentryAuthzProviderFilterPrivilegesScopeTest, TestFilterValidResponses) {
  const string& table_ident = Substitute("$0.$1", full_authorizable_.db, full_authorizable_.table);
  SentryAuthorizableScope granted_scope(GetParam());
  // Send valid requests and verify that we can get it back through the
  // SentryAuthzProvider.
  for (const auto& action : kAllActions) {
    TSentryPrivilege privilege = CreatePrivilege(full_authorizable_, granted_scope,
                                                 SentryAction(action));
    ASSERT_OK(AlterRoleGrantPrivilege(privilege));
  }
  for (const auto& requested_scope : { SentryAuthorizableScope::SERVER,
                                       SentryAuthorizableScope::DATABASE,
                                       SentryAuthorizableScope::TABLE }) {
    SentryPrivilegesBranch privileges_info;
    ASSERT_OK(sentry_authz_provider_->fetcher_.GetSentryPrivileges(
        requested_scope, table_ident, kTestUser,
        SentryCaching::ALL, &privileges_info));
    ASSERT_EQ(1, privileges_info.privileges().size());
    const auto& authorizable_privileges = *privileges_info.privileges().cbegin();
    ASSERT_EQ(GetParam(), authorizable_privileges.scope)
        << ScopeToString(authorizable_privileges.scope);
    ASSERT_FALSE(authorizable_privileges.allowed_actions.empty());
  }
}

INSTANTIATE_TEST_CASE_P(GrantedScopes, SentryAuthzProviderFilterPrivilegesScopeTest,
                        ::testing::Values(SentryAuthorizableScope::SERVER,
                                          SentryAuthorizableScope::DATABASE,
                                          SentryAuthorizableScope::TABLE,
                                          SentryAuthorizableScope::COLUMN));

// Test to create tables requiring ALL ON DATABASE with the grant option. This
// is parameterized on the ALL scope and OWNER actions, which behave
// identically.
class CreateTableAuthorizationTest :
    public SentryAuthzProviderTest,
    public ::testing::WithParamInterface<string> {
};

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
  ASSERT_OK(CreateRoleAndAddToGroups());
  TSentryPrivilege privilege = GetDatabasePrivilege("db", "DROP");
  ASSERT_OK(AlterRoleGrantPrivilege(privilege));
  s = sentry_authz_provider_->AuthorizeCreateTable("db.table", kTestUser, kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Authorize create table on a user with proper privileges.
  privilege = GetDatabasePrivilege("db", "CREATE");
  ASSERT_OK(AlterRoleGrantPrivilege(privilege));
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
  ASSERT_OK(AlterRoleGrantPrivilege(privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeCreateTable("db.table", kTestUser, "diff-user"));
}

INSTANTIATE_TEST_CASE_P(AllOrOwner, CreateTableAuthorizationTest,
    ::testing::Values("ALL", "OWNER"));

TEST_F(SentryAuthzProviderTest, TestAuthorizeDropTable) {
  // Don't authorize delete table on a user without required privileges.
  ASSERT_OK(CreateRoleAndAddToGroups());
  TSentryPrivilege privilege = GetDatabasePrivilege("db", "SELECT");
  ASSERT_OK(AlterRoleGrantPrivilege(privilege));
  Status s = sentry_authz_provider_->AuthorizeDropTable("db.table", kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Authorize delete table on a user with proper privileges.
  privilege = GetDatabasePrivilege("db", "DROP");
  ASSERT_OK(AlterRoleGrantPrivilege(privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeDropTable("db.table", kTestUser));
}

TEST_F(SentryAuthzProviderTest, TestAuthorizeAlterTable) {
  // Don't authorize alter table on a user without required privileges.
  ASSERT_OK(CreateRoleAndAddToGroups());
  TSentryPrivilege db_privilege = GetDatabasePrivilege("db", "SELECT");
  ASSERT_OK(AlterRoleGrantPrivilege(db_privilege));
  Status s = sentry_authz_provider_->AuthorizeAlterTable("db.table", "db.table", kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Authorize alter table without rename on a user with proper privileges.
  db_privilege = GetDatabasePrivilege("db", "ALTER");
  ASSERT_OK(AlterRoleGrantPrivilege(db_privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeAlterTable("db.table", "db.table", kTestUser));

  // Table alteration with rename requires 'ALL ON TABLE <old-table>' and
  // 'CREATE ON DATABASE <new-database>'
  s = sentry_authz_provider_->AuthorizeAlterTable("db.table", "new_db.new_table", kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Authorize alter table without rename on a user with proper privileges.
  db_privilege = GetDatabasePrivilege("new_db", "CREATE");
  ASSERT_OK(AlterRoleGrantPrivilege(db_privilege));
  TSentryPrivilege table_privilege = GetTablePrivilege("db", "table", "ALL");
  ASSERT_OK(AlterRoleGrantPrivilege(table_privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeAlterTable("db.table",
                                                        "new_db.new_table",
                                                        kTestUser));
}

TEST_F(SentryAuthzProviderTest, TestAuthorizeGetTableMetadata) {
  // Don't authorize getting metadata on a table for a user without required
  // privileges.
  ASSERT_OK(CreateRoleAndAddToGroups());
  Status s = sentry_authz_provider_->AuthorizeGetTableMetadata("db.table", kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // Authorize getting metadata on a table for a user with proper privileges.
  TSentryPrivilege privilege = GetDatabasePrivilege("db", "SELECT");
  ASSERT_OK(AlterRoleGrantPrivilege(privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeGetTableMetadata("db.table", kTestUser));
}

TEST_F(SentryAuthzProviderTest, TestInvalidAction) {
  ASSERT_OK(CreateRoleAndAddToGroups());
  TSentryPrivilege privilege = GetDatabasePrivilege("db", "invalid");
  ASSERT_OK(AlterRoleGrantPrivilege(privilege));
  // User has privileges with invalid action cannot operate on the table.
  Status s = sentry_authz_provider_->AuthorizeCreateTable("DB.table", kTestUser, kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
}

TEST_F(SentryAuthzProviderTest, TestInvalidAuthzScope) {
  ASSERT_OK(CreateRoleAndAddToGroups());
  TSentryPrivilege privilege = GetDatabasePrivilege("db", "ALL");
  privilege.__set_privilegeScope("invalid");
  ASSERT_OK(AlterRoleGrantPrivilege(privilege));
  // User has privileges with invalid authorizable scope cannot operate
  // on the table.
  Status s = sentry_authz_provider_->AuthorizeCreateTable("DB.table", kTestUser, kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
}

// Ensures Sentry privileges are case insensitive.
TEST_F(SentryAuthzProviderTest, TestPrivilegeCaseSensitivity) {
  ASSERT_OK(CreateRoleAndAddToGroups());
  TSentryPrivilege privilege = GetDatabasePrivilege("db", "create");
  ASSERT_OK(AlterRoleGrantPrivilege(privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeCreateTable("DB.table", kTestUser, kTestUser));
}

// Verify the behavior of the SentryAuthzProvider's cache upon fetching
// privilege information on authorizables of the TABLE scope in
// 'adjacent branches' of the authz hierarchy.
TEST_F(SentryAuthzProviderTest, CacheBehaviorScopeHierarchyAdjacentBranches) {
  ASSERT_OK(CreateRoleAndAddToGroups());
  ASSERT_OK(AlterRoleGrantPrivilege(GetDatabasePrivilege("db", "METADATA")));
  ASSERT_OK(AlterRoleGrantPrivilege(GetTablePrivilege("db", "t0", "ALTER")));
  ASSERT_OK(AlterRoleGrantPrivilege(GetTablePrivilege("db", "t1", "ALTER")));

  // ALTER TABLE, if not renaming the table itself, requires ALTER privilege
  // on the table, but nothing is required on the database that contains
  // the table.
  ASSERT_EQ(0, GetTasksSuccessful());
  ASSERT_OK(sentry_authz_provider_->AuthorizeAlterTable(
      "db.t0", "db.t0", kTestUser));
  ASSERT_EQ(1, GetTasksSuccessful());

  // The cache was empty. The query was for TABLE scope privileges, so the
  // cache was examined for both DATABASE and TABLE scope entries, and both
  // were missing. After fetching information on privileges granted to the user
  // on table 'db.t0', the information received from Sentry was split and put
  // into DATABASE and TABLE scope entries.
  ASSERT_EQ(2, GetCacheLookups());
  ASSERT_EQ(2, GetCacheMisses());
  ASSERT_EQ(0, GetCacheHits());
  ASSERT_EQ(2, GetCacheInserts());

  ASSERT_OK(sentry_authz_provider_->AuthorizeAlterTable(
      "db.t1", "db.t1", kTestUser));
  // Information on the user's privileges granted on 'db.t1' was not present
  // in the cache: there was an RPC request sent to Sentry.
  ASSERT_EQ(2, GetTasksSuccessful());

  // One more cache miss: TABLE scope entry for 'db.t1' was absent.
  ASSERT_EQ(3, GetCacheMisses());
  // One more cache hit: DATABASE scope entry was already present.
  ASSERT_EQ(1, GetCacheHits());
  // Updated already existing DATABASE and inserted new TABLE entry.
  ASSERT_EQ(4, GetCacheInserts());

  // METADATA requires corresponding privilege granted on the table, but nothing
  // is required on the database.
  ASSERT_OK(sentry_authz_provider_->AuthorizeGetTableMetadata(
      "db.other_table", kTestUser));
  ASSERT_EQ(3, GetTasksSuccessful());

  // One more cache miss: TABLE scope entry for 'db.other_table' was absent.
  ASSERT_EQ(4, GetCacheMisses());
  // One more cache hit: DATABASE scope entry was already present.
  ASSERT_EQ(2, GetCacheHits());
  // Updated already existing DATABASE and inserted new TABLE entry.
  ASSERT_EQ(6, GetCacheInserts());

  // Repeat all the requests above: not a single new RPC to Sentry should be
  // sent since all authz queries must hit the cache: that's about repeating
  // the same requests.
  ASSERT_OK(sentry_authz_provider_->AuthorizeAlterTable(
      "db.t0", "db.t0", kTestUser));
  ASSERT_EQ(4, GetCacheHits());
  ASSERT_OK(sentry_authz_provider_->AuthorizeAlterTable(
      "db.t1", "db.t1", kTestUser));
  ASSERT_EQ(6, GetCacheHits());
  ASSERT_OK(sentry_authz_provider_->AuthorizeGetTableMetadata(
      "db.other_table", kTestUser));
  ASSERT_EQ(8, GetCacheHits());
  // No new cache misses.
  ASSERT_EQ(4, GetCacheMisses());
  // No additional inserts, of course.
  ASSERT_EQ(6, GetCacheInserts());
  // No additional RPC requests to Sentry.
  ASSERT_EQ(3, GetTasksSuccessful());

  // All the requests below should also hit the cache since the information on
  // the privileges granted on each of the tables in the requests below
  // is in the cache. In the Sentry's privileges model for Kudu, DROP TABLE
  // requires privileges on the table itself, but nothing is required on the
  // database the table belongs to.
  Status s = sentry_authz_provider_->AuthorizeDropTable("db.t0", kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  ASSERT_EQ(10, GetCacheHits());
  s = sentry_authz_provider_->AuthorizeDropTable("db.t1", kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  ASSERT_EQ(12, GetCacheHits());
  s = sentry_authz_provider_->AuthorizeDropTable("db.other_table", kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  ASSERT_EQ(14, GetCacheHits());
  // No new cache misses.
  ASSERT_EQ(4, GetCacheMisses());
  // No additional inserts, of course.
  ASSERT_EQ(6, GetCacheInserts());
  // No additional RPC requests to Sentry.
  ASSERT_EQ(3, GetTasksSuccessful());

  // A sanity check: verify no failed requests are registered.
  ASSERT_EQ(0, GetTasksFailedFatal());
  ASSERT_EQ(0, GetTasksFailedNonFatal());
}

// Ensure requests to authorize CreateTables and AlterTables hit cache once
// the information was fetched from Sentry for an authorizable of the TABLE
// scope in the same hierarchy branch. A bit of context: Sentry sends all
// available information for the branch up the authz scope hierarchy.
TEST_F(SentryAuthzProviderTest, CacheBehaviorForCreateAndAlter) {
  ASSERT_OK(CreateRoleAndAddToGroups());
  ASSERT_OK(AlterRoleGrantPrivilege(GetDatabasePrivilege("db0", "ALTER")));
  ASSERT_OK(AlterRoleGrantPrivilege(GetDatabasePrivilege("db1", "CREATE")));
  ASSERT_EQ(0, GetTasksSuccessful());

  // ALTER TABLE, if not renaming the table itself, requires privileges on the
  // table only.
  ASSERT_OK(sentry_authz_provider_->AuthorizeAlterTable(
      "db0.t0", "db0.t0", kTestUser));
  ASSERT_EQ(1, GetTasksSuccessful());

  // The cache was empty. The query was for TABLE scope privileges, so the
  // cache was examined for both DATABASE and TABLE scope entries, and both
  // were missing. After fetching information on privileges granted to the user
  // on table 'db.t0', the information received from Sentry was split and put
  // into DATABASE and TABLE scope entries.
  ASSERT_EQ(2, GetCacheLookups());
  ASSERT_EQ(2, GetCacheMisses());
  ASSERT_EQ(0, GetCacheHits());
  ASSERT_EQ(2, GetCacheInserts());

  // The CREATE privileges is not granted on the 'db0', so the request must
  // not be authorized.
  auto s = sentry_authz_provider_->AuthorizeCreateTable(
      "db0.t1", kTestUser, kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized());
  // CREATE TABLE requires privileges on the database only, and those should
  // have been cached already due to the prior request.
  ASSERT_EQ(1, GetTasksSuccessful());

  // One more cache lookup of the corresponding DATABASE scope key.
  ASSERT_EQ(3, GetCacheLookups());
  // No new cache misses.
  ASSERT_EQ(2, GetCacheMisses());
  // Single cache lookup turned to be a cache hit.
  ASSERT_EQ(1, GetCacheHits());

  // No new RPCs to Sentry should be issued: the information on privileges
  // on 'db1' authorizable of the DATABASE scope should be fetched and cached
  // while fetching the information privileges on 'db1.t0' authorizable of the
  // TABLE scope.
  for (int idx = 0; idx < 10; ++idx) {
    const auto table_name = Substitute("db1.t$0", idx);
    ASSERT_OK(sentry_authz_provider_->AuthorizeCreateTable(
        table_name, kTestUser, kTestUser));
  }
  // Only a single new RPC should be issued to Sentry: to get information
  // for "db1" authorizable of the DATABASE scope while authorizing the creation
  // of table "db1.t0". All other requests must hit the cache.
  ASSERT_EQ(2, GetTasksSuccessful());

  // Ten more cache lookups of the corresponding DATABASE scope key: one turned
  // to be a miss and other nine hits after the information was fetched
  // from Sentry and inserted into the cache.
  ASSERT_EQ(13, GetCacheLookups());
  ASSERT_EQ(3, GetCacheMisses());
  ASSERT_EQ(10, GetCacheHits());
  // One more insert: adding an entry for the DATABASE scope key for 'db1'.
  ASSERT_EQ(3, GetCacheInserts());

  // Same story for requests for 'db1.t0', ..., 'db1.t19'.
  for (int idx = 0; idx < 20; ++idx) {
    const auto table_name = Substitute("db1.t$0", idx);
    ASSERT_OK(sentry_authz_provider_->AuthorizeCreateTable(
        table_name, kTestUser, kTestUser));
  }
  ASSERT_EQ(2, GetTasksSuccessful());

  // All twenty lookups hit the cache, no new misses.
  ASSERT_EQ(33, GetCacheLookups());
  ASSERT_EQ(30, GetCacheHits());
  ASSERT_EQ(3, GetCacheMisses());

  // A sanity check: verify no failed requests are registered.
  ASSERT_EQ(0, GetTasksFailedFatal());
  ASSERT_EQ(0, GetTasksFailedNonFatal());
}

// A scenario where a TABLE-scope privilege on a table is granted to a user,
// but there isn't any DATABASE-scope privilege granted on the database
// the table belongs to. After authorizing an operation on the table, there
// should not be another RPC to Sentry issued while authorizing an operation
// on the database itself.
TEST_F(SentryAuthzProviderTest, CacheBehaviorHybridLookups) {
  ASSERT_OK(CreateRoleAndAddToGroups());
  ASSERT_OK(AlterRoleGrantPrivilege(GetTablePrivilege("db", "t", "ALL")));

  ASSERT_EQ(0, GetTasksSuccessful());
  // In the Sentry's authz model for Kudu, DROP TABLE requires only privileges
  // on the table itself.
  ASSERT_OK(sentry_authz_provider_->AuthorizeDropTable("db.t", kTestUser));
  ASSERT_EQ(1, GetTasksSuccessful());

  // The cache was empty. The query was for TABLE scope privileges, so the
  // cache was examined for both DATABASE and TABLE scope entries, and both
  // were missing. After fetching information on privileges granted to the user
  // on table 'db.t', the information received from Sentry was split and put
  // into DATABASE and TABLE scope entries.
  ASSERT_EQ(0, GetCacheHits());
  ASSERT_EQ(2, GetCacheLookups());
  ASSERT_EQ(2, GetCacheMisses());
  ASSERT_EQ(2, GetCacheInserts());

  // CREATE TABLE requires privileges only on the database itself. No privileges
  // are granted on the database, so the request must not be authorized.
  auto s = sentry_authz_provider_->AuthorizeCreateTable(
      "db.t", kTestUser, kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  // No extra RPC should be sent to Sentry: the information on the privileges
  // granted on relevant authorizables of the DATABASE scope in corresponding
  // branch should have been fetched and cached.
  ASSERT_EQ(1, GetTasksSuccessful());
  // One more lookup in the cache that turned to a cache hit: it was necessary
  // to lookup only DATABASE scope entry in the cache.
  ASSERT_EQ(3, GetCacheLookups());
  ASSERT_EQ(1, GetCacheHits());

  // ALTER TABLE, if renaming the table, requires privileges both on the
  // database and the table. Even if ALL is granted on the table itself, there
  // isn't any privilege granted on the database, so the request to rename
  // the table must not be authorized.
  s = sentry_authz_provider_->AuthorizeAlterTable(
      "db.t", "db.t_renamed", kTestUser);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  // No extra RPCs are expected in this case.
  ASSERT_EQ(1, GetTasksSuccessful());
  // Three more lookups; three more cache hits: DATABASE and TABLE lookups
  // are 'db.t'-related, and DATABASE lookup is 'db.t_renamed' related.
  ASSERT_EQ(6, GetCacheLookups());
  ASSERT_EQ(4, GetCacheHits());

  // A sanity check: verify no failed requests are registered.
  ASSERT_EQ(0, GetTasksFailedFatal());
  ASSERT_EQ(0, GetTasksFailedNonFatal());
}

// Verify that information on TABLE-scope privileges are fetched from Sentry,
// but not cached when SentryPrivilegeFetcher receives a ListPrivilegesByUser
// response for a DATABASE-scope authorizable for CreateTables or AlterTables.
TEST_F(SentryAuthzProviderTest, CacheBehaviorNotCachingTableInfo) {
  ASSERT_OK(CreateRoleAndAddToGroups());
  ASSERT_OK(AlterRoleGrantPrivilege(GetDatabasePrivilege("db", "CREATE")));
  ASSERT_OK(AlterRoleGrantPrivilege(GetTablePrivilege("db", "t0", "ALL")));
  ASSERT_OK(AlterRoleGrantPrivilege(GetTablePrivilege("db", "t1", "ALTER")));

  ASSERT_EQ(0, GetTasksSuccessful());
  // In the Sentry's authz model for Kudu, CREATE TABLE requires only privileges
  // on the database.
  ASSERT_OK(sentry_authz_provider_->AuthorizeCreateTable(
      "db.table", kTestUser, kTestUser));
  ASSERT_EQ(1, GetTasksSuccessful());
  ASSERT_EQ(1, GetCacheInserts());
  ASSERT_EQ(1, GetCacheLookups());
  ASSERT_EQ(1, GetCacheMisses());

  // Examine the entry that has just been cached: it should not contain
  // any information on authorizables of the TABLE scope under the 'db':
  // the cache chops off everything of the TABLE and narrower scope from
  // Sentry response before adding corresponding entry into the cache.
  auto* cache = sentry_authz_provider_->fetcher_.cache_.get();
  ASSERT_NE(nullptr, cache);
  {
    auto handle = cache->Get(
        Substitute("$0/$1/$2", kTestUser, FLAGS_server_name, "db"));
    ASSERT_TRUE(handle);
    ASSERT_EQ(2, GetCacheLookups());
    ASSERT_EQ(1, GetCacheHits());

    const auto& value = handle.value();
    for (const auto& privilege : value.privileges()) {
      ASSERT_NE(SentryAuthorizableScope::TABLE, privilege.scope);
      ASSERT_NE(SentryAuthorizableScope::COLUMN, privilege.scope);
    }
  }

  // ALTER TABLE, if not renaming the table, requires privileges only on the
  // table itself.
  ASSERT_OK(sentry_authz_provider_->AuthorizeAlterTable(
      "db.t0", "db.t0", kTestUser));
  // Here an RPC request should be sent to Sentry to fetch information on
  // privileges granted at the table 'db.t0'. That information was fetched
  // from Sentry upon prior call to AuthorizeCreateTable(), but it was not
  // cached deliberately: that way the cache avoids storing information on
  // non-Kudu tables, if any, under an authorizable of the DATABASE scope.
  ASSERT_EQ(2, GetTasksSuccessful());
  ASSERT_EQ(2, GetCacheMisses());
  // Two more lookups: one DATABASE scope and another TABLE scope lookup.
  ASSERT_EQ(4, GetCacheLookups());
  // Inserted DATABASE and TABLE entries.
  ASSERT_EQ(3, GetCacheInserts());

  // The same as above stays valid for the 'db.t1' table.
  ASSERT_OK(sentry_authz_provider_->AuthorizeAlterTable(
      "db.t1", "db.t1", kTestUser));
  ASSERT_EQ(3, GetTasksSuccessful());
  ASSERT_EQ(3, GetCacheMisses());
  // Two more lookups: one DATABASE scope and another TABLE scope lookup.
  ASSERT_EQ(6, GetCacheLookups());
  // Updated already existing DATABASE and inserted new TABLE entry.
  ASSERT_EQ(5, GetCacheInserts());

  // A sanity check: verify no failed requests are registered.
  ASSERT_EQ(0, GetTasksFailedFatal());
  ASSERT_EQ(0, GetTasksFailedNonFatal());
}

// Whether the authz information received from Sentry is cached or not.
enum class AuthzCaching {
  Disabled,
  Enabled,
};

// Tests to ensure SentryAuthzProvider enforces access restrictions as expected.
// Parameterized by whether caching is enabled.
class SentryAuthzProviderReconnectionTest :
    public SentryAuthzProviderTest,
    public ::testing::WithParamInterface<AuthzCaching> {
 public:
  bool CachingEnabled() const override {
    return GetParam() == AuthzCaching::Enabled;
  }
};
INSTANTIATE_TEST_CASE_P(
    , SentryAuthzProviderReconnectionTest,
    ::testing::Values(AuthzCaching::Disabled, AuthzCaching::Enabled));

// Checks that the SentryAuthzProvider handles reconnecting to Sentry
// after a connection failure, or service being too busy.
TEST_P(SentryAuthzProviderReconnectionTest, ConnectionFailureOrTooBusy) {
  // Restart SentryAuthzProvider with configured timeout to reduce the run time
  // of this test.
  NO_FATALS(sentry_authz_provider_->Stop());
  FLAGS_sentry_service_rpc_addresses = sentry_->address().ToString();
  FLAGS_sentry_service_send_timeout_seconds = AllowSlowTests() ? 5 : 2;
  FLAGS_sentry_service_recv_timeout_seconds = AllowSlowTests() ? 5 : 2;
  sentry_authz_provider_.reset(new SentryAuthzProvider);
  ASSERT_OK(sentry_authz_provider_->Start());

  ASSERT_OK(CreateRoleAndAddToGroups());
  TSentryPrivilege privilege = GetDatabasePrivilege("db", "METADATA");
  ASSERT_OK(AlterRoleGrantPrivilege(privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeGetTableMetadata("db.table", kTestUser));

  // Shutdown Sentry and try a few operations.
  ASSERT_OK(StopSentry());

  Status s = sentry_authz_provider_->AuthorizeDropTable("db.table", kTestUser);
  if (CachingEnabled()) {
    EXPECT_TRUE(s.IsNotAuthorized()) << s.ToString();
  } else {
    EXPECT_TRUE(s.IsNetworkError()) << s.ToString();
  }

  s = sentry_authz_provider_->AuthorizeCreateTable("db.table", kTestUser, "diff-user");
  if (CachingEnabled()) {
    EXPECT_TRUE(s.IsNotAuthorized()) << s.ToString();
  } else {
    EXPECT_TRUE(s.IsNetworkError()) << s.ToString();
  }

  // Start Sentry back up and ensure that the same operations succeed.
  ASSERT_OK(StartSentry());
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(sentry_authz_provider_->AuthorizeGetTableMetadata(
        "db.table", kTestUser));
  });

  privilege = GetDatabasePrivilege("db", "DROP");
  ASSERT_OK(AlterRoleGrantPrivilege(privilege));
  ASSERT_OK(sentry_authz_provider_->AuthorizeDropTable("db.table", kTestUser));

  // Pause Sentry and try a few operations.
  ASSERT_OK(sentry_->Pause());

  s = sentry_authz_provider_->AuthorizeDropTable("db.table", kTestUser);
  if (CachingEnabled()) {
    EXPECT_TRUE(s.ok()) << s.ToString();
  } else {
    EXPECT_TRUE(s.IsTimedOut()) << s.ToString();
  }

  s = sentry_authz_provider_->AuthorizeGetTableMetadata("db.table", kTestUser);
  if (CachingEnabled()) {
    EXPECT_TRUE(s.ok()) << s.ToString();
  } else {
    EXPECT_TRUE(s.IsTimedOut()) << s.ToString();
  }

  // Resume Sentry and ensure that the same operations succeed.
  ASSERT_OK(sentry_->Resume());
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(sentry_authz_provider_->AuthorizeDropTable(
        "db.table", kTestUser));
  });
}

// Test to ensure the authorization hierarchy rule of SentryAuthzProvider
// works as expected.
class TestAuthzHierarchy :
    public SentryAuthzProviderTest,
    public ::testing::WithParamInterface<SentryAuthorizableScope::Scope> {
};

TEST_P(TestAuthzHierarchy, TestAuthorizableScope) {
  const SentryAuthorizableScope::Scope scope = GetParam();
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
    ASSERT_OK(CreateRoleAndAddToGroups());
    ASSERT_OK(AlterRoleGrantPrivilege(privilege));
    ASSERT_OK(sentry_authz_provider_->Authorize(scope, SentryAction::Action::ALL,
                                                Substitute("$0.$1", db, tbl), kTestUser));
    ASSERT_OK(DropRole());
  }

  // Privilege with lower scope on the hierarchy cannot imply privileges
  // with higher scope on the hierarchy.
  for (const auto& privilege : lower_hierarchy_privs) {
    ASSERT_OK(CreateRoleAndAddToGroups());
    ASSERT_OK(AlterRoleGrantPrivilege(privilege));
    Status s = sentry_authz_provider_->Authorize(scope, SentryAction::Action::ALL,
                                                 Substitute("$0.$1", db, tbl), kTestUser);
    ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
    ASSERT_OK(DropRole());
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
  bool CachingEnabled() const override {
    // For simplicity, scenarios of this test doesn't use caching. The scenarios
    // track updates of HaClient metrics upon issuing RPCs to Sentry.
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

  ASSERT_OK(CreateRoleAndAddToGroups());
  const auto privilege = GetDatabasePrivilege("db", "create");
  ASSERT_OK(AlterRoleGrantPrivilege(privilege));

  // Pause Sentry and try to send an RPC, expecting it to time out.
  ASSERT_OK(sentry_->Pause());
  s = sentry_authz_provider_->AuthorizeCreateTable(
      "db.table", kTestUser, kTestUser);
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_OK(sentry_->Resume());

  scoped_refptr<Histogram> hist(metric_entity_->FindOrCreateHistogram(
      &METRIC_sentry_client_task_execution_time_us));
  ASSERT_LT(0, hist->histogram()->MinValue());
  // Change the threshold to 1900000 in case of very unstable system clock
  // and other scheduler anomalies of the OS scheduler.
  ASSERT_LT(1900000, hist->histogram()->MaxValue());
  ASSERT_LE(5, hist->histogram()->TotalCount());
  ASSERT_LT(1900000, hist->histogram()->TotalSum());
}

enum class ThreadsNumPolicy {
  CloseToCPUsNum,
  MoreThanCPUsNum,
};

// Test to ensure concurrent requests to Sentry with the same set of parameters
// are accumulated by SentryAuthzProvider, so in total there is less RPC
// requests sent to Sentry than the total number of concurrent requests to
// the provider (ideally, there should be just single request to Sentry).
class TestConcurrentRequests :
    public SentryAuthzProviderTest,
    public ::testing::WithParamInterface<std::tuple<ThreadsNumPolicy,
                                                    AuthzCaching>> {
 public:
  bool CachingEnabled() const override {
    return std::get<1>(GetParam()) == AuthzCaching::Enabled;
  }
};

// Verify how multiple concurrent requests are handled when Sentry responds
// with success.
TEST_P(TestConcurrentRequests, SuccessResponses) {
  const auto kNumRequestThreads =
      std::get<0>(GetParam()) == ThreadsNumPolicy::CloseToCPUsNum
      ? std::min(base::NumCPUs(), 4) : base::NumCPUs() * 3;

  ASSERT_OK(CreateRoleAndAddToGroups());
  const auto privilege = GetDatabasePrivilege("db", "METADATA");
  ASSERT_OK(AlterRoleGrantPrivilege(privilege));

  Barrier barrier(kNumRequestThreads);

  vector<thread> threads;
  vector<Status> thread_status(kNumRequestThreads);
  for (auto i = 0; i < kNumRequestThreads; ++i) {
    const auto thread_idx = i;
    threads.emplace_back([&, thread_idx] () {
      barrier.Wait();
      thread_status[thread_idx] = sentry_authz_provider_->
          AuthorizeGetTableMetadata("db.table", kTestUser);
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  for (const auto& s : thread_status) {
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  const auto sentry_rpcs_num = GetTasksSuccessful();
  // Ideally all requests should result in a single RPC sent to Sentry, but some
  // scheduling anomalies might occur so even the current threshold of maximum
  // (kNumRequestThreads / 2) of actual RPC requests to Sentry might be reached
  // and the assertion below would be triggered. For example, the OS scheduler
  // might de-schedule the majority of the threads spawned above for a time
  // longer than it takes to complete an RPC to Sentry, and that de-scheduling
  // might happen exactly prior the point when the 'earlier-running' thread
  // added itself into a queue designed to track concurrent requests.
  // Essentially, that's about 'freezing' all incoming requests just before the
  // queueing point, and then awakening them one by one, so no more than one
  // thread is registered in the queue at any time.
  //
  // However, those anomalies are expected to be exceptionally rare. In fact,
  // (kNumRequestThreads / 2) seems to be a good enough threshold even for TSAN
  // builds while running the test scenario with --stress_cpu_threads=16.
  ASSERT_GE(kNumRequestThreads / 2, sentry_rpcs_num);

  // Issue the same request once more. If caching is enabled, there should be
  // no additional RPCs sent to Sentry.
  ASSERT_OK(sentry_authz_provider_->AuthorizeGetTableMetadata(
      "db.table", kTestUser));
  ASSERT_EQ(CachingEnabled() ? sentry_rpcs_num : sentry_rpcs_num + 1,
            GetTasksSuccessful());
}

// Verify how multiple concurrent requests are handled when Sentry responds
// with errors.
TEST_P(TestConcurrentRequests, FailureResponses) {
  const auto kNumRequestThreads =
      std::get<0>(GetParam()) == ThreadsNumPolicy::CloseToCPUsNum
      ? std::min(base::NumCPUs(), 4) : base::NumCPUs() * 3;

  Barrier barrier(kNumRequestThreads);

  vector<thread> threads;
  vector<Status> thread_status(kNumRequestThreads);
  for (auto i = 0; i < kNumRequestThreads; ++i) {
    const auto thread_idx = i;
    threads.emplace_back([&, thread_idx] () {
      barrier.Wait();
      thread_status[thread_idx] = sentry_authz_provider_->
          AuthorizeCreateTable("db.table", "nobody", "nobody");
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  for (const auto& s : thread_status) {
    ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  }
  ASSERT_EQ(0, GetTasksSuccessful());
  ASSERT_EQ(0, GetTasksFailedNonFatal());
  const auto sentry_rpcs_num = GetTasksFailedFatal();
  // See the TestConcurrentRequests.SuccessResponses scenario above for details
  // on setting the threshold for 'sentry_rpcs_num'.
  ASSERT_GE(kNumRequestThreads / 2, sentry_rpcs_num);

  // The cache does not store negative responses/errors, so in both caching and
  // non-caching case there should be one extra RPC sent to Sentry.
  auto s = sentry_authz_provider_->AuthorizeCreateTable(
      "db.table", "nobody", "nobody");
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  ASSERT_EQ(sentry_rpcs_num + 1, GetTasksFailedFatal());
}
INSTANTIATE_TEST_CASE_P(QueueingConcurrentRequests, TestConcurrentRequests,
    ::testing::Combine(::testing::Values(ThreadsNumPolicy::CloseToCPUsNum,
                                         ThreadsNumPolicy::MoreThanCPUsNum),
                       ::testing::Values(AuthzCaching::Disabled,
                                         AuthzCaching::Enabled)));

} // namespace master
} // namespace kudu
