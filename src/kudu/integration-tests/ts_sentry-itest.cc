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

#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/write_op.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/hms/hms_client.h"
#include "kudu/hms/mini_hms.h"
#include "kudu/integration-tests/hms_itest-base.h"
#include "kudu/master/sentry_authz_provider-test-base.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/sentry/mini_sentry.h"
#include "kudu/sentry/sentry_client.h"
#include "kudu/sentry/sentry_policy_service_types.h"
#include "kudu/tablet/transactions/write_transaction.h"
#include "kudu/thrift/client.h"
#include "kudu/tools/data_gen_util.h"
#include "kudu/util/barrier.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::client::sp::shared_ptr;
using kudu::client::KuduClient;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduDelete;
using kudu::client::KuduInsert;
using kudu::client::KuduError;
using kudu::client::KuduUpdate;
using kudu::client::KuduScanner;
using kudu::client::KuduScanBatch;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableAlterer;
using kudu::client::KuduTableCreator;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::master::AlterRoleGrantPrivilege;
using kudu::master::CreateRoleAndAddToGroups;
using kudu::master::GetColumnPrivilege;
using kudu::master::GetDatabasePrivilege;
using kudu::master::GetTablePrivilege;
using kudu::sentry::SentryClient;
using kudu::tablet::WritePrivileges;
using kudu::tablet::WritePrivilegeType;
using kudu::tools::GenerateDataForRow;
using sentry::TSentryGrantOption;
using std::pair;
using std::string;
using std::thread;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {

namespace {

// Encapsulates the set of read and write privileges granted to a user. This is
// used for easier composability of tests.
//
// Note: while full table scan privileges could also be included, leaving this
// out simplifies the below tests, which are aimed at testing functionality of
// privileges granted by authz tokens end-to-end; privilege-checking for
// different actions is tested in more depth elsewhere.
struct RWPrivileges {
  // The set of write privileges a user may be granted for a table.
  WritePrivileges table_write_privileges;

  // The set of column names that the user is authorized to scan.
  unordered_set<string> column_scan_privileges;
};

const WritePrivileges kFullPrivileges = {
  WritePrivilegeType::INSERT,
  WritePrivilegeType::UPDATE,
  WritePrivilegeType::DELETE,
};

// Returns a randomly generated set of read and write privileges, ensuring that
// it contains at least one read and one write privilege.
RWPrivileges GeneratePrivileges(const unordered_set<string>& all_cols, ThreadSafeRandom* prng) {
  WritePrivilegeType write_privilege =
      SelectRandomElement<WritePrivileges, WritePrivilegeType, ThreadSafeRandom>(
      kFullPrivileges, prng);
  vector<string> scan_privileges =
      SelectRandomSubset<unordered_set<string>, string, ThreadSafeRandom>(
      all_cols, /*min_to_return*/1, prng);
  RWPrivileges privileges;
  privileges.table_write_privileges = WritePrivileges({ write_privilege });
  privileges.column_scan_privileges =
      unordered_set<string>(scan_privileges.begin(), scan_privileges.end());
  return privileges;
}

// Returns the complentary set of privileges to 'orig_privileges'. This is
// useful for generating operations that should fail, if a user is granted the
// privileges in 'orig_privileges'.
RWPrivileges ComplementaryPrivileges(const unordered_set<string>& all_cols,
                                     const RWPrivileges& orig_privileges) {
  RWPrivileges privileges;
  for (const auto& wp : kFullPrivileges) {
    if (!ContainsKey(orig_privileges.table_write_privileges, wp)) {
      InsertOrDie(&privileges.table_write_privileges, wp);
    }
  }
  for (const auto& col : all_cols) {
    if (!ContainsKey(orig_privileges.column_scan_privileges, col)) {
      InsertOrDie(&privileges.column_scan_privileges, col);
    }
  }
  return privileges;
}

// Performs a write operation to 'table' that should be allowed based on the
// privileges in 'write_privileges', using 'prng' to determine the operation.
Status PerformWrite(const WritePrivileges& write_privileges,
                    ThreadSafeRandom* prng,
                    KuduTable* table) {
  WritePrivilegeType op_type =
      SelectRandomElement<WritePrivileges, WritePrivilegeType, ThreadSafeRandom>(
      write_privileges, prng);
  shared_ptr<KuduSession> session = table->client()->NewSession();
  const auto unwrap_session_error = [&session] (Status s) {
    if (s.IsIOError()) {
      vector<KuduError*> errors;
      session->GetPendingErrors(&errors, nullptr);
      ElementDeleter deleter(&errors);
      CHECK_EQ(1, errors.size());
      return errors[0]->status();
    }
    return s;
  };
  // Note: we could test UPSERTs, but it complicates the logic, and UPSERTs are
  // tested elsewhere anyway.
  switch (op_type) {
    case WritePrivilegeType::INSERT: {
        unique_ptr<KuduInsert> ins(table->NewInsert());
        GenerateDataForRow(table->schema(), prng->Next32(), prng, ins->mutable_row());
        return unwrap_session_error(session->Apply(ins.release()));
      }
      break;
    case WritePrivilegeType::UPDATE: {
        unique_ptr<KuduUpdate> upd(table->NewUpdate());
        GenerateDataForRow(table->schema(), prng->Next32(), prng, upd->mutable_row());
        return unwrap_session_error(session->Apply(upd.release()));
      }
      break;
    case WritePrivilegeType::DELETE: {
        unique_ptr<KuduDelete> del(table->NewDelete());
        KuduPartialRow* row = del->mutable_row();
        RETURN_NOT_OK(row->SetInt32(0, prng->Next32()));
        return unwrap_session_error(session->Apply(del.release()));
      }
      break;
  }
  return Status::OK();
}

// Performs a scan operation to 'table' that should be allowed if the user is
// granted scan privileges on all columns in 'columns'. If provided, uses
// 'prng' to select a subset of rows to scan; otherwise uses all columns.
Status PerformScan(const unordered_set<string>& columns,
                   ThreadSafeRandom* prng,
                   KuduTable* table) {
  vector<string> cols_to_scan = prng ?
      SelectRandomSubset<unordered_set<string>, string, ThreadSafeRandom>(
          columns, /*min_to_return*/1, prng) :
      vector<string>(columns.begin(), columns.end());
  KuduScanner scanner(table);
  RETURN_NOT_OK(scanner.SetTimeoutMillis(30000));
  RETURN_NOT_OK(scanner.SetProjectedColumnNames(cols_to_scan));
  RETURN_NOT_OK(scanner.Open());
  while (scanner.HasMoreRows()) {
    KuduScanBatch batch;
    RETURN_NOT_OK(scanner.NextBatch(&batch));
  }
  return Status::OK();
}

// Performs an action that should be allowed with the given set of
// privileges.
Status PerformAction(const RWPrivileges& privileges,
                     ThreadSafeRandom* prng, KuduTable* table) {
  bool can_write = !privileges.table_write_privileges.empty();
  bool can_scan = !privileges.column_scan_privileges.empty();
  CHECK(can_write || can_scan);
  // If the user can scan and write, flip a coin for what to do. Otherwise,
  // just perform whichever it can.
  bool should_write = (can_write && can_scan && rand() % 2 == 0) ||
                      (can_write && !can_scan);
  if (should_write) {
    CHECK(can_write);
    RETURN_NOT_OK(PerformWrite(privileges.table_write_privileges, prng, table));
  } else {
    CHECK(can_scan);
    RETURN_NOT_OK(PerformScan(privileges.column_scan_privileges, prng, table));
  }
  return Status::OK();
}

} // anonymous namespace

// These tests will use the HMS and Sentry, and thus, are very slow.
// SKIP_IF_SLOW_NOT_ALLOWED() should be the very first thing called in the body
// of every test based on this test class.
class TSSentryITest : public HmsITestBase {
 public:
  // Note: groups and users therein are statically provided to MiniSentry (see
  // mini_sentry.cc). We expect Sentry to be aware of users "user[0-2]".
  static constexpr int kNumUsers = 3;
  static constexpr const char* kAdminGroup = "admin";

  static constexpr int kNumTables = 3;
  static constexpr int kNumColsPerTable = 3;
  static constexpr const char* kDb = "db";
  static constexpr const char* kTablePrefix = "table";
  static constexpr const char* kAdminRole = "kudu-admin";

  static constexpr int kAuthzTokenTTLSecs = 1;
  static constexpr int kAuthzCacheTTLMultiplier = 3;

  void SetUp() override {
    SKIP_IF_SLOW_NOT_ALLOWED();
    for (int u = 0; u < kNumUsers; u++) {
      users_.emplace_back(Substitute("user$0", u));
    }

    ExternalMiniClusterOptions opts;
    opts.enable_kerberos = true;
    opts.enable_sentry = true;
    opts.hms_mode = HmsMode::ENABLE_METASTORE_INTEGRATION;
    // Set a low token timeout so we can ensure retries are working properly.
    opts.extra_master_flags.emplace_back(Substitute("--authz_token_validity_seconds=$0",
                                                    kAuthzTokenTTLSecs));
    opts.extra_master_flags.emplace_back(Substitute("--sentry_privileges_cache_ttl_factor=$0",
                                                    kAuthzCacheTTLMultiplier));
    // In addition to our users, we will be using the "kudu" user to perform
    // administrative tasks like creating tables.
    opts.extra_master_flags.emplace_back(
        Substitute("--user_acl=kudu,$0", JoinStrings(users_, ",")));
    opts.extra_tserver_flags.emplace_back(
        Substitute("--user_acl=$0", JoinStrings(users_, ",")));
    opts.extra_tserver_flags.emplace_back("--tserver_enforce_access_control=true");
    NO_FATALS(StartClusterWithOpts(std::move(opts)));
    ASSERT_OK(cluster_->kdc()->CreateUserPrincipal("kudu"));
    ASSERT_OK(cluster_->kdc()->Kinit("kudu"));

    // Set up the HMS client so we can set up a database.
    thrift::ClientOptions hms_opts;
    hms_opts.enable_kerberos = true;
    hms_opts.service_principal = "hive";
    hms_client_.reset(new hms::HmsClient(cluster_->hms()->address(), hms_opts));
    ASSERT_OK(hms_client_->Start());

    // Set up the Sentry client so we can set up privileges.
    thrift::ClientOptions sentry_opts;
    sentry_opts.enable_kerberos = true;
    sentry_opts.service_principal = "sentry";
    sentry_client_.reset(new SentryClient(cluster_->sentry()->address(), sentry_opts));
    ASSERT_OK(sentry_client_->Start());
    ASSERT_OK(CreateRoleAndAddToGroups(sentry_client_.get(), kAdminRole, kAdminGroup));
    ASSERT_OK(AlterRoleGrantPrivilege(sentry_client_.get(), kAdminRole,
        GetDatabasePrivilege(kDb, "ALL", TSentryGrantOption::DISABLED)));

    // Create the database in the HMS.
    ASSERT_OK(CreateDatabase(kDb));

    // Create a client as the "kudu" user, who now has admin privileges.
    ASSERT_OK(cluster_->CreateClient(nullptr, &admin_client_));

    // Finally populate a set of column names to use for our tables.
    for (int i = 0; i < kNumColsPerTable; i++) {
      cols_.emplace_back(Substitute("col$0", i));
    }
  }

  // Creates a table named 'table_ident' with 'kNumColsPerTable' columns.
  Status CreateTable(const string& table_ident) {
    KuduSchema schema;
    KuduSchemaBuilder b;
    auto iter = cols_.begin();
    b.AddColumn(*iter++)->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    while (iter < cols_.end()) {
      b.AddColumn(*iter++)->Type(KuduColumnSchema::INT32);
    }
    RETURN_NOT_OK(b.Build(&schema));
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    return table_creator->table_name(table_ident)
        .schema(&schema)
        .set_range_partition_columns({ "col0" })
        .num_replicas(1)
        .Create();
  }

  void TearDown() override {
    SKIP_IF_SLOW_NOT_ALLOWED();
    HmsITestBase::TearDown();
  }

 protected:
  // A Sentry client with which to grant privileges.
  unique_ptr<SentryClient> sentry_client_;

  // Kudu client with which to perform admin operations.
  shared_ptr<KuduClient> admin_client_;

  // A list of users that may try to do things.
  vector<string> users_;

  // A list of columns that each table should have.
  vector<string> cols_;
};

// Tests authorizing read and write operations coming from multiple concurrent
// users for multiple tables.
TEST_F(TSSentryITest, TestReadsAndWrites) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  // First, set up the tables.
  vector<string> tables;
  for (int i = 0; i < kNumTables; i++) {
    string table_name = Substitute("$0$1", kTablePrefix, i);
    ASSERT_OK(CreateTable(Substitute("$0.$1", kDb, table_name)));
    tables.emplace_back(std::move(table_name));
  }

  // Keep track of the privileges that each user has been granted and not been
  // granted per table.
  typedef pair<RWPrivileges, RWPrivileges> GrantedNotGrantedPrivileges;
  typedef unordered_map<string, GrantedNotGrantedPrivileges> TableNameToPrivileges;
  unordered_map<string, TableNameToPrivileges> user_to_privileges;

  // Set up a bunch of clients for each user.
  unordered_map<string, vector<shared_ptr<KuduClient>>> user_to_clients;
  ThreadSafeRandom prng(SeedRandom());
  unordered_set<string> cols(cols_.begin(), cols_.end());
  static constexpr int kNumClientsPerUser = 4;
  for (int i = 0; i < kNumUsers; i++) {
    const string& user = users_[i];
    // Register the user with the KDC, and add a role to the user's group
    // (provided to MiniSentry in mini_sentry.cc).
    ASSERT_OK(cluster_->kdc()->CreateUserPrincipal(user));
    ASSERT_OK(cluster_->kdc()->Kinit(user));
    const string role = Substitute("role$0", i);
    ASSERT_OK(CreateRoleAndAddToGroups(sentry_client_.get(), role, Substitute("group$0", i)));

    // Set up multiple clients for each user.
    vector<shared_ptr<KuduClient>> clients;
    for (int i = 0; i < kNumClientsPerUser; i++) {
      shared_ptr<KuduClient> client;
      ASSERT_OK(cluster_->CreateClient(nullptr, &client));
      clients.emplace_back(std::move(client));
    }
    EmplaceOrDie(&user_to_clients, user, std::move(clients));

    // Generate privileges for each user for every table, and grant the
    // appropriate Sentry privileges.
    TableNameToPrivileges table_to_privileges;
    for (const string& table_name : tables) {
      RWPrivileges granted_privileges = GeneratePrivileges(cols, &prng);
      for (const auto& wp : granted_privileges.table_write_privileges) {
        ASSERT_OK(AlterRoleGrantPrivilege(sentry_client_.get(), role,
                  GetTablePrivilege(kDb, table_name, WritePrivilegeToString(wp))));
      }
      for (const auto& col : granted_privileges.column_scan_privileges) {
        ASSERT_OK(AlterRoleGrantPrivilege(sentry_client_.get(), role,
                  GetColumnPrivilege(kDb, table_name, col, "SELECT")));
      }
      RWPrivileges not_granted_privileges = ComplementaryPrivileges(cols, granted_privileges);
      InsertOrDie(&table_to_privileges, table_name,
          { std::move(granted_privileges), std::move(not_granted_privileges) });
    }
    EmplaceOrDie(&user_to_privileges, user, std::move(table_to_privileges));
  }

  // In parallel, have each user's clients perform a series of operations on a
  // table for some extended period of time (longer than the token timeout). Do
  // this for a few tables for each client.
  static constexpr int kNumOpPeriods = 3;
  static const MonoDelta kPeriodTime = MonoDelta::FromSeconds(kAuthzTokenTTLSecs * 3);
  vector<thread> threads;
  Barrier b(kNumUsers * kNumClientsPerUser);
  SCOPED_CLEANUP({
    for (auto& t : threads) {
      t.join();
    }
  });
  for (const string& user : users_) {
    // Start a thread for every user that performs a bunch of operations.
    const auto* const table_to_privileges = FindOrNull(user_to_privileges, user);
    for (const auto& client_sp : FindOrDie(user_to_clients, user)) {
      KuduClient* client = client_sp.get();
      threads.emplace_back([client, table_to_privileges, &b, &tables, &prng] {
        b.Wait();
        // Perform a bunch of operations, switching back and forth between
        // different tables to ensure a client uses the appropriate privileges.
        for (int i = 0; i < kNumOpPeriods; i++) {
          const auto& table_name =
              SelectRandomElement<vector<string>, string, ThreadSafeRandom>(tables, &prng);
          shared_ptr<KuduTable> table;
          ASSERT_OK(client->OpenTable(Substitute("$0.$1", kDb, table_name), &table));
          const MonoTime end_time = MonoTime::Now() + kPeriodTime;
          while (MonoTime::Now() < end_time) {
            const auto& privileges = FindOrDie(*table_to_privileges, table_name);
            const auto& granted_privileges = privileges.first;
            const auto& non_granted_privileges = privileges.second;
            // Perform a permitted operation. We might not get an OK status if
            // e.g. we're inserting a row that already exists, but the operation
            // should always be permitted.
            Status s = PerformAction(granted_privileges, &prng, table.get());
            ASSERT_FALSE(s.IsNotAuthorized()) << s.ToString();
            ASSERT_STR_NOT_CONTAINS(s.ToString(), "not authorized");

            // Now perform an operation based on the privileges we _don't_ have;
            // this should always yield authorization errors.
            s = PerformAction(non_granted_privileges, &prng, table.get());
            ASSERT_TRUE(s.IsRemoteError()) << s.ToString();
            ASSERT_STR_CONTAINS(s.ToString(), "not authorized");
          }
        }
      });
    }
  }
}

// Test for a couple of scenarios related to alter tables.
TEST_F(TSSentryITest, TestAlters) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  static const string kTableName = "table";
  const string table_ident = Substitute("$0.$1", kDb, kTableName);
  ASSERT_OK(CreateTable(table_ident));

  const string user = "user0";
  ASSERT_OK(cluster_->kdc()->CreateUserPrincipal(user));
  ASSERT_OK(cluster_->kdc()->Kinit(user));
  const string role = "role0";
  ASSERT_OK(CreateRoleAndAddToGroups(sentry_client_.get(), role, "group0"));

  shared_ptr<KuduClient> user_client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &user_client));

  // Note: we only need privileges on the metadata for OpenTable() calls.
  // METADATA isn't a first-class Sentry privilege and won't get carried over
  // on table rename, so we just grant INSERT privileges.
  ASSERT_OK(AlterRoleGrantPrivilege(sentry_client_.get(), role,
            GetTablePrivilege(kDb, kTableName, "INSERT")));

  // First, grant privileges on a new column that doesn't yet exist. Once that
  // column is created, we should be able to scan it immediately.
  const string new_column = Substitute("col$0", kNumColsPerTable);
  ASSERT_OK(AlterRoleGrantPrivilege(sentry_client_.get(), role,
            GetColumnPrivilege(kDb, kTableName, new_column, "SELECT")));
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(table_ident));
    table_alterer->AddColumn(new_column)->Type(KuduColumnSchema::INT32);
    ASSERT_OK(table_alterer->Alter());
  }
  shared_ptr<KuduTable> table;
  ASSERT_OK(user_client->OpenTable(table_ident, &table));
  ASSERT_OK(PerformScan({ new_column }, /*prng=*/nullptr, table.get()));

  // Now create another column and grant the user privileges for that column.
  // Since privileges are cached, even though we've granted privileges, clients
  // will use the cached privilege and not be authorized for a bit.
  const string another_column = Substitute("col$0", kNumColsPerTable + 1);
  ASSERT_OK(AlterRoleGrantPrivilege(sentry_client_.get(), role,
            GetColumnPrivilege(kDb, kTableName, another_column, "SELECT")));
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(table_ident));
    table_alterer->AddColumn(another_column)->Type(KuduColumnSchema::INT32);
    ASSERT_OK(table_alterer->Alter());
  }
  ASSERT_OK(user_client->OpenTable(table_ident, &table));
  Status s = PerformScan({ another_column }, /*prng=*/nullptr, table.get());
  ASSERT_TRUE(s.IsRemoteError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "not authorized");

  // Wait the full duration of the cache TTL, and an additional full token TTL.
  // This ensures that the client's token will expire we will get a new one
  // with the most up-to-date privileges from Sentry.
  SleepFor(MonoDelta::FromSeconds(kAuthzTokenTTLSecs * (1 + kAuthzCacheTTLMultiplier)));
  ASSERT_OK(PerformScan({ another_column }, /*prng=*/nullptr, table.get()));

  // Now rename the table to something else. There shouldn't be any privileges
  // cached for the newly-renamed table, so we should immediately be able to
  // scan it.
  const string new_table_ident = Substitute("$0.$1", kDb, "newtable");
  {
    unique_ptr<KuduTableAlterer> table_alterer(client_->NewTableAlterer(table_ident));
    table_alterer->RenameTo(new_table_ident);
    ASSERT_OK(table_alterer->Alter());
  }
  ASSERT_OK(user_client->OpenTable(new_table_ident, &table));
  ASSERT_OK(PerformScan({ another_column }, nullptr, table.get()));
}

} // namespace kudu
