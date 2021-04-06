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

#include <stdint.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/client.pb.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/tablet/key_value_test_schema.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int32(heartbeat_interval_ms);
DECLARE_string(superuser_acl);
using std::string;
using std::unique_ptr;
using std::vector;

DECLARE_bool(enable_table_write_limit);
DECLARE_bool(tserver_enforce_access_control);
DECLARE_double(table_write_limit_ratio);
DECLARE_int32(log_segment_size_mb);
DECLARE_int32(flush_threshold_mb);
DECLARE_int32(flush_threshold_secs);
DECLARE_int32(tablet_history_max_age_sec);
DECLARE_int64(authz_token_validity_seconds);
DECLARE_int64(table_disk_size_limit);
DECLARE_int64(table_row_count_limit);

namespace kudu {

using cluster::InternalMiniCluster;
using cluster::InternalMiniClusterOptions;
using client::AuthenticationCredentialsPB;
using client::sp::shared_ptr;
using client::KuduClient;
using client::KuduClientBuilder;
using client::KuduDelete;
using client::KuduError;
using client::KuduInsert;
using client::KuduScanner;
using client::KuduSchema;
using client::KuduSession;
using client::KuduTable;
using client::KuduTableAlterer;
using client::KuduTableCreator;
using client::KuduTableStatistics;
using client::KuduUpdate;

namespace {

// Relatively low timeout used so we don't have to wait too long for an
// "invalid token" error.
const int kRpcTimeoutSecs = 3;
const int kOperationTimeoutSecs = kRpcTimeoutSecs * 3;

// Inserts a single row to the given key-value table for the given key.
Status InsertKeyToTable(KuduTable* table, KuduSession* session, int key) {
  RETURN_NOT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  unique_ptr<KuduInsert> insert(table->NewInsert());
  KuduPartialRow* row = insert->mutable_row();
  RETURN_NOT_OK(row->SetInt32(0, key));
  RETURN_NOT_OK(row->SetInt32(1, key));
  return session->Apply(insert.release());
}

// Update table value according to key.
Status UpdateKeyToTable(KuduTable* table, KuduSession* session, int key, int value) {
  RETURN_NOT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  unique_ptr<KuduUpdate> update(table->NewUpdate());
  KuduPartialRow* row = update->mutable_row();
  RETURN_NOT_OK(row->SetInt32("key", key));
  RETURN_NOT_OK(row->SetInt32("val", value));
  return session->Apply(update.release());
}

Status DeleteKeyToTable(KuduTable* table, KuduSession* session, int key) {
  RETURN_NOT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  unique_ptr<KuduDelete> del(table->NewDelete());
  KuduPartialRow* row = del->mutable_row();
  RETURN_NOT_OK(row->SetInt32("key", key));
  return session->Apply(del.release());
}

vector<Status> GetSessionErrors(KuduSession* session) {
  vector<KuduError*> errors;
  session->GetPendingErrors(&errors, nullptr);
  vector<Status> ret(errors.size());
  for (int i = 0; i < errors.size(); i++) {
    ret[i] = errors[i]->status();
  }
  ElementDeleter deleter(&errors);
  return ret;
}

Status ScanWholeTable(KuduTable* table, vector<string>* rows) {
  KuduScanner scanner(table);
  scanner.SetTimeoutMillis(kOperationTimeoutSecs * 1000);
  return ScanToStrings(&scanner, rows);
}

Status SetTableLimit(const string& table_name,
                     const shared_ptr<KuduClient>& client,
                     int64_t disk_size_limit,
                     int64_t row_count_limit) {
  unique_ptr<KuduTableAlterer> alterer(
      client->NewTableAlterer(table_name));
  return alterer->SetTableDiskSizeLimit(disk_size_limit)
                ->SetTableRowCountLimit(row_count_limit)
                ->Alter();
}

} // anonymous namespace

class DisableWriteWhenExceedingQuotaTest : public KuduTest {
 public:
  DisableWriteWhenExceedingQuotaTest()
      : schema_(KuduSchema::FromSchema(CreateKeyValueTestSchema())) {}
  const char* const kTableName = "test-table";
  const char* const kUser = "token-user";
  const char* const kSuperUser = "super-user";
  const int64_t kRowCountLimit = 20;

  void SetUp() override {
    KuduTest::SetUp();
    FLAGS_superuser_acl = kSuperUser;
    FLAGS_tserver_enforce_access_control = true;
    FLAGS_authz_token_validity_seconds = 1;
    FLAGS_enable_table_write_limit = true;
    FLAGS_table_row_count_limit = 2;
    FLAGS_table_disk_size_limit = 1024 * 1024 * 2;
    FLAGS_log_segment_size_mb = 1;
    FLAGS_flush_threshold_mb = 0;
    FLAGS_tablet_history_max_age_sec = 1;
    FLAGS_flush_threshold_secs = 1;

    // Create a table with a basic schema.
    cluster_.reset(new InternalMiniCluster(env_, InternalMiniClusterOptions()));
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(SetupClientAndTable());
  }

  // Sets up the client_ and client_table_ members.
  Status SetupClient(const string& user) {
    RETURN_NOT_OK(CreateClientForUser(user, &client_));
    RETURN_NOT_OK(client_->OpenTable(kTableName, &client_table_));
    table_id_ = client_table_->id();
    return Status::OK();
  }

  // Sets up the client_ and client_table_ members.
  Status SetupClientAndTable() {
    RETURN_NOT_OK(CreateClientForUser(kUser, &client_));
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    RETURN_NOT_OK(table_creator->table_name(kTableName)
                               .schema(&schema_)
                               .num_replicas(1)
                               .set_range_partition_columns({ "key" })
                               .Create());
    RETURN_NOT_OK(client_->OpenTable(kTableName, &client_table_));
    table_id_ = client_table_->id();
    return Status::OK();
  }

  // Creates a client for the given user.
  Status CreateClientForUser(const string& user, shared_ptr<KuduClient>* client) const {
    // Many tests will expect operations to fail, so let's get there quicker by
    // setting a low timeout.
    KuduClientBuilder client_builder;
    client_builder.default_rpc_timeout(MonoDelta::FromSeconds(kRpcTimeoutSecs));
    string authn_creds;
    AuthenticationCredentialsPB authn_pb;
    authn_pb.set_real_user(user);
    CHECK(authn_pb.SerializeToString(&authn_creds));
    client_builder.import_authentication_credentials(std::move(authn_creds));
    return cluster_->CreateClient(&client_builder, client);
  }

  // Wait for the tservers update table statisitcs to master.
  static void WaitForTServerUpdatesStatisticsToMaster(int ms = FLAGS_heartbeat_interval_ms * 10) {
    SleepFor(MonoDelta::FromMilliseconds(ms));
  }

  // Disable write privilege through authz token when exceeding size quota
  void TestSizeLimit() {
    shared_ptr<KuduSession> good_session(client_->NewSession());
    Status s = Status::OK();
    int k = 0;
    for (k = 0; k < kRowCountLimit; k++) {
      ASSERT_OK(InsertKeyToTable(client_table_.get(), good_session.get(), k));
      s = good_session->Flush();
      if (!s.ok()) {
        // break the loop once the write is blocked
        break;
      }
      WaitForTServerUpdatesStatisticsToMaster(1000);
    }
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    if (!s.ok()) {
      vector<Status> errors = GetSessionErrors(good_session.get());
      for (const auto& e : errors) {
        ASSERT_TRUE(e.IsRemoteError()) << e.ToString();
        ASSERT_STR_CONTAINS(e.ToString(), "Not authorized");
      }
    }
    // Scanning still works.
    vector<string> rows;
    ASSERT_OK(ScanWholeTable(client_table_.get(), &rows));
    ASSERT_EQ(rows.size(), k);
    // Update also blocked
    ASSERT_OK(UpdateKeyToTable(client_table_.get(), good_session.get(), 0, 1000));
    s = good_session->Flush();
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    vector<Status> errors = GetSessionErrors(good_session.get());
    for (const auto& e : errors) {
      ASSERT_TRUE(e.IsRemoteError()) << e.ToString();
      ASSERT_STR_CONTAINS(e.ToString(), "Not authorized");
    }
    // Delete operation is taken as UPDATE, which will also increase table size.
    // So, remove all rows in order to reclaim the space
    for (k--; k >= 0; k--) {
      good_session = client_->NewSession();
      ASSERT_OK(DeleteKeyToTable(client_table_.get(), good_session.get(), k));
      ASSERT_OK(good_session->Flush());
      WaitForTServerUpdatesStatisticsToMaster(1000);
    }
    // Suppose the table is empty
    rows.clear();
    ASSERT_OK(ScanWholeTable(client_table_.get(), &rows));
    ASSERT_EQ(rows.size(), 0);
    // Insertion should work again
    ASSERT_OK(InsertKeyToTable(client_table_.get(), good_session.get(), 0));
    ASSERT_OK(good_session->Flush());
    WaitForTServerUpdatesStatisticsToMaster(1000);
    // Remove the just inserted row to clean the table
    good_session = client_->NewSession();
    ASSERT_OK(DeleteKeyToTable(client_table_.get(), good_session.get(), 0));
    ASSERT_OK(good_session->Flush());
    WaitForTServerUpdatesStatisticsToMaster(1000);
  }

  // Disable write privilege through authz token when exceeding rows quota
  void TestRowLimit() {
    // insert 2 rows to fill the quota
    shared_ptr<KuduSession> good_session(client_->NewSession());
    auto key = next_row_key_++;
    ASSERT_OK(InsertKeyToTable(client_table_.get(), good_session.get(), key));
    ASSERT_OK(good_session->Flush());
    WaitForTServerUpdatesStatisticsToMaster();

    ASSERT_OK(InsertKeyToTable(client_table_.get(), good_session.get(), key + 1));
    Status s = good_session->Flush();
    WaitForTServerUpdatesStatisticsToMaster();
    // exceeds row quota and failed to update
    shared_ptr<KuduSession> bad_session(client_->NewSession());
    ASSERT_OK(InsertKeyToTable(client_table_.get(), bad_session.get(), key + 2));
    s = bad_session->Flush();
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    vector<Status> errors = GetSessionErrors(bad_session.get());
    for (const auto& e : errors) {
      ASSERT_TRUE(e.IsRemoteError()) << e.ToString();
      ASSERT_STR_CONTAINS(e.ToString(), "Not authorized");
    }

    ASSERT_OK(UpdateKeyToTable(client_table_.get(), bad_session.get(), key, key + 2));
    s = bad_session->Flush();
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    errors = GetSessionErrors(bad_session.get());
    for (const auto& e : errors) {
      ASSERT_TRUE(e.IsRemoteError()) << e.ToString();
      ASSERT_STR_CONTAINS(e.ToString(), "Not authorized");
    }
    // Scans still works.
    vector<string> rows;
    ASSERT_OK(ScanWholeTable(client_table_.get(), &rows));
    ASSERT_EQ(rows.size(), 2);
    // remove one row to avoid reaching quota
    good_session = client_->NewSession();
    ASSERT_OK(DeleteKeyToTable(client_table_.get(), good_session.get(), key));
    ASSERT_OK(good_session->Flush());
    WaitForTServerUpdatesStatisticsToMaster();
    // Check rows
    rows.clear();
    ASSERT_OK(ScanWholeTable(client_table_.get(), &rows));
    ASSERT_EQ(rows.size(), 1);
    // reinsert should be allowed
    ASSERT_OK(InsertKeyToTable(client_table_.get(), good_session.get(), key + 3));
    ASSERT_OK(good_session->Flush());

    // empty the table by deleting all rows
    ASSERT_OK(DeleteKeyToTable(client_table_.get(), good_session.get(), key + 3));
    ASSERT_OK(good_session->Flush());
    WaitForTServerUpdatesStatisticsToMaster();

    ASSERT_OK(DeleteKeyToTable(client_table_.get(), good_session.get(), key + 1));
    ASSERT_OK(good_session->Flush());
  }

  // change the table limit through admin user
  void ModifyLimit(const int64_t disk_size_limit, const int64_t row_count_limit) {
    ASSERT_OK(SetupClient(kSuperUser));
    ASSERT_OK(SetTableLimit(kTableName, client_, disk_size_limit, row_count_limit));
  }

 protected:
  const KuduSchema schema_;
  unique_ptr<InternalMiniCluster> cluster_;

  // Client authenticated as the default user.
  shared_ptr<KuduClient> client_;

  // Table created with 'client_'.
  shared_ptr<KuduTable> client_table_;
  string table_id_;

  // The next row key to insert.
  int next_row_key_ = 0;
};

// Refuse the table limit change if the alteration contains any other non-table-limit
// related operation, like renaming, changing owner, adding/altering/dropping column, etc.
TEST_F(DisableWriteWhenExceedingQuotaTest, TestDisallowedToChangeLimitMixedWithOtherOps) {
  const int64_t on_disk_size_limit = 100L * 1024 * 1024;
  const int64_t row_count_limit = 10L;
  ASSERT_OK(SetupClient(kSuperUser));
  // Failed to change table disk size limit because it contains another
  // non-table-limit related operation: rename the table
  unique_ptr<KuduTableAlterer> alterer(client_->NewTableAlterer(kTableName));
  Status s = alterer->RenameTo("failedRenaming")
                    ->SetTableDiskSizeLimit(on_disk_size_limit)
                    ->Alter();
  ASSERT_TRUE(s.IsConfigurationError()) << s.ToString();

  // Failed to change the row count limit of the table because it contains
  // another non-table-limit related operation: set owner.
  s = alterer->SetOwner("failedSetOwner")
             ->SetTableRowCountLimit(row_count_limit)
             ->Alter();
  ASSERT_TRUE(s.IsConfigurationError()) << s.ToString();

  // It is successful only changing the table 'limit'.
  ASSERT_OK(SetTableLimit(kTableName, client_, on_disk_size_limit, row_count_limit));

  // restart the cluster to verify it again
  cluster_->Shutdown();
  ASSERT_OK(cluster_->Start());
  unique_ptr<KuduTableStatistics> statistics;
  KuduTableStatistics *table_statistics;
  ASSERT_OK(client_->GetTableStatistics(kTableName, &table_statistics));
  statistics.reset(table_statistics);
  ASSERT_EQ(on_disk_size_limit, statistics->on_disk_size_limit());
  ASSERT_EQ(row_count_limit, statistics->live_row_count_limit());
}

// Refuse the table limit change if the user is not superuser
TEST_F(DisableWriteWhenExceedingQuotaTest, TestOnlySuperUserAllowedToChangeTableLimit) {
  const int64_t on_disk_size_limit = 100L * 1024 * 1024;
  const int64_t row_count_limit = 10L;
  Status s = SetTableLimit(kTableName, client_, on_disk_size_limit, row_count_limit);
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  NO_FATALS(ModifyLimit(on_disk_size_limit, row_count_limit));
  ASSERT_OK(SetupClient(kSuperUser));

  ASSERT_OK(SetTableLimit(kTableName, client_, on_disk_size_limit, row_count_limit));
  // restart the cluster to verify it again
  cluster_->Shutdown();
  ASSERT_OK(cluster_->Start());
  unique_ptr<KuduTableStatistics> statistics;
  KuduTableStatistics *table_statistics;
  ASSERT_OK(client_->GetTableStatistics(kTableName, &table_statistics));
  statistics.reset(table_statistics);
  ASSERT_EQ(on_disk_size_limit, statistics->on_disk_size_limit());
  ASSERT_EQ(row_count_limit, statistics->live_row_count_limit());
}

// Verify the table's row limit
TEST_F(DisableWriteWhenExceedingQuotaTest, TestDisableWritePrivilegeWhenExceedingRowsQuota) {
  NO_FATALS(TestRowLimit());
  // restart the cluster to verify it again
  cluster_->Shutdown();
  ASSERT_OK(cluster_->Start());
  ASSERT_OK(SetupClient(kUser));
  NO_FATALS(TestRowLimit());
}

// Verify the table's disk size limit
TEST_F(DisableWriteWhenExceedingQuotaTest, TestDisableWritePrivilegeWhenExceedingSizeQuota) {
  // modify the table limit to allow more rows but small size
  NO_FATALS(ModifyLimit(1024L * 1024 + 120L * 1024, kRowCountLimit));
  // refresh the client
  ASSERT_OK(SetupClient(kUser));
  NO_FATALS(TestSizeLimit());

  // restart the cluster to verify it again
  cluster_->Shutdown();
  ASSERT_OK(cluster_->Start());
  ASSERT_OK(SetupClient(kUser));
  NO_FATALS(TestSizeLimit());
}

TEST_F(DisableWriteWhenExceedingQuotaTest, TestDisableWriteWhenExceedingRowsQuotaWithFactor) {
  FLAGS_table_write_limit_ratio = 0.5;
  // restart the cluster to make the limit factor take effect
  cluster_->Shutdown();
  ASSERT_OK(cluster_->Start());
  // modify the table limit to allow more rows but small size
  NO_FATALS(ModifyLimit(10L * 1024 * 1024, 4L));
  // refresh the client
  ASSERT_OK(SetupClient(kUser));
  NO_FATALS(TestRowLimit());

  // modify the table limit to allow more rows but small size
  NO_FATALS(ModifyLimit(2L * 1024 * 1024 + 120L * 1024, kRowCountLimit));
  // refresh the client
  ASSERT_OK(SetupClient(kUser));
  NO_FATALS(TestSizeLimit());
}
} // namespace kudu
