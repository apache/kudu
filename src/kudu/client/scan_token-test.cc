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
#include <memory>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/client/client.pb.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/integration-tests/mini_cluster.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace client {

using sp::shared_ptr;
using std::atomic;
using std::string;
using std::thread;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using tserver::MiniTabletServer;

class ScanTokenTest : public KuduTest {

 protected:

  void SetUp() override {
    // Set up the mini cluster
    cluster_.reset(new MiniCluster(env_, MiniClusterOptions()));
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(cluster_->CreateClient(nullptr, &client_));
  }

  // Count the rows in a table which satisfy the specified predicates. Simulates
  // a central query planner / remote task execution by creating a thread per
  // token, each with a new client.
  int CountRows(const vector<KuduScanToken*>& tokens) {
    atomic<uint32_t> rows(0);
    vector<thread> threads;
    for (KuduScanToken* token : tokens) {
      string buf;
      CHECK_OK(token->Serialize(&buf));

      threads.emplace_back([this, &rows] (string serialized_token) {
        shared_ptr<KuduClient> client;
        ASSERT_OK(cluster_->CreateClient(nullptr, &client));
        KuduScanner* scanner_ptr;
        ASSERT_OK(KuduScanToken::DeserializeIntoScanner(client.get(),
                                                        serialized_token,
                                                        &scanner_ptr));
        unique_ptr<KuduScanner> scanner(scanner_ptr);
        ASSERT_OK(scanner->Open());

        while (scanner->HasMoreRows()) {
          KuduScanBatch batch;
          CHECK_OK(scanner->NextBatch(&batch));
          rows += batch.NumRows();
        }
        scanner->Close();
      }, std::move(buf));
    }

    for (thread& thread : threads) {
      thread.join();
    }

    return rows;
  }

  void VerifyTabletInfo(const vector<KuduScanToken*>& tokens) {
    unordered_set<string> tablet_ids;
    for (auto t : tokens) {
      tablet_ids.insert(t->tablet().id());

      // Check that there's only one replica; this is a non-replicated table.
      ASSERT_EQ(1, t->tablet().replicas().size());

      // Check that this replica is a leader; since there's only one tserver,
      // it must be.
      const KuduReplica* r = t->tablet().replicas()[0];
      ASSERT_TRUE(r->is_leader());

      // Check that the tserver associated with the replica is the sole tserver
      // started for this cluster.
      const MiniTabletServer* ts = cluster_->mini_tablet_server(0);
      ASSERT_EQ(ts->server()->instance_pb().permanent_uuid(),
                r->ts().uuid());
      ASSERT_EQ(ts->bound_rpc_addr().host(), r->ts().hostname());
      ASSERT_EQ(ts->bound_rpc_addr().port(), r->ts().port());
    }
    // Check that there are no duplicate tablet IDs.
    ASSERT_EQ(tokens.size(), tablet_ids.size());
  }

  shared_ptr<KuduClient> client_;
  gscoped_ptr<MiniCluster> cluster_;
};

TEST_F(ScanTokenTest, TestScanTokens) {
  // Create schema
  KuduSchema schema;
  {
    KuduSchemaBuilder builder;
    builder.AddColumn("col")->NotNull()->Type(KuduColumnSchema::INT64)->PrimaryKey();
    ASSERT_OK(builder.Build(&schema));
  }

  // Create table
  shared_ptr<KuduTable> table;
  {
    unique_ptr<KuduPartialRow> split(schema.NewRow());
    ASSERT_OK(split->SetInt64("col", 0));
    unique_ptr<client::KuduTableCreator> table_creator(client_->NewTableCreator());
    ASSERT_OK(table_creator->table_name("table")
                            .schema(&schema)
                            .add_hash_partitions({ "col" }, 4)
                            .split_rows({ split.release() })
                            .num_replicas(1)
                            .Create());
    ASSERT_OK(client_->OpenTable("table", &table));
  }

  // Create session
  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(10000);
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));

  // Insert rows
  for (int i = -100; i < 100; i++) {
      unique_ptr<KuduInsert> insert(table->NewInsert());
      ASSERT_OK(insert->mutable_row()->SetInt64("col", i));
      ASSERT_OK(session->Apply(insert.release()));
  }
  ASSERT_OK(session->Flush());

  { // no predicates
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    ASSERT_OK(KuduScanTokenBuilder(table.get()).Build(&tokens));

    ASSERT_EQ(8, tokens.size());
    ASSERT_EQ(200, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // range predicate
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPredicate> predicate(table->NewComparisonPredicate("col",
                                                                      KuduPredicate::GREATER_EQUAL,
                                                                      KuduValue::FromInt(0)));
    ASSERT_OK(builder.AddConjunctPredicate(predicate.release()));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_EQ(4, tokens.size());
    ASSERT_EQ(100, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // equality predicate
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPredicate> predicate(table->NewComparisonPredicate("col",
                                                                      KuduPredicate::EQUAL,
                                                                      KuduValue::FromInt(42)));
    ASSERT_OK(builder.AddConjunctPredicate(predicate.release()));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_EQ(1, tokens.size());
    ASSERT_EQ(1, CountRows(std::move(tokens)));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // primary key bound
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPartialRow> lower_bound(schema.NewRow());
    ASSERT_OK(lower_bound->SetInt64("col", 40));

    ASSERT_OK(builder.AddLowerBound(*lower_bound));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_EQ(4, tokens.size());
    ASSERT_EQ(60, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }
}

TEST_F(ScanTokenTest, TestScanTokensWithNonCoveringRange) {
  // Create schema
  KuduSchema schema;
  {
    KuduSchemaBuilder builder;
    builder.AddColumn("col")->NotNull()->Type(KuduColumnSchema::INT64)->PrimaryKey();
    ASSERT_OK(builder.Build(&schema));
  }

  // Create table
  shared_ptr<KuduTable> table;
  {
    unique_ptr<KuduPartialRow> lower_bound(schema.NewRow());
    unique_ptr<KuduPartialRow> upper_bound(schema.NewRow());
    unique_ptr<client::KuduTableCreator> table_creator(client_->NewTableCreator());
    table_creator->table_name("table");
    table_creator->num_replicas(1);
    table_creator->schema(&schema);

    ASSERT_OK(lower_bound->SetInt64("col", 0));
    ASSERT_OK(upper_bound->SetInt64("col", 100));
    table_creator->add_range_partition(lower_bound.release(), upper_bound.release());

    lower_bound.reset(schema.NewRow());
    upper_bound.reset(schema.NewRow());
    ASSERT_OK(lower_bound->SetInt64("col", 200));
    ASSERT_OK(upper_bound->SetInt64("col", 400));
    table_creator->add_range_partition(lower_bound.release(), upper_bound.release());

    unique_ptr<KuduPartialRow> split(schema.NewRow());
    ASSERT_OK(split->SetInt64("col", 300));
    table_creator->add_range_partition_split(split.release());
    table_creator->add_hash_partitions({ "col" }, 2);

    ASSERT_OK(table_creator->Create());
    ASSERT_OK(client_->OpenTable("table", &table));
  }

  // Create session
  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(10000);
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));

  // Insert rows
  for (int i = 0; i < 100; i++) {
      unique_ptr<KuduInsert> insert(table->NewInsert());
      ASSERT_OK(insert->mutable_row()->SetInt64("col", i));
      ASSERT_OK(session->Apply(insert.release()));
  }
  for (int i = 200; i < 400; i++) {
      unique_ptr<KuduInsert> insert(table->NewInsert());
      ASSERT_OK(insert->mutable_row()->SetInt64("col", i));
      ASSERT_OK(session->Apply(insert.release()));
  }
  ASSERT_OK(session->Flush());

  { // no predicates
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    ASSERT_OK(KuduScanTokenBuilder(table.get()).Build(&tokens));

    ASSERT_EQ(6, tokens.size());
    ASSERT_EQ(300, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // range predicate
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPredicate> predicate(table->NewComparisonPredicate("col",
                                                                      KuduPredicate::GREATER_EQUAL,
                                                                      KuduValue::FromInt(200)));
    ASSERT_OK(builder.AddConjunctPredicate(predicate.release()));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_EQ(4, tokens.size());
    ASSERT_EQ(200, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // equality predicate
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPredicate> predicate(table->NewComparisonPredicate("col",
                                                                      KuduPredicate::EQUAL,
                                                                      KuduValue::FromInt(42)));
    ASSERT_OK(builder.AddConjunctPredicate(predicate.release()));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_EQ(1, tokens.size());
    ASSERT_EQ(1, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }

  { // primary key bound
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table.get());
    unique_ptr<KuduPartialRow> upper_bound(schema.NewRow());
    ASSERT_OK(upper_bound->SetInt64("col", 40));

    ASSERT_OK(builder.AddUpperBound(*upper_bound));
    ASSERT_OK(builder.Build(&tokens));

    ASSERT_EQ(2, tokens.size());
    ASSERT_EQ(40, CountRows(tokens));
    NO_FATALS(VerifyTabletInfo(tokens));
  }
}

// When building a scanner from a serialized scan token,
// verify that the propagated timestamp from the token makes its way into the
// latest observed timestamp of the client object.
TEST_F(ScanTokenTest, TestTimestampPropagation) {
  static const string kTableName = "p_ts_table";

  // Create a table to work with:
  //   * Deserializing a scan token into a scanner requires the table to exist.
  //   * Creating a scan token requires the table to exist.
  shared_ptr<KuduTable> table;
  {
    static const string kKeyColumnName = "c_key";
    KuduSchema schema;
    {
      KuduSchemaBuilder builder;
      builder.AddColumn(kKeyColumnName)->NotNull()->
          Type(KuduColumnSchema::INT64)->PrimaryKey();
      ASSERT_OK(builder.Build(&schema));
    }

    {
      unique_ptr<KuduPartialRow> split(schema.NewRow());
      ASSERT_OK(split->SetInt64(kKeyColumnName, 0));
      unique_ptr<client::KuduTableCreator> creator(client_->NewTableCreator());
      ASSERT_OK(creator->table_name(kTableName)
                .schema(&schema)
                .add_hash_partitions({ kKeyColumnName }, 2)
                .split_rows({ split.release() })
                .num_replicas(1)
                .Create());
    }
  }

  // Deserialize a scan token and make sure the client's last observed timestamp
  // is updated accordingly.
  {
    const uint64_t ts_prev = client_->GetLatestObservedTimestamp();
    const uint64_t ts_propagated = ts_prev + 1000000;

    ScanTokenPB pb;
    pb.set_table_name(kTableName);
    pb.set_read_mode(::kudu::READ_AT_SNAPSHOT);
    pb.set_propagated_timestamp(ts_propagated);
    const string serialized_token = pb.SerializeAsString();
    EXPECT_EQ(ts_prev, client_->GetLatestObservedTimestamp());

    KuduScanner* scanner_raw;
    ASSERT_OK(KuduScanToken::DeserializeIntoScanner(client_.get(),
                                                    serialized_token,
                                                    &scanner_raw));
    // The caller of the DeserializeIntoScanner() is responsible for
    // de-allocating the result scanner object.
    unique_ptr<KuduScanner> scanner(scanner_raw);
    EXPECT_EQ(ts_propagated, client_->GetLatestObservedTimestamp());
  }

  // Build the set of scan tokens for the table, serialize them and
  // make sure the serialized tokens contain the propagated timestamp.
  {
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    const uint64_t ts_prev = client_->GetLatestObservedTimestamp();
    const uint64_t ts_propagated = ts_prev + 1000000;

    client_->SetLatestObservedTimestamp(ts_propagated);
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    ASSERT_OK(KuduScanTokenBuilder(table.get()).Build(&tokens));
    for (const auto* t : tokens) {
      string serialized_token;
      ASSERT_OK(t->Serialize(&serialized_token));

      ScanTokenPB pb;
      ASSERT_TRUE(pb.ParseFromString(serialized_token));
      ASSERT_TRUE(pb.has_propagated_timestamp());
      EXPECT_EQ(ts_propagated, pb.propagated_timestamp());
    }
  }
}

} // namespace client
} // namespace kudu
