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

#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <atomic>

#include "kudu/client/client.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/integration-tests/mini_cluster.h"
#include "kudu/util/test_util.h"

using std::atomic;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace client {

using sp::shared_ptr;

class ScanTokenTest : public KuduTest {

 protected:

  void SetUp() override {
    // Set up the mini cluster
    cluster_.reset(new MiniCluster(env_.get(), MiniClusterOptions()));
    ASSERT_OK(cluster_->Start());
    KuduClientBuilder client_builder;
    ASSERT_OK(cluster_->CreateClient(&client_builder, &client_));
  }

  void TearDown() override {
    if (cluster_) {
      cluster_->Shutdown();
      cluster_.reset();
    }
  }

  // Creates a new session in manual flush mode.
  shared_ptr<KuduSession> CreateSession() {
    shared_ptr<KuduSession> session = client_->NewSession();
    session->SetTimeoutMillis(10000);
    CHECK_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    return session;
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

      threads.emplace_back(thread([this, &rows] (string serialized_token) {
        KuduClientBuilder client_builder;
        shared_ptr<KuduClient> client;
        ASSERT_OK(cluster_->CreateClient(&client_builder, &client));
        KuduScanner* scanner_ptr;
        ASSERT_OK(KuduScanToken::DeserializeIntoScanner(client.get(),
                                                        serialized_token,
                                                        &scanner_ptr));
        unique_ptr<KuduScanner> scanner(scanner_ptr);
        scanner->Open();

        while (scanner->HasMoreRows()) {
          KuduScanBatch batch;
          CHECK_OK(scanner->NextBatch(&batch));
          rows += batch.NumRows();
        }
        scanner->Close();
      }, std::move(buf)));
    }

    for (thread& thread : threads) {
      thread.join();
    }

    return rows;
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
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

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
  }
}

} // namespace client
} // namespace kudu
