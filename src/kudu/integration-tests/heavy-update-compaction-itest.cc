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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/row_result.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/scan_predicate.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/value.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/util/faststring.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int32(flush_threshold_mb);

DEFINE_int32(rounds, 100,
             "How many rounds of updates will be performed. More rounds make the "
             "test take longer, but add more data and stress.");
DEFINE_int32(rows, 500,
             "How many base rows in the update set. More rounds results in more rowsets "
             "with updates.");
DEFINE_int32(num_columns, 5, "The number of value columns to test.");
DEFINE_int32(scan_timeout_ms, 120 * 1000,
             "Sets the scanner timeout. It may be necessary to increase the "
             "timeout if the number of rounds or rows is increased.");

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduInsert;
using kudu::client::KuduPredicate;
using kudu::client::KuduRowResult;
using kudu::client::KuduScanBatch;
using kudu::client::KuduScanner;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::KuduUpdate;
using kudu::client::KuduValue;
using kudu::client::sp::shared_ptr;
using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace tablet {

class HeavyUpdateCompactionITest : public KuduTest {
 protected:

  const char* const kTableName = "heavy-update-compaction-test";

  HeavyUpdateCompactionITest()
      : rand_(SeedRandom()) {

#ifdef THREAD_SANITIZER
    // The test is very slow with TSAN enabled due to the amount of data
    // written and compacted, so turn down the volume a bit.
    if (gflags::GetCommandLineFlagInfoOrDie("rows").is_default) {
      FLAGS_rows = 20;
    }
#endif

    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT64)->NotNull()->PrimaryKey();

    for (int i = 0; i < FLAGS_num_columns; i++) {
      string name("val_");
      name.push_back('a' + i);
      b.AddColumn(name)->Type(KuduColumnSchema::STRING)->NotNull();
    }

    CHECK_OK(b.Build(&schema_));
  }

  void SetUp() override {
    // Encourage frequent flushes.
    FLAGS_flush_threshold_mb = 2;
    KuduTest::SetUp();
  }

  void CreateTable() {
    NO_FATALS(InitCluster());
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    ASSERT_OK(table_creator->table_name(kTableName)
             .schema(&schema_)
             .set_range_partition_columns({})
             .num_replicas(1)
             .Create());
    ASSERT_OK(client_->OpenTable(kTableName, &table_));
  }

  void TearDown() override {
    if (cluster_) {
      cluster_->Shutdown();
    }
    KuduTest::TearDown();
  }

  shared_ptr<KuduSession> CreateSession() {
    shared_ptr<KuduSession> session = client_->NewSession();
    session->SetTimeoutMillis(30000);
    CHECK_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));
    return session;
  }

  // Sets the passed values on the row.
  void MakeRow(int64_t key, KuduPartialRow* row) {
    CHECK_OK(row->SetInt64(0, key));

    faststring s;
    s.resize(1024 * 8);
    for (int idx = 1; idx <= FLAGS_num_columns; idx++) {
      RandomString(s.data(), s.size(), &rand_);
      CHECK_OK(row->SetStringCopy(idx, s));
    }
  }

  void InitCluster() {
    // Start mini-cluster with 1 tserver.
    cluster_.reset(new InternalMiniCluster(env_, InternalMiniClusterOptions()));
    ASSERT_OK(cluster_->Start());
    KuduClientBuilder client_builder;
    client_builder.add_master_server_addr(cluster_->mini_master()->bound_rpc_addr_str());
    ASSERT_OK(client_builder.Build(&client_));
  }

  KuduSchema schema_;
  std::shared_ptr<InternalMiniCluster> cluster_;
  shared_ptr<KuduTable> table_;
  shared_ptr<KuduClient> client_;
  Random rand_;
};

// Repro for KUDU-2231, which is an integer overflow in the RowSetInfo class.
// This test creates a rowset with a very large amount of REDO delta files
// (the bug was a 2GiB overflow), as well as mixed inserts. This causes the
// maintanance manager to schedule rowset compactions, which sometimes
// reproduces the overflow.
TEST_F(HeavyUpdateCompactionITest, TestHeavyUpdateCompaction) {
  NO_FATALS(CreateTable());
  shared_ptr<KuduSession> session = CreateSession();

  // Insert an initial batch of rows.
  LOG_TIMING(INFO, "inserting") {
    for (int64_t key = 0; key < FLAGS_rows; key++) {
      unique_ptr<KuduInsert> insert(table_->NewInsert());
      MakeRow(key, insert->mutable_row());
      ASSERT_OK(session->Apply(insert.release()));
    }
    ASSERT_OK(session->Flush());
  }

  // Vector of flattened final values of the updated rows. We keep these around
  // in order to verify the table is consistent during the scan step.
  vector<string> final_values;

  // Update the rows.
  LOG_TIMING(INFO, "updating") {
    for (int64_t round = 0; round < FLAGS_rounds; round++) {
      for (int64_t key = 0; key < FLAGS_rows; key++) {
        unique_ptr<KuduUpdate> update(table_->NewUpdate());
        MakeRow(key, update->mutable_row());

        if (round + 1 == FLAGS_rounds) {
          for (int idx = 1; idx <= FLAGS_num_columns; idx++) {
            Slice val;
            ASSERT_OK(update->mutable_row()->GetString(idx, &val));
            final_values.emplace_back(val.ToString());
          }
        }

        ASSERT_OK(session->Apply(update.release()));
      }

      // Insert an additional row so that more rowsets are created, and the MM
      // will run the rowset compaction calculations.
      unique_ptr<KuduInsert> insert(table_->NewInsert());
      MakeRow(FLAGS_rows + round, insert->mutable_row());
      ASSERT_OK(session->Apply(insert.release()));
      ASSERT_OK(session->Flush());
    }
  }

  // Scan the updated rows and ensure the final values are present.
  KuduScanner scanner(table_.get());
  ASSERT_OK(scanner.SetFaultTolerant());
  ASSERT_OK(scanner.AddConjunctPredicate(
      table_->NewComparisonPredicate(
        "key", KuduPredicate::LESS, KuduValue::FromInt(FLAGS_rows))));

  // Walking the updates can take a long time.
  scanner.SetTimeoutMillis(FLAGS_scan_timeout_ms);

  LOG_TIMING(INFO, "scanning") {
    ASSERT_OK(scanner.Open());
    size_t final_values_offset = 0;
    KuduScanBatch batch;
    while (scanner.HasMoreRows()) {
      ASSERT_OK(scanner.NextBatch(&batch));
      for (const KuduScanBatch::RowPtr row : batch) {
        for (int idx = 1; idx <= FLAGS_num_columns; idx++) {
          ASSERT_GT(final_values.size(), final_values_offset);
          Slice actual_val;
          ASSERT_OK(row.GetString(idx, &actual_val));
          EXPECT_EQ(actual_val, final_values[final_values_offset++]);
        }
      }
    }
  }
}

} // namespace tablet
} // namespace kudu
