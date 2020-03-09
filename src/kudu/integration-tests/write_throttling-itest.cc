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
#include <ostream>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

DEFINE_int32(throttling_test_time, 3,
             "Number of seconds to run write throttling test");

using kudu::client::KuduColumnSchema;
using kudu::client::KuduInsert;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace tablet {

class WriteThrottlingTest : public ExternalMiniClusterITestBase {
 protected:
  WriteThrottlingTest() {
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT64)->NotNull()->PrimaryKey();
    b.AddColumn("string_val")->Type(KuduColumnSchema::STRING)->NotNull();
    CHECK_OK(b.Build(&schema_));
  }

  void CreateTable() {
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    ASSERT_OK(table_creator->table_name(kTableName)
             .schema(&schema_)
             .set_range_partition_columns({ "key" })
             .num_replicas(1)
             .Create());
    ASSERT_OK(client_->OpenTable(kTableName, &table_));
  }

 protected:
  static const char* const kTableName;

  KuduSchema schema_;
  client::sp::shared_ptr<KuduTable> table_;
};

const char* const WriteThrottlingTest::kTableName = "write-throttling-test-tbl";

TEST_F(WriteThrottlingTest, ThrottleWriteRpcPerSec) {
  // Normally a single thread session can write at 500 qps,
  // so we throttle at 100 qps.
  const int TARGET_QPS = 100;
  vector<string> ts_flags = { "--tablet_throttler_rpc_per_sec=100",
                              "--tablet_throttler_bytes_per_sec=0" };
  NO_FATALS(StartCluster(ts_flags));
  NO_FATALS(CreateTable());
  string string_val = string(10, 'a');
  client::sp::shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(5000);
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));
  for (int t = 0; t < FLAGS_throttling_test_time; t++) {
    MonoTime begin = MonoTime::Now();
    for (int i = 0; i < TARGET_QPS; i++) {
      unique_ptr<KuduInsert> insert(table_->NewInsert());
      KuduPartialRow* row = insert->mutable_row();
      CHECK_OK(row->SetInt64("key", t * TARGET_QPS + i));
      CHECK_OK(row->SetStringNoCopy("string_val", string_val));
      CHECK_OK(session->Apply(insert.release()));
    }
    MonoTime end = MonoTime::Now();
    MonoDelta delta = end - begin;
    double qps = TARGET_QPS / delta.ToSeconds();
    LOG(INFO) << "Iteration " << t << " qps: " << qps;
    ASSERT_LE(qps, TARGET_QPS * 1.2f);
  }
}

TEST_F(WriteThrottlingTest, ThrottleWriteBytesPerSec) {
  const int TARGET_QPS = 100;
  const int BYTES_PER_RPC = 3000;
  // Normally a single thread session can write at 500 qps,
  // so we throttle at 100 qps * 3000 byte = 300000 byte/s.
  // 8 byte key + 2992 byte string_val, plus other insignificant parts,
  // total bytes is still about 3000+ byte per RPC.
  vector<string> ts_flags = { "--tablet_throttler_rpc_per_sec=0",
                              "--tablet_throttler_bytes_per_sec=300000" };
  NO_FATALS(StartCluster(ts_flags));
  NO_FATALS(CreateTable());
  string string_val = string(BYTES_PER_RPC - 8, 'a');
  client::sp::shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(5000);
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));
  for (int t = 0; t < FLAGS_throttling_test_time; t++) {
    MonoTime begin = MonoTime::Now();
    for (int i = 0; i < TARGET_QPS; i++) {
      unique_ptr<KuduInsert> insert(table_->NewInsert());
      KuduPartialRow* row = insert->mutable_row();
      CHECK_OK(row->SetInt64("key", t * TARGET_QPS + i));
      CHECK_OK(row->SetStringNoCopy("string_val", string_val));
      CHECK_OK(session->Apply(insert.release()));
    }
    MonoTime end = MonoTime::Now();
    MonoDelta delta = end - begin;
    double qps = TARGET_QPS / delta.ToSeconds();
    double bps = TARGET_QPS * BYTES_PER_RPC / delta.ToSeconds();
    LOG(INFO) << "Iteration " << t << " qps: " << qps << " " << bps << " byte/s";
    ASSERT_LE(bps, TARGET_QPS * BYTES_PER_RPC * 1.2f);
  }
}

} // namespace tablet
} // namespace kudu
