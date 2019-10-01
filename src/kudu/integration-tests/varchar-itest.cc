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

#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using std::unique_ptr;

namespace kudu {
namespace client {

using sp::shared_ptr;

class VarcharItest : public ExternalMiniClusterITestBase {
 protected:
  void SetUp() override {
    NO_FATALS(StartCluster({}, {}, kNumServers));
  }

  const int kNumServers = 3;
  const int kNumTablets = 3;
  const char* const kTableName = "varchar-table";
};

TEST_F(VarcharItest, TestVarcharTruncation) {
  KuduSchemaBuilder builder;
  builder.AddColumn("key")->Type(KuduColumnSchema::INT64)
    ->NotNull()->PrimaryKey();
  builder.AddColumn("value")->Type(KuduColumnSchema::VARCHAR)
    ->Length(10);
  KuduSchema schema;
  ASSERT_OK(builder.Build(&schema));

  // Create table
  unique_ptr<client::KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(kTableName)
      .schema(&schema)
      .num_replicas(kNumServers)
      .add_hash_partitions({ "key" }, kNumTablets)
      .Create());
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableName, &table));

  // Insert row with long varchar values to test the truncation happens properly
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));
  KuduInsert* insert = table->NewInsert();
  KuduPartialRow* write = insert->mutable_row();

  ASSERT_OK(write->SetInt64("key", 1));
  ASSERT_OK(write->SetVarchar("value", "foobar         baz"));
  ASSERT_OK(session->Apply(insert));
  ASSERT_OK(session->Flush());

  // Read rows
  KuduScanner scanner(table.get());
  ASSERT_OK(scanner.SetFaultTolerant());
  ASSERT_OK(scanner.Open());
  while (scanner.HasMoreRows()) {
    KuduScanBatch batch;
    ASSERT_OK(scanner.NextBatch(&batch));
    for (const KuduScanBatch::RowPtr& read : batch) {
      int64_t key;
      ASSERT_OK(read.GetInt64("key", &key));
      ASSERT_EQ(1, key);
      Slice value;
      ASSERT_OK(read.GetVarchar("value", &value));
      ASSERT_EQ("foobar    ", value);
    }
  }
}

TEST_F(VarcharItest, TestInvalidLength) {
  KuduSchemaBuilder builder;
  builder.AddColumn("key")->Type(KuduColumnSchema::INT64)
    ->NotNull()->PrimaryKey();
  builder.AddColumn("value")->Type(KuduColumnSchema::VARCHAR)
    ->Length(0);
  KuduSchema schema;
  Status s = builder.Build(&schema);
  ASSERT_FALSE(s.ok());
}

TEST_F(VarcharItest, TestVarcharRangePartition) {
  KuduSchemaBuilder builder;
  builder.AddColumn("key")->Type(KuduColumnSchema::VARCHAR)
    ->Length(10)->NotNull()->PrimaryKey();
  builder.AddColumn("value")->Type(KuduColumnSchema::VARCHAR)
    ->Length(10);
  KuduSchema schema;
  ASSERT_OK(builder.Build(&schema));

  KuduPartialRow* lower_bound = schema.NewRow();
  ASSERT_OK(lower_bound->SetVarchar("key", "bar"));
  KuduPartialRow* upper_bound = schema.NewRow();
  ASSERT_OK(upper_bound->SetVarchar("key", "foo"));

  unique_ptr<client::KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(kTableName)
      .schema(&schema)
      .num_replicas(kNumServers)
      .set_range_partition_columns({ "key" })
      .add_range_partition(lower_bound, upper_bound)
      .Create());
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableName, &table));

  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));
  KuduInsert* insert = table->NewInsert();
  KuduPartialRow* write = insert->mutable_row();

  ASSERT_OK(write->SetVarchar("key", "baz"));
  ASSERT_OK(write->SetVarchar("value", "foobar"));
  ASSERT_OK(session->Apply(insert));
  ASSERT_OK(session->Flush());

  KuduScanner scanner(table.get());
  ASSERT_OK(scanner.SetFaultTolerant());
  ASSERT_OK(scanner.Open());
  ASSERT_TRUE(scanner.HasMoreRows());
  while (scanner.HasMoreRows()) {
    KuduScanBatch batch;
    ASSERT_OK(scanner.NextBatch(&batch));
    for (const KuduScanBatch::RowPtr& read : batch) {
      Slice key;
      ASSERT_OK(read.GetVarchar("key", &key));
      ASSERT_EQ("baz", key);
      Slice value;
      ASSERT_OK(read.GetVarchar("value", &value));
      ASSERT_EQ("foobar", value);
    }
  }
}

} // namespace client
} // namespace kudu
