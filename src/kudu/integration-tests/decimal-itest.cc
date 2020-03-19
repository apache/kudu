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

#include <cstdint>
#include <memory>
#include <string>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/value.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/util/decimal_util.h"
#include "kudu/util/int128.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using std::string;
using std::unique_ptr;

namespace kudu {
namespace client {

using sp::shared_ptr;

class DecimalItest : public ExternalMiniClusterITestBase {
};

// Tests writing and reading various decimal columns with various
// precisions and scales to ensure the values match expectations.
TEST_F(DecimalItest, TestDecimalTypes) {
  const int kNumServers = 3;
  const int kNumTablets = 3;
  const char* const kTableName = "decimal-table";
  NO_FATALS(StartCluster({}, {}, kNumServers));

  // Create Schema
  KuduSchemaBuilder builder;
  builder.AddColumn("key")->Type(KuduColumnSchema::DECIMAL)
      ->Precision(kMaxDecimal64Precision)->NotNull()->PrimaryKey();
  builder.AddColumn("numeric32")->Type(KuduColumnSchema::DECIMAL)
      ->Precision(kMaxDecimal32Precision);
  builder.AddColumn("scale32")->Type(KuduColumnSchema::DECIMAL)
      ->Precision(kMaxDecimal32Precision)
      ->Scale(kMaxDecimal32Precision);
  builder.AddColumn("numeric64")->Type(KuduColumnSchema::DECIMAL)
      ->Precision(kMaxDecimal64Precision);
  builder.AddColumn("scale64")->Type(KuduColumnSchema::DECIMAL)
      ->Precision(kMaxDecimal64Precision)
      ->Scale(kMaxDecimal64Precision);
  builder.AddColumn("numeric128")->Type(KuduColumnSchema::DECIMAL)
      ->Precision(kMaxDecimal128Precision);
  builder.AddColumn("scale128")->Type(KuduColumnSchema::DECIMAL)
      ->Precision(kMaxDecimal128Precision)
      ->Scale(kMaxDecimal128Precision);
  builder.AddColumn("default")->Type(KuduColumnSchema::DECIMAL)
      ->Precision(5)->Scale(2)
      ->Default(KuduValue::FromDecimal(12345, 2));
  builder.AddColumn("alteredDefault")->Type(KuduColumnSchema::DECIMAL)
      ->Precision(5)->Scale(2);
  builder.AddColumn("money")->Type(KuduColumnSchema::DECIMAL)
      ->Precision(6)->Scale(2);
  builder.AddColumn("small")->Type(KuduColumnSchema::DECIMAL)
      ->Precision(2);
  builder.AddColumn("null")->Type(KuduColumnSchema::DECIMAL)
      ->Precision(8);
  KuduSchema schema;
  ASSERT_OK(builder.Build(&schema));

  // Create Table
  unique_ptr<client::KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(kTableName)
      .schema(&schema)
      .num_replicas(kNumServers)
      .add_hash_partitions({ "key" }, kNumTablets)
      .Create());
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableName, &table));

  // Alter Default Value
  unique_ptr<client::KuduTableAlterer> table_alterer(client_->NewTableAlterer(kTableName));
  table_alterer->AlterColumn("alteredDefault")->Default(KuduValue::FromDecimal(456789, 2));
  ASSERT_OK(table_alterer->Alter());

  // Insert Row
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));
  KuduInsert* insert = table->NewInsert();
  KuduPartialRow* write = insert->mutable_row();
  ASSERT_OK(write->SetUnscaledDecimal("key", 1));
  ASSERT_OK(write->SetUnscaledDecimal("numeric32", 123456789));
  ASSERT_OK(write->SetUnscaledDecimal("scale32", 123456789));
  ASSERT_OK(write->SetUnscaledDecimal("numeric64", 12345678910111213L));
  ASSERT_OK(write->SetUnscaledDecimal("scale64", 12345678910111213L));
  ASSERT_OK(write->SetUnscaledDecimal("numeric128",
      static_cast<int128_t>(12345678910111213L) * 1000000000000000000L));
  ASSERT_OK(write->SetUnscaledDecimal("scale128",
      static_cast<int128_t>(12345678910111213L) * 1000000000000000000L));
  ASSERT_OK(write->SetUnscaledDecimal("money", 123400));
  // Test a value thats too large
  Status s = write->SetUnscaledDecimal("small", 999);
  EXPECT_EQ("Invalid argument: value 999 out of range for decimal column 'small'",
            s.ToString());
  ASSERT_OK(session->Apply(insert));
  ASSERT_OK(session->Flush());

  // Read Rows
  KuduScanner scanner(table.get());
  ASSERT_OK(scanner.SetFaultTolerant());
  ASSERT_OK(scanner.Open());
  while (scanner.HasMoreRows()) {
    KuduScanBatch batch;
    CHECK_OK(scanner.NextBatch(&batch));
    for (const KuduScanBatch::RowPtr& read : batch) {
      // Verify the row
      int128_t key;
      ASSERT_OK(read.GetUnscaledDecimal("key", &key));
      ASSERT_EQ("1", DecimalToString(key, kDefaultDecimalScale));
      int128_t numeric32;
      ASSERT_OK(read.GetUnscaledDecimal("numeric32", &numeric32));
      ASSERT_EQ("123456789", DecimalToString(numeric32, kDefaultDecimalScale));
      int128_t scale32;
      ASSERT_OK(read.GetUnscaledDecimal("scale32", &scale32));
      ASSERT_EQ("0.123456789",
                DecimalToString(scale32, kMaxDecimal32Precision));
      int128_t numeric64;
      ASSERT_OK(read.GetUnscaledDecimal("numeric64", &numeric64));
      ASSERT_EQ("12345678910111213", DecimalToString(numeric64, kDefaultDecimalScale));
      int128_t scale64;
      ASSERT_OK(read.GetUnscaledDecimal("scale64", &scale64));
      ASSERT_EQ("0.012345678910111213",
                DecimalToString(scale64, kMaxDecimal64Precision));
      int128_t numeric128;
      ASSERT_OK(read.GetUnscaledDecimal("numeric128", &numeric128));
      ASSERT_EQ("12345678910111213000000000000000000",
                DecimalToString(numeric128, kDefaultDecimalScale));
      int128_t scale128;
      ASSERT_OK(read.GetUnscaledDecimal("scale128", &scale128));
      ASSERT_EQ("0.00012345678910111213000000000000000000",
                DecimalToString(scale128, kMaxDecimal128Precision));
      int128_t money;
      ASSERT_OK(read.GetUnscaledDecimal("money", &money));
      ASSERT_EQ("1234.00", DecimalToString(money, 2));
      int128_t defaulted;
      ASSERT_OK(read.GetUnscaledDecimal("default", &defaulted));
      ASSERT_EQ("123.45", DecimalToString(defaulted, 2));
      int128_t altered;
      ASSERT_OK(read.GetUnscaledDecimal("alteredDefault", &altered));
      ASSERT_EQ("4567.89", DecimalToString(altered, 2));
      ASSERT_TRUE(read.IsNull("null"));
      // Try to read a decimal as an integer.
      int32_t int32;
      Status s = read.GetInt32("numeric32", &int32);
      EXPECT_EQ("Invalid argument: invalid type int32 provided for column "
                "'numeric32' (expected decimal)", s.ToString());
    }
  }
}

} // namespace client
} // namespace kudu
