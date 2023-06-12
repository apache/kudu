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
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/iterator.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tablet {

namespace {
Schema CreateAutoIncrementingTestSchema() {
  return Schema({ColumnSchema("key", INT64, false, false,
                              /*is_auto_incrementing*/ true, nullptr, nullptr, {}, {}, ""),
                 ColumnSchema("val", INT32, true) }, 1);
}
} // anonymous namespace

// Creates a table with single tablet with an auto incrementing column
// and a writer object for the tablet.
class AutoIncrementingTabletTest : public KuduTabletTest {
 public:
  AutoIncrementingTabletTest()
      : KuduTabletTest(CreateAutoIncrementingTestSchema()) {}

  void SetUp() override {
    KuduTabletTest::SetUp();
    writer_.reset(new LocalTabletWriter(tablet().get(), &client_schema_));
  }
 protected:
  unique_ptr<LocalTabletWriter> writer_;
};

TEST_F(AutoIncrementingTabletTest, TestInsertOp) {
  // Insert rows into the tablet populating only non auto-incrementing columns.
  for (int i = 0; i < 10; i++) {
    unique_ptr<KuduPartialRow> row(new KuduPartialRow(&client_schema_));
    ASSERT_OK(row->SetInt32(1, 1337));
    ASSERT_OK(writer_->Insert(*row));
  }

  // Scan the tablet data and verify the auto increment counter is set correctly.
  unique_ptr<RowwiseIterator> iter;
  ASSERT_OK(tablet()->NewRowIterator(schema_.CopyWithoutColumnIds(), &iter));
  ASSERT_OK(iter->Init(nullptr));
  vector<string> out;
  IterateToStringList(iter.get(), &out);
  for (int i = 0; i < 10; i++) {
    ASSERT_STR_CONTAINS(out[i], Substitute("int64 key=$0", i + 1));
  }
}


TEST_F(AutoIncrementingTabletTest, TestUpsertOp) {
  // Insert 20 rows with auto-incrementing column populated into an empty tablet
  // and validate the data written using Upsert() and UpsertIgnore().

  {
    unique_ptr<KuduPartialRow> row(new KuduPartialRow(&client_schema_));
    for (int i = 1; i <= 10; i++) {
      ASSERT_OK(row->SetInt64(0, i));
      ASSERT_OK(row->SetInt32(1, 1337));
      ASSERT_OK(writer_->Upsert(*row));
    }
    for (int i = 10; i <= 20; i++) {
      ASSERT_OK(row->SetInt64(0, i));
      ASSERT_OK(row->SetInt32(1, 1337));
      ASSERT_OK(writer_->UpsertIgnore(*row));
    }
    unique_ptr<RowwiseIterator> iter;
    ASSERT_OK(tablet()->NewRowIterator(schema_.CopyWithoutColumnIds(), &iter));
    ASSERT_OK(iter->Init(nullptr));
    vector<string> out;
    IterateToStringList(iter.get(), &out);
    for (int i = 1; i <= 20; i++) {
      ASSERT_STR_MATCHES(out[i - 1], Substitute("(int64 key=$0, int32 val=1337)", i));
    }
  }
  // Update the same 20 rows with auto-incrementing column populated but with different
  // non-primary key column data and validate the data written using Upsert() and UpsertIgnore().
  {
    unique_ptr<KuduPartialRow> row(new KuduPartialRow(&client_schema_));
    for (int i = 1; i <= 10; i++) {
      ASSERT_OK(row->SetInt64(0, i));
      ASSERT_OK(row->SetInt32(1, 1338));
      ASSERT_OK(writer_->Upsert(*row));
    }
    for (int i = 10; i <= 20; i++) {
      ASSERT_OK(row->SetInt64(0, i));
      ASSERT_OK(row->SetInt32(1, 1338));
      ASSERT_OK(writer_->UpsertIgnore(*row));
    }
    unique_ptr<RowwiseIterator> iter;
    ASSERT_OK(tablet()->NewRowIterator(schema_.CopyWithoutColumnIds(), &iter));
    ASSERT_OK(iter->Init(nullptr));
    vector<string> out;
    IterateToStringList(iter.get(), &out);
    for (int i = 1; i <= 20; i++) {
      ASSERT_STR_MATCHES(out[i - 1], Substitute("(int64 key=$0, int32 val=1338)", i));
    }
  }
  // Now insert new set of rows and validate the data written.
  {
    unique_ptr<KuduPartialRow> row(new KuduPartialRow(&client_schema_));
    for (int i = 20; i <= 30; i++) {
      ASSERT_OK(row->SetInt32(1, 1337));
      ASSERT_OK(writer_->Insert(*row));
    }

    // Scan the tablet data and verify the auto increment counter is set correctly.
    unique_ptr<RowwiseIterator> iter;
    ASSERT_OK(tablet()->NewRowIterator(schema_.CopyWithoutColumnIds(), &iter));
    ASSERT_OK(iter->Init(nullptr));
    vector<string> out;
    IterateToStringList(iter.get(), &out);
    for (int i = 21; i <= 30; i++) {
      ASSERT_STR_MATCHES(out[i-1], Substitute("int64 key=$0, int32 val=1337", i));
    }
  }
}

TEST_F(AutoIncrementingTabletTest, TestNegatives) {
  // Insert a row into the tablet setting auto-incrementing column.
  {
    unique_ptr<KuduPartialRow> row(new KuduPartialRow(&client_schema_));
    ASSERT_OK(row->SetInt64(0, 10));
    ASSERT_OK(row->SetInt32(1, 1337));
    Status s = writer_->Insert(*row);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_EQ("Invalid argument: auto-incrementing column should not be set "
              "for INSERT/INSERT_IGNORE operations", s.ToString());
  }

  // Upsert a row without auto-incrementing column set
  {
    unique_ptr<KuduPartialRow> row(new KuduPartialRow(&client_schema_));
    ASSERT_OK(row->SetInt32(1, 1337));
    Status s = writer_->Upsert(*row);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_EQ("Invalid argument: auto-incrementing column should be set "
              "for UPSERT/UPSERT_IGNORE operations", s.ToString());
  }

  // Upsert a row with auto-incrementing set to negative value
  {
    unique_ptr<KuduPartialRow> row(new KuduPartialRow(&client_schema_));
    ASSERT_OK(row->SetInt64(0, -1));
    ASSERT_OK(row->SetInt32(1, 1337));
    Status s = writer_->Upsert(*row);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_EQ("Invalid argument: auto-incrementing column value must be greater than zero",
              s.ToString());
  }
  // Upsert a row with auto-incrementing set to INT64_MAX and insert a row
  {
    unique_ptr<KuduPartialRow> row(new KuduPartialRow(&client_schema_));
    ASSERT_OK(row->SetInt64(0, INT64_MAX));
    ASSERT_OK(row->SetInt32(1, 1337));
    ASSERT_OK(writer_->Upsert(*row));
  }
  {
    unique_ptr<KuduPartialRow> row(new KuduPartialRow(&client_schema_));
    ASSERT_OK(row->SetInt32(1, 1337));
    Status s = writer_->Insert(*row);
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
    ASSERT_EQ("Illegal state: max auto-incrementing column value reached",
              s.ToString());
  }

  // Upsert rows with auto-incrementing set to INT64_MAX and a new value respectively.
  // Insert a new row and later upsert a row with INT64_MAX.
  // Note: Technically, since we are operating on the same table as above, UPSERT with
  // Auto-incrementing column value set to INT64_MAX is not needed as we have already
  // done it above, but having it enhances the readability of the sub-test.
  {
    unique_ptr<KuduPartialRow> row(new KuduPartialRow(&client_schema_));
    ASSERT_OK(row->SetInt64(0, INT64_MAX));
    ASSERT_OK(row->SetInt32(1, 1337));
    ASSERT_OK(writer_->Upsert(*row));
  }
  {
    unique_ptr<KuduPartialRow> row(new KuduPartialRow(&client_schema_));
    ASSERT_OK(row->SetInt64(0, 100));
    ASSERT_OK(row->SetInt32(1, 1337));
    ASSERT_OK(writer_->Upsert(*row));
  }
  {
    unique_ptr<KuduPartialRow> row(new KuduPartialRow(&client_schema_));
    ASSERT_OK(row->SetInt32(1, 1337));
    Status s = writer_->Insert(*row);
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
    ASSERT_EQ("Illegal state: max auto-incrementing column value reached",
              s.ToString());
  }
  {
    unique_ptr<KuduPartialRow> row(new KuduPartialRow(&client_schema_));
    ASSERT_OK(row->SetInt64(0, INT64_MAX));
    ASSERT_OK(row->SetInt32(1, 1338));
    ASSERT_OK(writer_->Upsert(*row));
  }
}

} // namespace tablet
} // namespace kudu
