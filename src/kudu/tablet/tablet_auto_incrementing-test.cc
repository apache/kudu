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

TEST_F(AutoIncrementingTabletTest, TestInsertOpWithAutoIncrementSet) {
  // Insert a row into the tablet populating auto-incrementing column.
  unique_ptr<KuduPartialRow> row(new KuduPartialRow(&client_schema_));
  ASSERT_OK(row->SetInt64(0, 10));
  ASSERT_OK(row->SetInt32(1, 1337));
  Status s = writer_->Insert(*row);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_EQ("Invalid argument: auto-incrementing column is incorrectly set", s.ToString());
}

TEST_F(AutoIncrementingTabletTest, TestUpsertOp) {
  // Insert a row into the tablet populating only non auto-incrementing columns
  // using UPSERT.
  unique_ptr<KuduPartialRow> row(new KuduPartialRow(&client_schema_));
  ASSERT_OK(row->SetInt32(1, 1337));
  Status s = writer_->Upsert(*row);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  ASSERT_EQ("Not implemented: tables with auto-incrementing "
            "column do not support UPSERT operations", s.ToString());
}

TEST_F(AutoIncrementingTabletTest, TestUpsertIgnoreOp) {
  // Insert a row into the tablet populating only non auto-incrementing columns
  // using UPSERT_IGNORE.
  unique_ptr<KuduPartialRow> row(new KuduPartialRow(&client_schema_));
  ASSERT_OK(row->SetInt32(1, 1337));
  Status s = writer_->UpsertIgnore(*row);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  ASSERT_EQ("Not implemented: tables with auto-incrementing "
            "column do not support UPSERT_IGNORE operations", s.ToString());
}

} // namespace tablet
} // namespace kudu
