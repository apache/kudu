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

#include <functional>

#include <gtest/gtest.h>

#include "kudu/common/partial_row.h"
#include "kudu/common/row.h"
#include "kudu/common/schema.h"
#include "kudu/util/test_util.h"

using std::string;

namespace kudu {

class PartialRowTest : public KuduTest {
 public:
  PartialRowTest()
    : schema_({ ColumnSchema("key", INT32),
                ColumnSchema("int_val", INT32),
                ColumnSchema("string_val", STRING, true),
                ColumnSchema("binary_val", BINARY, true) },
              1) {
    SeedRandom();
  }

 protected:
  // A couple of typedefs to facilitate transformation of KuduPartialRow member
  // function pointers into std::function<...> wrappers.
  //
  // The typedefs and explicit casting below via static_cast<>
  // would not be necessary if the
  // KuduPartialRow::Set{Binary,String}{,Copy,NoCopy}() and
  // KuduPartialRow::Get{Binary,String}() methods had no overloaded
  // counterparts for column name-based and index-based operations.
  typedef Status (KuduPartialRow::*BinarySetter)(int, const Slice&);
  typedef Status (KuduPartialRow::*BinaryGetter)(int, Slice*) const;

  // Expected behavior of the
  // KuduPartialRow::Set{Binary,String}{,Copy,NoCopy}() methods:
  // whether the source data is copied or not.
  enum CopyBehavior {
    COPY,
    NO_COPY,
  };

  Schema schema_;

  // Utility method to perform checks on copy/no-copy behavior of the
  // PartialRow::Set{Binary,String}{,Copy,NoCopy}() methods.
  void BinaryDataSetterTest(
      std::function<Status(const KuduPartialRow&, int, Slice*)> getter,
      std::function<Status(KuduPartialRow&, int, const Slice&)> setter,
      int column_idx, CopyBehavior copy_behavior) {

    KuduPartialRow row(&schema_);
    string src_data = "src-data";
    ASSERT_OK(setter(row, column_idx, src_data));

    Slice column_slice;
    ASSERT_OK(getter(row, column_idx, &column_slice));

    // Check that the row's column contains the right data.
    EXPECT_EQ("src-data", column_slice.ToString());

    switch (copy_behavior) {
      case COPY:
        // Check that the row keeps an independent copy of the source data.
        EXPECT_NE(reinterpret_cast<uintptr_t>(src_data.data()),
                  reinterpret_cast<uintptr_t>(column_slice.data()));
        break;
      case NO_COPY:
        // Check that the row keeps a reference to the source data.
        EXPECT_EQ(reinterpret_cast<uintptr_t>(src_data.data()),
                  reinterpret_cast<uintptr_t>(column_slice.data()));
        break;
      default:
        ASSERT_TRUE(false) << "unexpected copy behavior specified";
        break;  // unreachable
    }

    // Additional, more high-level check.
    src_data.replace(0, src_data.find("-"), "new");
    ASSERT_EQ("new-data", src_data);

    switch (copy_behavior) {
      case COPY:
        EXPECT_EQ("src-data", column_slice.ToString());
        break;
      case NO_COPY:
        EXPECT_EQ("new-data", column_slice.ToString());
        break;
      default:
        ASSERT_TRUE(false) << "unexpected copy behavior specified";
        break;  // unreachable
    }
  }
};

TEST_F(PartialRowTest, UnitTest) {
  KuduPartialRow row(&schema_);
  string enc_key;

  // Initially all columns are unset.
  EXPECT_FALSE(row.IsColumnSet(0));
  EXPECT_FALSE(row.IsColumnSet(1));
  EXPECT_FALSE(row.IsColumnSet(2));
  EXPECT_FALSE(row.IsKeySet());
  EXPECT_EQ("", row.ToString());

  // Encoding the key when it is not set should give an error.
  EXPECT_EQ("Invalid argument: All key columns must be set: key",
            row.EncodeRowKey(&enc_key).ToString());

  // Set just the key.
  EXPECT_OK(row.SetInt32("key", 12345));
  EXPECT_TRUE(row.IsKeySet());
  EXPECT_FALSE(row.IsColumnSet(1));
  EXPECT_FALSE(row.IsColumnSet(2));
  EXPECT_EQ("int32 key=12345", row.ToString());
  int32_t x;
  EXPECT_OK(row.GetInt32("key", &x));
  EXPECT_EQ(12345, x);
  EXPECT_FALSE(row.IsNull("key"));

  // Test key encoding.
  EXPECT_EQ("OK", row.EncodeRowKey(&enc_key).ToString());
  EXPECT_EQ("\\x80\\x0009", Slice(enc_key).ToDebugString());

  // Fill in the other columns.
  EXPECT_OK(row.SetInt32("int_val", 54321));
  EXPECT_OK(row.SetStringCopy("string_val", "hello world"));
  EXPECT_TRUE(row.IsColumnSet(1));
  EXPECT_TRUE(row.IsColumnSet(2));
  EXPECT_EQ(R"(int32 key=12345, int32 int_val=54321, string string_val="hello world")",
            row.ToString());
  Slice slice;
  EXPECT_OK(row.GetString("string_val", &slice));
  EXPECT_EQ("hello world", slice.ToString());
  EXPECT_FALSE(row.IsNull("key"));

  // Set a nullable entry to NULL
  EXPECT_OK(row.SetNull("string_val"));
  EXPECT_EQ("int32 key=12345, int32 int_val=54321, string string_val=NULL",
            row.ToString());
  EXPECT_TRUE(row.IsNull("string_val"));

  // Try to set an entry with the wrong type
  Status s = row.SetStringCopy("int_val", "foo");
  EXPECT_EQ("Invalid argument: invalid type string provided for column 'int_val' (expected int32)",
            s.ToString());

  // Try to get an entry with the wrong type
  s = row.GetString("int_val", &slice);
  EXPECT_EQ("Invalid argument: invalid type string provided for column 'int_val' (expected int32)",
            s.ToString());

  // Try to set a non-nullable entry to NULL
  s = row.SetNull("key");
  EXPECT_EQ("Invalid argument: column not nullable: key[int32 NOT NULL]", s.ToString());

  // Set the NULL string back to non-NULL
  EXPECT_OK(row.SetStringCopy("string_val", "goodbye world"));
  EXPECT_EQ(R"(int32 key=12345, int32 int_val=54321, string string_val="goodbye world")",
            row.ToString());

  // Unset some columns.
  EXPECT_OK(row.Unset("string_val"));
  EXPECT_EQ("int32 key=12345, int32 int_val=54321", row.ToString());

  EXPECT_OK(row.Unset("key"));
  EXPECT_EQ("int32 int_val=54321", row.ToString());

  // Set the column by index
  EXPECT_OK(row.SetInt32(1, 99999));
  EXPECT_EQ("int32 int_val=99999", row.ToString());

  // Set the binary column as a copy.
  EXPECT_OK(row.SetBinaryCopy("binary_val", "hello_world"));
  EXPECT_EQ(R"(int32 int_val=99999, binary binary_val="hello_world")",
              row.ToString());
  // Unset the binary column.
  EXPECT_OK(row.Unset("binary_val"));
  EXPECT_EQ("int32 int_val=99999", row.ToString());

  // Even though the storage is actually the same at the moment, we shouldn't be
  // able to set string columns with SetBinary and vice versa.
  EXPECT_FALSE(row.SetBinaryCopy("string_val", "oops").ok());
  EXPECT_FALSE(row.SetStringCopy("binary_val", "oops").ok());
}

TEST_F(PartialRowTest, TestCopy) {
  KuduPartialRow row(&schema_);

  // The assignment operator is used in this test because it internally calls
  // the copy constructor.

  // Check an empty copy.
  KuduPartialRow copy = row;
  EXPECT_FALSE(copy.IsColumnSet(0));
  EXPECT_FALSE(copy.IsColumnSet(1));
  EXPECT_FALSE(copy.IsColumnSet(2));

  ASSERT_OK(row.SetInt32(0, 42));
  ASSERT_OK(row.SetInt32(1, 99));
  ASSERT_OK(row.SetStringCopy(2, "copied-string"));

  int32_t int_val;
  Slice string_val;
  Slice binary_val;

  // Check a copy with values.
  copy = row;
  ASSERT_OK(copy.GetInt32(0, &int_val));
  EXPECT_EQ(42, int_val);
  ASSERT_OK(copy.GetInt32(1, &int_val));
  EXPECT_EQ(99, int_val);
  ASSERT_OK(copy.GetString(2, &string_val));
  EXPECT_EQ("copied-string", string_val.ToString());

  // Check a copy with a null value.
  ASSERT_OK(row.SetNull(2));
  copy = row;
  EXPECT_TRUE(copy.IsNull(2));

  // Check a copy with a borrowed value.
  string borrowed_string = "borrowed-string";
  string borrowed_binary = "borrowed-binary";
  ASSERT_OK(row.SetStringNoCopy(2, borrowed_string));
  ASSERT_OK(row.SetBinaryNoCopy(3, borrowed_binary));

  copy = row;
  ASSERT_OK(copy.GetString(2, &string_val));
  EXPECT_EQ("borrowed-string", string_val.ToString());
  ASSERT_OK(copy.GetBinary(3, &binary_val));
  EXPECT_EQ("borrowed-binary", binary_val.ToString());

  borrowed_string.replace(0, 8, "mutated-");
  borrowed_binary.replace(0, 8, "mutated-");
  ASSERT_OK(copy.GetString(2, &string_val));
  EXPECT_EQ("mutated--string", string_val.ToString());
  ASSERT_OK(copy.GetBinary(3, &string_val));
  EXPECT_EQ("mutated--binary", string_val.ToString());
}

// Check that PartialRow::SetBinaryCopy() copies the input data.
TEST_F(PartialRowTest, TestSetBinaryCopy) {
  BinaryDataSetterTest(
      static_cast<BinaryGetter>(&KuduPartialRow::GetBinary),
      static_cast<BinarySetter>(&KuduPartialRow::SetBinaryCopy),
      3, COPY);
}

// Check that PartialRow::SetStringCopy() copies the input data.
TEST_F(PartialRowTest, TestSetStringCopy) {
  BinaryDataSetterTest(
      static_cast<BinaryGetter>(&KuduPartialRow::GetString),
      static_cast<BinarySetter>(&KuduPartialRow::SetStringCopy),
      2, COPY);
}

// Check that PartialRow::SetBinaryNoCopy() does not copy the input data.
TEST_F(PartialRowTest, TestSetBinaryNoCopy) {
  BinaryDataSetterTest(
      static_cast<BinaryGetter>(&KuduPartialRow::GetBinary),
      static_cast<BinarySetter>(&KuduPartialRow::SetBinaryNoCopy),
      3, NO_COPY);
}

// Check that PartialRow::SetStringNoCopy() does not copy the input data.
TEST_F(PartialRowTest, TestSetStringNoCopy) {
  BinaryDataSetterTest(
      static_cast<BinaryGetter>(&KuduPartialRow::GetString),
      static_cast<BinarySetter>(&KuduPartialRow::SetStringNoCopy),
      2, NO_COPY);
}

// Check that PartialRow::SetBinary() copies the input data.
TEST_F(PartialRowTest, TestSetBinary) {
  BinaryDataSetterTest(
      static_cast<BinaryGetter>(&KuduPartialRow::GetBinary),
      static_cast<BinarySetter>(&KuduPartialRow::SetBinary),
      3, COPY);
}

// Check that PartialRow::SetString() copies the input data.
TEST_F(PartialRowTest, TestSetString) {
  BinaryDataSetterTest(
      static_cast<BinaryGetter>(&KuduPartialRow::GetString),
      static_cast<BinarySetter>(&KuduPartialRow::SetString),
      2, COPY);
}

} // namespace kudu
