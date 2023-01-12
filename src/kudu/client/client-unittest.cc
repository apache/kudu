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
//
// Tests for the client which are true unit tests and don't require a cluster, etc.

#include "kudu/client/client.h"

#include <openssl/crypto.h>

#include <cstddef>
#include <functional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/client/client-internal.h"
#include "kudu/client/error_collector.h"
#include "kudu/client/schema.h"
#include "kudu/client/value.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using kudu::client::internal::ErrorCollector;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace client {

TEST(ClientUnitTest, TestSchemaBuilder_EmptySchema) {
  KuduSchema s;
  KuduSchemaBuilder b;
  ASSERT_EQ("Invalid argument: no primary key or non-unique primary key specified",
            b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestSchemaBuilder_KeyNotSpecified) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NotNull();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32)->NotNull();
  ASSERT_EQ("Invalid argument: no primary key or non-unique primary key specified",
            b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestSchemaBuilder_DuplicateColumn) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b.AddColumn("x")->Type(KuduColumnSchema::INT32);
  b.AddColumn("x")->Type(KuduColumnSchema::INT32);
  ASSERT_EQ("Invalid argument: Duplicate column name: x", b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestSchemaBuilder_KeyAndNonUniqueKeyOnSameColumn) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey()->NonUniquePrimaryKey();
  ASSERT_OK(b.Build(&s));
  ASSERT_EQ(2, s.num_columns());
  ASSERT_EQ(1, s.GetAutoIncrementingColumnIndex());
}

TEST(ClientUnitTest, TestSchemaBuilder_NonUniqueKeyAndKeyOnSameColumn) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->NonUniquePrimaryKey()->PrimaryKey();
  ASSERT_OK(b.Build(&s));
  ASSERT_EQ(1, s.num_columns());
  ASSERT_EQ(-1, s.GetAutoIncrementingColumnIndex());
}

TEST(ClientUnitTest, TestSchemaBuilder_KeyNotFirstColumn) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32);
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32);
  ASSERT_EQ("Invalid argument: primary key column must be the first column",
            b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestSchemaBuilder_NonUniqueKeyNotFirstColumn) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32);
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NotNull()->NonUniquePrimaryKey();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32);
  ASSERT_EQ("Invalid argument: primary key column must be the first column",
            b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestSchemaBuilder_TwoPrimaryKeys) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->PrimaryKey();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32)->PrimaryKey();
  ASSERT_EQ("Invalid argument: multiple columns specified for primary key: a, b",
            b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestSchemaBuilder_PrimaryKeyAndNonUniquePrimaryKey) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->PrimaryKey();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32)->NonUniquePrimaryKey();
  ASSERT_EQ("Invalid argument: multiple columns specified for primary key: a, b",
            b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestSchemaBuilder_TwoNonUniquePrimaryKeys) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NonUniquePrimaryKey();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32)->NonUniquePrimaryKey();
  ASSERT_EQ("Invalid argument: multiple columns specified for primary key: a, b",
            b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestSchemaBuilder_PrimaryKeyOnColumnAndSetPrimaryKey) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->PrimaryKey();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32);
  b.SetPrimaryKey({"a", "b"});
  ASSERT_EQ(
      "Invalid argument: primary key specified by both "
      "SetPrimaryKey() and on a specific column: a",
      b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestSchemaBuilder_PrimaryKeyOnColumnAndSetNonUniquePrimaryKey) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->PrimaryKey();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32);
  b.SetNonUniquePrimaryKey({"a", "b"});
  ASSERT_EQ(
      "Invalid argument: primary key specified by both "
      "SetNonUniquePrimaryKey() and on a specific column: a",
      b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestSchemaBuilder_NonUniquePrimaryKeyOnColumnAndSetPrimaryKey) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NonUniquePrimaryKey();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32);
  b.SetPrimaryKey({"a", "b"});
  ASSERT_EQ(
      "Invalid argument: primary key specified by both "
      "SetPrimaryKey() and on a specific column: a",
      b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestSchemaBuilder_NonUniquePrimaryKeyOnColumnAndSetNonUniquePrimaryKey) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NonUniquePrimaryKey();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32);
  b.SetNonUniquePrimaryKey({"a", "b"});
  ASSERT_EQ(
      "Invalid argument: primary key specified by both "
      "SetNonUniquePrimaryKey() and on a specific column: a",
      b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestSchemaBuilder_SingleKey_GoodSchema) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32);
  b.AddColumn("c")->Type(KuduColumnSchema::INT32)->NotNull();
  ASSERT_OK(b.Build(&s));
}

TEST(ClientUnitTest, TestSchemaBuilder_SingleNonUniqueKey_GoodSchema) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NotNull()->NonUniquePrimaryKey();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32);
  b.AddColumn("c")->Type(KuduColumnSchema::INT32)->NotNull();
  ASSERT_OK(b.Build(&s));
  ASSERT_EQ(4, s.num_columns());
  ASSERT_EQ(1, s.GetAutoIncrementingColumnIndex());
}

TEST(ClientUnitTest, TestSchemaBuilder_CompoundKey_GoodSchema) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NotNull();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32)->NotNull();
  b.SetPrimaryKey({"a", "b"});
  ASSERT_OK(b.Build(&s));

  vector<int> key_columns;
  s.GetPrimaryKeyColumnIndexes(&key_columns);
  ASSERT_EQ(vector<int>({0, 1}), key_columns);
  ASSERT_EQ(-1, s.GetAutoIncrementingColumnIndex());
}

TEST(ClientUnitTest, TestSchemaBuilder_CompoundNonUniqueKey_GoodSchema) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NotNull();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32)->NotNull();
  b.SetNonUniquePrimaryKey({"a", "b"});
  ASSERT_OK(b.Build(&s));

  vector<int> key_columns;
  s.GetPrimaryKeyColumnIndexes(&key_columns);
  ASSERT_EQ(vector<int>({0, 1, 2}), key_columns);
  ASSERT_EQ(2, s.GetAutoIncrementingColumnIndex());
}

TEST(ClientUnitTest, TestSchemaBuilder_UniquePrimaryAndNonUniquePrimaryCompound) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NotNull();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32)->NotNull();
  b.SetPrimaryKey({"a", "b"});
  b.SetNonUniquePrimaryKey({"a", "b"});
  ASSERT_OK(b.Build(&s));

  vector<int> key_columns;
  s.GetPrimaryKeyColumnIndexes(&key_columns);
  ASSERT_EQ(vector<int>({0, 1, 2}), key_columns);
  ASSERT_EQ(2, s.GetAutoIncrementingColumnIndex());
}

TEST(ClientUnitTest, TestSchemaBuilder_NonUniqueAndUniquePrimaryCompound) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NotNull();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32)->NotNull();
  b.SetNonUniquePrimaryKey({"a", "b"});
  b.SetPrimaryKey({"a", "b"});
  ASSERT_OK(b.Build(&s));
  ASSERT_EQ(-1, s.GetAutoIncrementingColumnIndex());

  vector<int> key_columns;
  s.GetPrimaryKeyColumnIndexes(&key_columns);
  ASSERT_EQ(vector<int>({0, 1}), key_columns);
  ASSERT_EQ(-1, s.GetAutoIncrementingColumnIndex());
}

TEST(ClientUnitTest, TestSchemaBuilder_KeyColumnWithAutoIncrementingReservedName) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn(Schema::GetAutoIncrementingColumnName())
      ->Type(KuduColumnSchema::INT64)
      ->NotNull()
      ->PrimaryKey();
  ASSERT_EQ(Substitute("Invalid argument: $0 is a reserved column name",
                       Schema::GetAutoIncrementingColumnName()),
            b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestSchemaBuilder_NonUniqueKeyColumnWithAutoIncrementingReservedName) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn(Schema::GetAutoIncrementingColumnName())
      ->Type(KuduColumnSchema::INT64)
      ->NotNull()
      ->NonUniquePrimaryKey();
  ASSERT_EQ(Substitute("Invalid argument: $0 is a reserved column name",
                       Schema::GetAutoIncrementingColumnName()),
            b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestSchemaBuilder_ColumnWithAutoIncrementingReservedName) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b.AddColumn(Schema::GetAutoIncrementingColumnName())
      ->Type(KuduColumnSchema::INT64)
      ->NotNull();
  ASSERT_EQ(Substitute("Invalid argument: $0 is a reserved column name",
                       Schema::GetAutoIncrementingColumnName()),
            b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestSchemaBuilder_DefaultValues) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32)->NotNull()->Default(KuduValue::FromInt(12345));
  ASSERT_OK(b.Build(&s));
}

TEST(ClientUnitTest, TestSchemaBuilder_DefaultValueString) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b.AddColumn("b")
      ->Type(KuduColumnSchema::STRING)
      ->NotNull()
      ->Default(KuduValue::CopyString("abc"));
  b.AddColumn("c")
      ->Type(KuduColumnSchema::BINARY)
      ->NotNull()
      ->Default(KuduValue::CopyString("def"));
  ASSERT_OK(b.Build(&s));
}

TEST(ClientUnitTest, TestSchemaBuilder_CompoundKey_KeyNotFirst) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("x")->Type(KuduColumnSchema::INT32)->NotNull();
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NotNull();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32)->NotNull();
  b.SetPrimaryKey({"a", "b"});
  ASSERT_EQ(
      "Invalid argument: primary key columns must be listed "
      "first in the schema: a",
      b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestSchemaBuilder_CompoundNonUniqueKey_NonUniqueKeyNotFirst) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("x")->Type(KuduColumnSchema::INT32)->NotNull();
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NotNull();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32)->NotNull();
  b.SetNonUniquePrimaryKey({"a", "b"});
  ASSERT_EQ("Invalid argument: primary key columns must be listed first in the schema: a",
            b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestSchemaBuilder_CompoundKey_BadColumnName) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NotNull();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32)->NotNull();
  b.SetPrimaryKey({"foo"});
  ASSERT_EQ("Invalid argument: primary key column not defined: foo", b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestSchemaBuilder_CompoundNonUniqueKey_BadColumnName) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NotNull();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32)->NotNull();
  b.SetNonUniquePrimaryKey({"foo"});
  ASSERT_EQ("Invalid argument: primary key column not defined: foo", b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestDisableSslFailsIfNotInitialized) {
  const auto s = DisableOpenSSLInitialization();
#if OPENSSL_VERSION_NUMBER < 0x10100000L
  // With the pre-1.1.0 OpenSSL library, if we try to disable SSL
  // initialization without setting up SSL properly, it should return an error.
  ASSERT_STR_MATCHES(s.ToString(), "Locking callback not initialized");
#else
  // Starting with OpenSSL 1.1.0, the library can be implicitly initialized
  // upon calling the relevant methods of the API (e.g. SSL_CTX_new()) and
  // overall there is no reliable non-intrusive way to determine that the
  // library has already been initialized. So, the requirement to have
  // the library initialized before calling DisableOpenSSLInitialization()
  // is gone since OpenSSL 1.1.0.
  ASSERT_TRUE(s.ok()) << s.ToString();
#endif
}

namespace {
Status TestFunc(const MonoTime& deadline, bool* retry, int* counter) {
  (*counter)++;
  *retry = true;
  return Status::RuntimeError("x");
}
} // anonymous namespace

TEST(ClientUnitTest, TestRetryFunc) {
  MonoTime deadline = MonoTime::Now() + MonoDelta::FromMilliseconds(100);
  int counter = 0;
  Status s = RetryFunc(deadline, "retrying test func", "timed out",
                       [&](const MonoTime& deadline, bool* retry) {
                         return TestFunc(deadline, retry, &counter);
                       });
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
  ASSERT_GT(counter, 5);
  ASSERT_LT(counter, 20);
}

TEST(ClientUnitTest, TestErrorCollector) {
  {
    scoped_refptr<ErrorCollector> ec(new ErrorCollector);
    // Setting the max memory size limit to 'unlimited'.
    EXPECT_OK(ec->SetMaxMemSize(0));
    // Setting the max memory size to 1 byte.
    EXPECT_OK(ec->SetMaxMemSize(1));
  }

  // Check that the error collector does not allow to set the memory size limit
  // if at least one error has been dropped since last flush.
  {
    scoped_refptr<ErrorCollector> ec(new ErrorCollector);
    ec->dropped_errors_cnt_ = 1;
    Status s = ec->SetMaxMemSize(0);
    EXPECT_TRUE(s.IsIllegalState());
    ASSERT_STR_CONTAINS(s.ToString(),
                        "cannot set new limit: already dropped some errors");
  }

  // Check that the error collector does not overflow post-factum on call of the
  // SetMaxMemSize() method.
  {
    const size_t size_bytes = 8;
    scoped_refptr<ErrorCollector> ec(new ErrorCollector);
    ec->mem_size_bytes_ = size_bytes;
    EXPECT_OK(ec->SetMaxMemSize(0));
    EXPECT_OK(ec->SetMaxMemSize(size_bytes));
    Status s = ec->SetMaxMemSize(size_bytes - 1);
    EXPECT_TRUE(s.IsIllegalState());
    ASSERT_STR_CONTAINS(s.ToString(), "already accumulated errors for");
  }
}

TEST(KuduSchemaTest, TestToString_OneUniquePrimaryKey) {
  // Test on unique PK.
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b.AddColumn("int_val")->Type(KuduColumnSchema::INT32)->NotNull();
  b.AddColumn("string_val")->Type(KuduColumnSchema::STRING)->Nullable();
  b.AddColumn("non_null_with_default")
      ->Type(KuduColumnSchema::INT32)
      ->NotNull()
      ->Default(KuduValue::FromInt(12345));
  ASSERT_OK(b.Build(&s));

  string schema_str =
      "(\n"
      "    key INT32 NOT NULL,\n"
      "    int_val INT32 NOT NULL,\n"
      "    string_val STRING NULLABLE,\n"
      "    non_null_with_default INT32 NOT NULL,\n"
      "    PRIMARY KEY (key)\n"
      ")";
  EXPECT_EQ(schema_str, s.ToString());
}

TEST(KuduSchemaTest, TestToString_OneNonUniquePrimaryKey) {
  // Test on non-unique PK.
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->NonUniquePrimaryKey();
  b.AddColumn("int_val")->Type(KuduColumnSchema::INT32)->NotNull();
  ASSERT_OK(b.Build(&s));

  string schema_str = Substitute(
      "(\n"
      "    key INT32 NOT NULL,\n"
      "    $0 INT64 NOT NULL,\n"
      "    int_val INT32 NOT NULL,\n"
      "    PRIMARY KEY (key, $0)\n"
      ")",
      KuduSchema::GetAutoIncrementingColumnName());
  EXPECT_EQ(schema_str, s.ToString());
}

TEST(KuduSchemaTest, TestToString_EmptySchem) {
  // Test empty schema.
  KuduSchema s;
  EXPECT_EQ("()", s.ToString());
}

TEST(KuduSchemaTest, TestToString_CompositePrimaryKey) {
  // Test on composite PK.
  // Create a different schema with a multi-column PK.
  KuduSchemaBuilder b;
  KuduSchema s;
  b.AddColumn("k1")->Type(KuduColumnSchema::INT32)->NotNull();
  b.AddColumn("k2")->Type(KuduColumnSchema::UNIXTIME_MICROS)->NotNull();
  b.AddColumn("k3")->Type(KuduColumnSchema::INT8)->NotNull();
  b.AddColumn("date_val")->Type(KuduColumnSchema::DATE)->NotNull();
  b.AddColumn("dec_val")->Type(KuduColumnSchema::DECIMAL)->Nullable()->Precision(9)->Scale(2);
  b.AddColumn("int_val")->Type(KuduColumnSchema::INT32)->NotNull();
  b.AddColumn("string_val")->Type(KuduColumnSchema::STRING)->Nullable();
  b.AddColumn("non_null_with_default")
      ->Type(KuduColumnSchema::INT32)
      ->NotNull()
      ->Default(KuduValue::FromInt(12345));
  b.SetPrimaryKey({"k1", "k2", "k3"});
  ASSERT_OK(b.Build(&s));

  string schema_str =
      "(\n"
      "    k1 INT32 NOT NULL,\n"
      "    k2 UNIXTIME_MICROS NOT NULL,\n"
      "    k3 INT8 NOT NULL,\n"
      "    date_val DATE NOT NULL,\n"
      "    dec_val DECIMAL(9, 2) NULLABLE,\n"
      "    int_val INT32 NOT NULL,\n"
      "    string_val STRING NULLABLE,\n"
      "    non_null_with_default INT32 NOT NULL,\n"
      "    PRIMARY KEY (k1, k2, k3)\n"
      ")";
  EXPECT_EQ(schema_str, s.ToString());
}

TEST(KuduSchemaTest, TestEquals) {
  KuduSchemaBuilder b1;
  b1.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b1.AddColumn("int_val")->Type(KuduColumnSchema::INT32);

  KuduSchema s1;
  ASSERT_OK(b1.Build(&s1));

  ASSERT_FALSE(s1 != s1);
  ASSERT_TRUE(s1 == s1);
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
  ASSERT_TRUE(s1.Equals(s1));
#pragma GCC diagnostic pop

  KuduSchema s2;
  ASSERT_OK(b1.Build(&s2));

  ASSERT_FALSE(s1 != s2);
  ASSERT_TRUE(s1 == s2);
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
  ASSERT_TRUE(s1.Equals(s2));
#pragma GCC diagnostic pop

  KuduSchemaBuilder b2;
  b2.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b2.AddColumn("string_val")->Type(KuduColumnSchema::STRING);

  KuduSchema s3;
  ASSERT_OK(b2.Build(&s3));

  ASSERT_TRUE(s1 != s3);
  ASSERT_FALSE(s1 == s3);
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
  ASSERT_FALSE(s1.Equals(s3));
#pragma GCC diagnostic pop
}

TEST(ClientUnitTest, TestKuduSchemaToStringWithColumnIds) {
  // Build a KuduSchema from a Schema, so that the KuduSchema's internal Schema
  // has column ids.
  SchemaBuilder builder;
  builder.AddKeyColumn("key", DataType::INT32);
  const auto schema = builder.Build();
  const auto kudu_schema = KuduSchema::FromSchema(schema);

  // The string version of the KuduSchema should not have column ids, even
  // though the default string version of the underlying Schema should.
  EXPECT_EQ(
      Substitute("(\n"
                 "    $0:key INT32 NOT NULL,\n"
                 "    PRIMARY KEY (key)\n"
                 ")",
                 schema.column_id(0)),
      schema.ToString());
  EXPECT_EQ("(\n"
            "    key INT32 NOT NULL,\n"
            "    PRIMARY KEY (key)\n"
            ")",
            kudu_schema.ToString());
}

TEST(KuduColumnSchemaTest, TestEquals) {
  KuduColumnSchema a32("a", KuduColumnSchema::INT32);
  ASSERT_TRUE(a32 == a32);

  KuduColumnSchema a32_2(a32);
  ASSERT_EQ(a32, a32_2);

  KuduColumnSchema b32("b", KuduColumnSchema::INT32);
  ASSERT_FALSE(a32 == b32);
  ASSERT_TRUE(a32 != b32);

  KuduColumnSchema a16("a", KuduColumnSchema::INT16);
  ASSERT_NE(a32, a16);

  const int kDefaultOf7 = 7;
  KuduColumnSchema a32_dflt("a", KuduColumnSchema::INT32, /*is_nullable=*/false,
                            /*is_immutable=*/false, /*is_auto_incrementing=*/false,
                            /*default_value=*/&kDefaultOf7);
  ASSERT_NE(a32, a32_dflt);
}

} // namespace client
} // namespace kudu
