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

#include <cstddef>
#include <string>
#include <vector>

#include <boost/bind.hpp>
#include <boost/function.hpp>
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

using std::string;
using std::vector;
using strings::Substitute;
using kudu::client::internal::ErrorCollector;

namespace kudu {
namespace client {

TEST(ClientUnitTest, TestSchemaBuilder_EmptySchema) {
  KuduSchema s;
  KuduSchemaBuilder b;
  ASSERT_EQ("Invalid argument: no primary key specified",
            b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestSchemaBuilder_KeyNotSpecified) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NotNull();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32)->NotNull();
  ASSERT_EQ("Invalid argument: no primary key specified",
            b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestSchemaBuilder_DuplicateColumn) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b.AddColumn("x")->Type(KuduColumnSchema::INT32);
  b.AddColumn("x")->Type(KuduColumnSchema::INT32);
  ASSERT_EQ("Invalid argument: Duplicate column name: x",
            b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestSchemaBuilder_KeyNotFirstColumn) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32);
  b.AddColumn("x")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();;
  b.AddColumn("x")->Type(KuduColumnSchema::INT32);
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

TEST(ClientUnitTest, TestSchemaBuilder_PrimaryKeyOnColumnAndSet) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->PrimaryKey();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32);
  b.SetPrimaryKey({ "a", "b" });
  ASSERT_EQ("Invalid argument: primary key specified by both "
            "SetPrimaryKey() and on a specific column: a",
            b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestSchemaBuilder_SingleKey_GoodSchema) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32);
  b.AddColumn("c")->Type(KuduColumnSchema::INT32)->NotNull();
  ASSERT_EQ("OK", b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestSchemaBuilder_CompoundKey_GoodSchema) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NotNull();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32)->NotNull();
  b.SetPrimaryKey({ "a", "b" });
  ASSERT_EQ("OK", b.Build(&s).ToString());

  vector<int> key_columns;
  s.GetPrimaryKeyColumnIndexes(&key_columns);
  ASSERT_EQ(vector<int>({ 0, 1 }), key_columns);
}

TEST(ClientUnitTest, TestSchemaBuilder_DefaultValues) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32)->NotNull()
    ->Default(KuduValue::FromInt(12345));
  ASSERT_EQ("OK", b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestSchemaBuilder_DefaultValueString) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b.AddColumn("b")->Type(KuduColumnSchema::STRING)->NotNull()
    ->Default(KuduValue::CopyString("abc"));
  b.AddColumn("c")->Type(KuduColumnSchema::BINARY)->NotNull()
    ->Default(KuduValue::CopyString("def"));
  ASSERT_EQ("OK", b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestSchemaBuilder_CompoundKey_KeyNotFirst) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("x")->Type(KuduColumnSchema::INT32)->NotNull();
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NotNull();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32)->NotNull();
  b.SetPrimaryKey({ "a", "b" });
  ASSERT_EQ("Invalid argument: primary key columns must be listed "
            "first in the schema: a",
            b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestSchemaBuilder_CompoundKey_BadColumnName) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NotNull();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32)->NotNull();
  b.SetPrimaryKey({ "foo" });
  ASSERT_EQ("Invalid argument: primary key column not defined: foo",
            b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestDisableSslFailsIfNotInitialized) {
  // If we try to disable SSL initialization without setting up SSL properly,
  // it should return an error.
  Status s = DisableOpenSSLInitialization();
  ASSERT_STR_MATCHES(s.ToString(), "Locking callback not initialized");
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
                       boost::bind(TestFunc, _1, _2, &counter));
  ASSERT_TRUE(s.IsTimedOut());
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

TEST(ClientUnitTest, TestKuduSchemaToString) {
  // Test on unique PK.
  KuduSchema s1;
  KuduSchemaBuilder b1;
  b1.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b1.AddColumn("int_val")->Type(KuduColumnSchema::INT32)->NotNull();
  b1.AddColumn("string_val")->Type(KuduColumnSchema::STRING)->Nullable();
  b1.AddColumn("non_null_with_default")->Type(KuduColumnSchema::INT32)->NotNull()
    ->Default(KuduValue::FromInt(12345));
  ASSERT_OK(b1.Build(&s1));

  string schema_str_1 = "(\n"
                        "    key INT32 NOT NULL,\n"
                        "    int_val INT32 NOT NULL,\n"
                        "    string_val STRING NULLABLE,\n"
                        "    non_null_with_default INT32 NOT NULL,\n"
                        "    PRIMARY KEY (key)\n"
                        ")";
  EXPECT_EQ(schema_str_1, s1.ToString());

  // Test empty schema.
  KuduSchema s2;
  EXPECT_EQ("()", s2.ToString());

  // Test on composite PK.
  // Create a different schema with a multi-column PK.
  KuduSchemaBuilder b2;
  b2.AddColumn("k1")->Type(KuduColumnSchema::INT32)->NotNull();
  b2.AddColumn("k2")->Type(KuduColumnSchema::UNIXTIME_MICROS)->NotNull();
  b2.AddColumn("k3")->Type(KuduColumnSchema::INT8)->NotNull();
  b2.AddColumn("dec_val")->Type(KuduColumnSchema::DECIMAL)->Nullable()->Precision(9)->Scale(2);
  b2.AddColumn("int_val")->Type(KuduColumnSchema::INT32)->NotNull();
  b2.AddColumn("string_val")->Type(KuduColumnSchema::STRING)->Nullable();
  b2.AddColumn("non_null_with_default")->Type(KuduColumnSchema::INT32)->NotNull()
    ->Default(KuduValue::FromInt(12345));
  b2.SetPrimaryKey({"k1", "k2", "k3"});
  ASSERT_OK(b2.Build(&s2));

  string schema_str_2 = "(\n"
                        "    k1 INT32 NOT NULL,\n"
                        "    k2 UNIXTIME_MICROS NOT NULL,\n"
                        "    k3 INT8 NOT NULL,\n"
                        "    dec_val DECIMAL(9, 2) NULLABLE,\n"
                        "    int_val INT32 NOT NULL,\n"
                        "    string_val STRING NULLABLE,\n"
                        "    non_null_with_default INT32 NOT NULL,\n"
                        "    PRIMARY KEY (k1, k2, k3)\n"
                        ")";
  EXPECT_EQ(schema_str_2, s2.ToString());
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

} // namespace client
} // namespace kudu
