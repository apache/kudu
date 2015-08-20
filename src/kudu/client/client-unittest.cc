// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
//
// Tests for the client which are true unit tests and don't require a cluster, etc.

#include <boost/assign/list_of.hpp>
#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "kudu/client/client.h"

using boost::assign::list_of;
using std::string;
using std::vector;

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
  b.SetPrimaryKey(list_of<string>("a")("b"));
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
  b.SetPrimaryKey(list_of<string>("a")("b"));
  ASSERT_EQ("OK", b.Build(&s).ToString());

  vector<int> key_columns;
  s.GetPrimaryKeyColumnIndexes(&key_columns);
  ASSERT_EQ(list_of<int>(0)(1), key_columns);
}

TEST(ClientUnitTest, TestSchemaBuilder_DefaultValues) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32)->NotNull()
    ->Default(KuduValue::FromInt(12345));
  ASSERT_EQ("OK", b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestSchemaBuilder_CompoundKey_KeyNotFirst) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("x")->Type(KuduColumnSchema::INT32)->NotNull();
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NotNull();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32)->NotNull();
  b.SetPrimaryKey(list_of<string>("a")("b"));
  ASSERT_EQ("Invalid argument: primary key columns must be listed "
            "first in the schema: a",
            b.Build(&s).ToString());
}

TEST(ClientUnitTest, TestSchemaBuilder_CompoundKey_BadColumnName) {
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("a")->Type(KuduColumnSchema::INT32)->NotNull();
  b.AddColumn("b")->Type(KuduColumnSchema::INT32)->NotNull();
  b.SetPrimaryKey(list_of<string>("foo"));
  ASSERT_EQ("Invalid argument: primary key column not defined: foo",
            b.Build(&s).ToString());
}

} // namespace client
} // namespace kudu

