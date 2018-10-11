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

#include "kudu/common/table_util.h"

#include <string>

#include <gtest/gtest.h>

#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

namespace kudu {

using std::string;

TEST(TestTableUtil, TestParseHiveTableIdentifier) {
  Slice db;
  Slice tbl;
  string table;

  table = "foo.bar";
  ASSERT_OK(ParseHiveTableIdentifier(table, &db, &tbl));
  EXPECT_EQ("foo", db);
  EXPECT_EQ("bar", tbl);

  table = "99bottles.my_awesome/table/22";
  ASSERT_OK(ParseHiveTableIdentifier(table, &db, &tbl));
  EXPECT_EQ("99bottles", db);
  EXPECT_EQ("my_awesome/table/22", tbl);

  table = "_leading_underscore.trailing_underscore_";
  ASSERT_OK(ParseHiveTableIdentifier(table, &db, &tbl));
  EXPECT_EQ("_leading_underscore", db);
  EXPECT_EQ("trailing_underscore_", tbl);

  EXPECT_TRUE(ParseHiveTableIdentifier(".", &db, &tbl).IsInvalidArgument());
  EXPECT_TRUE(ParseHiveTableIdentifier("no-table", &db, &tbl).IsInvalidArgument());
  EXPECT_TRUE(ParseHiveTableIdentifier("lots.of.tables", &db, &tbl).IsInvalidArgument());
  EXPECT_TRUE(ParseHiveTableIdentifier(".no_table", &db, &tbl).IsInvalidArgument());
  EXPECT_TRUE(ParseHiveTableIdentifier("no_table.", &db, &tbl).IsInvalidArgument());
  EXPECT_TRUE(ParseHiveTableIdentifier(".no_database", &db, &tbl).IsInvalidArgument());
  EXPECT_TRUE(ParseHiveTableIdentifier("punctuation?.no", &db, &tbl).IsInvalidArgument());
  EXPECT_TRUE(ParseHiveTableIdentifier("white space.no", &db, &tbl).IsInvalidArgument());
  EXPECT_TRUE(ParseHiveTableIdentifier("unicode☃tables.no", &db, &tbl).IsInvalidArgument());
  EXPECT_TRUE(ParseHiveTableIdentifier(string("\0.\0", 3), &db, &tbl).IsInvalidArgument());
}

} // namespace kudu
