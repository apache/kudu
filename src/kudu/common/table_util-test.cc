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

  EXPECT_TRUE(ParseHiveTableIdentifier("", &db, &tbl).IsInvalidArgument());
  EXPECT_TRUE(ParseHiveTableIdentifier(".", &db, &tbl).IsInvalidArgument());
  EXPECT_TRUE(ParseHiveTableIdentifier("no_table", &db, &tbl).IsInvalidArgument());
  EXPECT_TRUE(ParseHiveTableIdentifier("lots.of.tables", &db, &tbl).IsInvalidArgument());
  EXPECT_TRUE(ParseHiveTableIdentifier(".no_table", &db, &tbl).IsInvalidArgument());
  EXPECT_TRUE(ParseHiveTableIdentifier("no_table.", &db, &tbl).IsInvalidArgument());
  EXPECT_TRUE(ParseHiveTableIdentifier(".no_database", &db, &tbl).IsInvalidArgument());
  EXPECT_TRUE(ParseHiveTableIdentifier("punctuation?.no", &db, &tbl).IsInvalidArgument());
  EXPECT_TRUE(ParseHiveTableIdentifier("white space.no", &db, &tbl).IsInvalidArgument());
  EXPECT_TRUE(ParseHiveTableIdentifier("unicode☃tables.no", &db, &tbl).IsInvalidArgument());
  EXPECT_TRUE(ParseHiveTableIdentifier(string("\0.\0", 3), &db, &tbl).IsInvalidArgument());
}

TEST(TestTableUtil, TestRangerTableIdentifier) {
  string db;
  Slice tbl;
  string table;
  bool default_database;

  table = "foo.bar";
  EXPECT_OK(ParseRangerTableIdentifier(table, &db, &tbl, &default_database));
  EXPECT_EQ("foo", db);
  EXPECT_EQ("bar", tbl);
  EXPECT_FALSE(default_database);

  table = "99bottles.my_awesome/table/22";
  EXPECT_OK(ParseRangerTableIdentifier(table, &db, &tbl, &default_database));
  EXPECT_EQ("99bottles", db);
  EXPECT_EQ("my_awesome/table/22", tbl);
  EXPECT_FALSE(default_database);

  table = "99/bottles.my_awesome/table/22";
  EXPECT_OK(ParseRangerTableIdentifier(table, &db, &tbl, &default_database));
  EXPECT_EQ("99/bottles", db);
  EXPECT_EQ("my_awesome/table/22", tbl);
  EXPECT_FALSE(default_database);

  table = "_leading_underscore.trailing_underscore_";
  EXPECT_OK(ParseRangerTableIdentifier(table, &db, &tbl, &default_database));
  EXPECT_EQ("_leading_underscore", db);
  EXPECT_EQ("trailing_underscore_", tbl);
  EXPECT_FALSE(default_database);

  table = "foo";
  EXPECT_OK(ParseRangerTableIdentifier(table, &db, &tbl, &default_database));
  EXPECT_EQ("default", db);
  EXPECT_EQ("foo", tbl);
  EXPECT_TRUE(default_database);

  table = "default.foo";
  EXPECT_OK(ParseRangerTableIdentifier(table, &db, &tbl, &default_database));
  EXPECT_EQ("default", db);
  EXPECT_EQ("foo", tbl);
  EXPECT_FALSE(default_database);

  table = "lots.of.tables";
  EXPECT_OK(ParseRangerTableIdentifier(table, &db, &tbl, &default_database));
  EXPECT_EQ("lots", db);
  EXPECT_EQ("of.tables", tbl);
  EXPECT_FALSE(default_database);

  table = "db_name..table_name";
  EXPECT_OK(ParseRangerTableIdentifier(table, &db, &tbl, &default_database));
  EXPECT_EQ("db_name", db);
  EXPECT_EQ(".table_name", tbl);
  EXPECT_FALSE(default_database);

  EXPECT_TRUE(ParseRangerTableIdentifier("", &db, &tbl, &default_database)
      .IsInvalidArgument());
  EXPECT_TRUE(ParseRangerTableIdentifier(".", &db, &tbl, &default_database)
      .IsInvalidArgument());
  EXPECT_OK(ParseRangerTableIdentifier("no_table", &db, &tbl,
                                       &default_database));
  EXPECT_OK(ParseRangerTableIdentifier("lots.of.tables", &db, &tbl,
                                       &default_database));
  EXPECT_TRUE(ParseRangerTableIdentifier("no_table.", &db, &tbl,
                                         &default_database)
      .IsInvalidArgument());
  EXPECT_TRUE(ParseRangerTableIdentifier(".no_database", &db, &tbl,
                                         &default_database)
      .IsInvalidArgument());
  EXPECT_OK(ParseRangerTableIdentifier("punctuation?.yes", &db, &tbl,
                                       &default_database));
  EXPECT_OK(ParseRangerTableIdentifier("white space.yes", &db, &tbl,
                                       &default_database));
  EXPECT_OK(ParseRangerTableIdentifier("unicode☃tables.yes", &db, &tbl,
                                       &default_database));
  EXPECT_OK(ParseRangerTableIdentifier("unicode.☃tables.yes", &db, &tbl,
                                       &default_database));
  EXPECT_OK(ParseRangerTableIdentifier(string("\0.\0", 3), &db, &tbl,
                                       &default_database));
}

} // namespace kudu
