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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/column_predicate.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/iterator.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"

using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace tablet {

const char* const kTestHostnames[] = { "foo", "foobar", "baz", nullptr };

class CompositePushdownTest : public KuduTabletTest {
 public:
  CompositePushdownTest()
      : KuduTabletTest(Schema({ ColumnSchema("year", INT16),
                                ColumnSchema("month", INT8),
                                ColumnSchema("day", INT8),
                                ColumnSchema("hostname", STRING),
                                ColumnSchema("data", STRING) },
                              4)) {
  }

  virtual void SetUp() OVERRIDE {
    KuduTabletTest::SetUp();

    FillTestTablet();
  }

  void FillTestTablet() {
    uint32_t nrows = 10 * 12 * 28;
    int i = 0;

    LocalTabletWriter writer(tablet().get(), &client_schema_);
    KuduPartialRow row(&client_schema_);
    for (int16_t year = 2000; year <= 2010; year++) {
      for (int8_t month = 1; month <= 12; month++) {
        for (int8_t day = 1; day <= 28; day++) {
          for (int host_idx = 0; kTestHostnames[host_idx] != nullptr; host_idx++) {
            CHECK_OK(row.SetInt16(0, year));
            CHECK_OK(row.SetInt8(1, month));
            CHECK_OK(row.SetInt8(2, day));
            CHECK_OK(row.SetStringCopy(3, kTestHostnames[host_idx]));
            CHECK_OK(row.SetStringCopy(4, StringPrintf("%d/%02d/%02d-%s", year, month, day,
                                                       kTestHostnames[host_idx])));
            ASSERT_OK_FAST(writer.Insert(row));

            if (i == nrows * 9 / 10) {
              ASSERT_OK(tablet()->Flush());
            }
            ++i;
          }
        }
      }
    }
  }

  // Helper function for sorting returned results by the 'data' field.
  // This is needed as "2" is lexicographically greater than "12" which means
  // that, e.g., comparing "(int16 year=2001, int8 month=2, int8 day=7, string
  // data=2001/02/07)" to "(int16 year=2001, int8 month=12, int8
  // day=7, string data=2001/12/07)" would be semantically incorrect if
  // the comparison was on the whole string vs the last portion of the
  // string ("2001/02/01" vs. "2001/12/01")
  struct SuffixComparator {
    bool operator()(const string &a, const string &b) {
      string s_a = a.substr(a.find("data="));
      string s_b = b.substr(b.find("data="));
      return s_a < s_b;
    }
  };

  void ScanTablet(ScanSpec *spec, vector<string> *results, const char *descr) {
    SCOPED_TRACE(descr);

    SchemaPtr schema_ptr = std::make_shared<Schema>(client_schema_);
    unique_ptr<RowwiseIterator> iter;
    ASSERT_OK(tablet()->NewRowIterator(schema_ptr, &iter));
    ASSERT_OK(iter->Init(spec));
    ASSERT_TRUE(spec->predicates().empty()) << "Should have accepted all predicates";
    LOG_TIMING(INFO, descr) {
      ASSERT_OK(IterateToStringList(iter.get(), results));
    }
    std::sort(results->begin(), results->end(), SuffixComparator());
    for (const string &str : *results) {
      VLOG(1) << str;
    }
  }
};

TEST_F(CompositePushdownTest, TestPushDownExactEquality) {
  ScanSpec spec;
  int16_t year = 2001;
  int8_t month = 9;
  int8_t day = 7;
  Slice host(kTestHostnames[0]);
  auto pred_year = ColumnPredicate::Equality(schema_.column(0), &year);
  auto pred_month = ColumnPredicate::Equality(schema_.column(1), &month);
  auto pred_day = ColumnPredicate::Equality(schema_.column(2), &day);
  auto pred_host = ColumnPredicate::Equality(schema_.column(3), &host);
  spec.AddPredicate(pred_year);
  spec.AddPredicate(pred_month);
  spec.AddPredicate(pred_day);
  spec.AddPredicate(pred_host);
  vector<string> results;

  NO_FATALS(ScanTablet(&spec, &results, "Exact match using compound key"));
  ASSERT_EQ(1, results.size());
  EXPECT_EQ("(int16 year=2001, int8 month=9, int8 day=7, "
            R"(string hostname="foo", string data="2001/09/07-foo"))",
            results.front());
}


// Test for "host <= 'foo'" which should reject 'foobaz'.
// Regression test for a bug in an earlier implementation of predicate pushdown.
TEST_F(CompositePushdownTest, TestPushDownStringInequality) {
  Arena arena(256);
  ScanSpec spec;
  int16_t year = 2001;
  int8_t month = 9;
  int8_t day = 7;
  Slice host("foo");
  auto pred_year = ColumnPredicate::Equality(schema_.column(0), &year);
  auto pred_month = ColumnPredicate::Equality(schema_.column(1), &month);
  auto pred_day = ColumnPredicate::Equality(schema_.column(2), &day);
  auto pred_host = ColumnPredicate::InclusiveRange(schema_.column(3), nullptr, &host, &arena);
  spec.AddPredicate(pred_year);
  spec.AddPredicate(pred_month);
  spec.AddPredicate(pred_day);
  spec.AddPredicate(*pred_host);
  vector<string> results;

  NO_FATALS(ScanTablet(&spec, &results, "Exact match using compound key"));
  ASSERT_EQ(2, results.size());
  EXPECT_EQ("(int16 year=2001, int8 month=9, int8 day=7, "
            R"(string hostname="baz", string data="2001/09/07-baz"))",
            results.front());
  EXPECT_EQ("(int16 year=2001, int8 month=9, int8 day=7, "
            R"(string hostname="foo", string data="2001/09/07-foo"))",
            results.back());
}


TEST_F(CompositePushdownTest, TestPushDownDateEquality) {
  ScanSpec spec;
  int16_t year = 2001;
  int8_t month = 9;
  int8_t day = 7;
  auto pred_year = ColumnPredicate::Equality(schema_.column(0), &year);
  auto pred_month = ColumnPredicate::Equality(schema_.column(1), &month);
  auto pred_day = ColumnPredicate::Equality(schema_.column(2), &day);
  spec.AddPredicate(pred_year);
  spec.AddPredicate(pred_month);
  spec.AddPredicate(pred_day);
  vector<string> results;

  NO_FATALS(ScanTablet(&spec, &results, "Exact match using compound key"));
  ASSERT_EQ(3, results.size());
  EXPECT_EQ("(int16 year=2001, int8 month=9, int8 day=7, "
            R"(string hostname="baz", string data="2001/09/07-baz"))",
            results[0]);
  EXPECT_EQ("(int16 year=2001, int8 month=9, int8 day=7, "
            R"(string hostname="foo", string data="2001/09/07-foo"))",
            results[1]);
  EXPECT_EQ("(int16 year=2001, int8 month=9, int8 day=7, "
            R"(string hostname="foobar", string data="2001/09/07-foobar"))",
            results[2]);
}

TEST_F(CompositePushdownTest, TestPushDownPrefixEquality) {
  int16_t year = 2001;
  int8_t month = 9;
  ColumnPredicate pred_year = ColumnPredicate::Equality(schema_.column(0), &year);
  ColumnPredicate pred_month = ColumnPredicate::Equality(schema_.column(1), &month);

  {
    ScanSpec spec;
    spec.AddPredicate(pred_year);
    spec.AddPredicate(pred_month);
    vector<string> results;
    NO_FATALS(ScanTablet(&spec, &results,
                         "Prefix match using 2/3 of a compound key"));
    ASSERT_EQ(28 * 3, results.size());
    EXPECT_EQ("(int16 year=2001, int8 month=9, int8 day=1, "
              R"(string hostname="baz", string data="2001/09/01-baz"))",
              results.front());
    EXPECT_EQ("(int16 year=2001, int8 month=9, int8 day=28, "
              R"(string hostname="foobar", string data="2001/09/28-foobar"))",
              results.back());
  }

  {
    ScanSpec spec;
    spec.AddPredicate(pred_year);
    vector<string> results;
    NO_FATALS(ScanTablet(&spec, &results,
                         "Prefix match using 1/3 of a compound key"));
    ASSERT_EQ(28 * 12 * 3, results.size());
    EXPECT_EQ("(int16 year=2001, int8 month=1, int8 day=1, "
              R"(string hostname="baz", string data="2001/01/01-baz"))",
              results.front());
    EXPECT_EQ("(int16 year=2001, int8 month=2, int8 day=1, "
              R"(string hostname="baz", string data="2001/02/01-baz"))",
              results[28 * 3]);
    EXPECT_EQ("(int16 year=2001, int8 month=12, int8 day=28, "
              R"(string hostname="foobar", string data="2001/12/28-foobar"))",
              results.back());
  }
}

TEST_F(CompositePushdownTest, TestPushDownPrefixEqualitySuffixInequality) {
  int16_t year = 2001;
  int8_t month_l = 9;
  int8_t month_u = 12;
  int8_t day_l = 1;
  int8_t day_u = 16;

  auto pred_year = ColumnPredicate::Equality(schema_.column(0), &year);

  auto pred_month_eq = ColumnPredicate::Equality(schema_.column(1), &month_l);
  auto pred_month_ge_lt = ColumnPredicate::Range(schema_.column(1), &month_l, &month_u);
  auto pred_month_lt = ColumnPredicate::Range(schema_.column(1), nullptr, &month_l);

  auto pred_day_ge_lt = ColumnPredicate::Range(schema_.column(2), &day_l, &day_u);
  auto pred_day_ge = ColumnPredicate::Range(schema_.column(2), &day_l, nullptr);
  auto pred_day_lt = ColumnPredicate::Range(schema_.column(2), nullptr, &day_u);

  {
    // year=2001, month=9, day >= 1 && day < 16
    ScanSpec spec;
    spec.AddPredicate(pred_year);
    spec.AddPredicate(pred_month_eq);
    spec.AddPredicate(pred_day_ge_lt);
    vector<string> results;
    NO_FATALS(ScanTablet(&spec, &results, "Prefix equality, suffix inequality"));
    ASSERT_EQ(15 * 3, results.size());
    EXPECT_EQ("(int16 year=2001, int8 month=9, int8 day=1, "
              R"(string hostname="baz", string data="2001/09/01-baz"))",
              results.front());
    EXPECT_EQ("(int16 year=2001, int8 month=9, int8 day=15, "
              R"(string hostname="foobar", string data="2001/09/15-foobar"))",
              results.back());
  }

  {
    // year=2001, month=9, day >= 1
    ScanSpec spec;
    spec.AddPredicate(pred_year);
    spec.AddPredicate(pred_month_eq);
    spec.AddPredicate(pred_day_ge);
    vector<string> results;
    NO_FATALS(ScanTablet(&spec, &results, "Prefix equality, suffix inequality"));
    ASSERT_EQ(28 * 3, results.size());
    EXPECT_EQ("(int16 year=2001, int8 month=9, int8 day=1, "
              R"(string hostname="baz", string data="2001/09/01-baz"))",
              results.front());
    EXPECT_EQ("(int16 year=2001, int8 month=9, int8 day=28, "
              R"(string hostname="foobar", string data="2001/09/28-foobar"))",
              results.back());
  }

  {
    // year=2001, month=9, day < 16
    ScanSpec spec;
    spec.AddPredicate(pred_year);
    spec.AddPredicate(pred_month_eq);
    spec.AddPredicate(pred_day_lt);
    vector<string> results;
    NO_FATALS(ScanTablet(&spec, &results, "Prefix equality, suffix inequality"));
    ASSERT_EQ(15 * 3, results.size());
    EXPECT_EQ("(int16 year=2001, int8 month=9, int8 day=1, "
              R"(string hostname="baz", string data="2001/09/01-baz"))",
              results.front());
    EXPECT_EQ("(int16 year=2001, int8 month=9, int8 day=15, "
              R"(string hostname="foobar", string data="2001/09/15-foobar"))",
              results.back());
  }

  {
    // year=2001, month >= 9 && month < 12
    ScanSpec spec;
    spec.AddPredicate(pred_year);
    spec.AddPredicate(pred_month_ge_lt);
    vector<string> results;
    NO_FATALS(ScanTablet(&spec, &results, "Prefix equality, suffix inequality"));
    ASSERT_EQ(3 * 28 * 3, results.size());
    EXPECT_EQ("(int16 year=2001, int8 month=9, int8 day=1, "
              R"(string hostname="baz", string data="2001/09/01-baz"))",
              results.front());
    EXPECT_EQ("(int16 year=2001, int8 month=11, int8 day=28, "
              R"(string hostname="foobar", string data="2001/11/28-foobar"))",
              results.back());
  }

  {
    // year=2001, month < 9
    ScanSpec spec;
    spec.AddPredicate(pred_year);
    spec.AddPredicate(pred_month_lt);
    vector<string> results;
    NO_FATALS(ScanTablet(&spec, &results, "Prefix equality, suffix inequality"));
    ASSERT_EQ(8 * 28 * 3, results.size());
    EXPECT_EQ("(int16 year=2001, int8 month=1, int8 day=1, "
              R"(string hostname="baz", string data="2001/01/01-baz"))",
              results.front());
    EXPECT_EQ("(int16 year=2001, int8 month=8, int8 day=28, "
              R"(string hostname="foobar", string data="2001/08/28-foobar"))",
              results.back());
  }
}

TEST_F(CompositePushdownTest, TestPushdownPrefixInequality) {

  int16_t year_2001 = 2001;
  int16_t year_2004 = 2004;
  {
    // year >= 2001 && year < 2004
    auto pred_year = ColumnPredicate::Range(schema_.column(0), &year_2001, &year_2004);
    ScanSpec spec;
    spec.AddPredicate(pred_year);
    vector<string> results;
    NO_FATALS(ScanTablet(&spec, &results, "Prefix inequality"));
    ASSERT_EQ(3 * 12 * 28 * 3, results.size());
    EXPECT_EQ("(int16 year=2001, int8 month=1, int8 day=1, "
              R"(string hostname="baz", string data="2001/01/01-baz"))",
              results.front());
    EXPECT_EQ("(int16 year=2003, int8 month=12, int8 day=28, "
              R"(string hostname="foobar", string data="2003/12/28-foobar"))",
              results.back());
  }

  {
    // year >= 2001
    auto pred_year = ColumnPredicate::Range(schema_.column(0), &year_2001, nullptr);
    ScanSpec spec;
    spec.AddPredicate(pred_year);
    vector<string> results;
    NO_FATALS(ScanTablet(&spec, &results, "Prefix inequality"));
    ASSERT_EQ(10 * 12 * 28 * 3, results.size());
    // Needed because results from memrowset are returned first and memrowset begins
    // with last 10% of the keys (e.g., last few years)
    EXPECT_EQ("(int16 year=2001, int8 month=1, int8 day=1, "
              R"(string hostname="baz", string data="2001/01/01-baz"))",
              results.front());
    EXPECT_EQ("(int16 year=2010, int8 month=12, int8 day=28, "
              R"(string hostname="foobar", string data="2010/12/28-foobar"))",
              results.back());
  }

  {
    // year < 2004
    auto pred_year = ColumnPredicate::Range(schema_.column(0), nullptr, &year_2004);
    ScanSpec spec;
    spec.AddPredicate(pred_year);
    vector<string> results;
    NO_FATALS(ScanTablet(&spec, &results, "Prefix inequality"));
    ASSERT_EQ(4 * 12 * 28 * 3, results.size());
    EXPECT_EQ("(int16 year=2000, int8 month=1, int8 day=1, "
              R"(string hostname="baz", string data="2000/01/01-baz"))",
              results.front());
    EXPECT_EQ("(int16 year=2003, int8 month=12, int8 day=28, "
              R"(string hostname="foobar", string data="2003/12/28-foobar"))",
              results.back());
  }
}
} // namespace tablet
} // namespace kudu
