// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
// All rights reserved.

#include <boost/assign/list_of.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/schema.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet-test-base.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace tablet {

class CompositePushdownTest : public KuduTabletTest {
 public:
  CompositePushdownTest()
      : KuduTabletTest(Schema(boost::assign::list_of
                              (ColumnSchema("year", INT16))
                              (ColumnSchema("month", INT8))
                              (ColumnSchema("day", INT8))
                              (ColumnSchema("data", STRING)),
                              3)) {
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
          CHECK_OK(row.SetInt16(0, year));
          CHECK_OK(row.SetInt8(1, month));
          CHECK_OK(row.SetInt8(2, day));
          CHECK_OK(row.SetStringCopy(3, StringPrintf("%d/%02d/%02d", year, month, day)));
          ASSERT_OK_FAST(writer.Insert(row));

          if (i == nrows * 9 / 10) {
            ASSERT_OK(tablet()->Flush());
          }
          ++i;
        }
      }
    }
  }

  void ScanTablet(ScanSpec *spec, vector<string> *results, const char *descr) {
    SCOPED_TRACE(descr);

    gscoped_ptr<RowwiseIterator> iter;
    ASSERT_OK(tablet()->NewRowIterator(client_schema_, &iter));
    ASSERT_OK(iter->Init(spec));
    ASSERT_TRUE(spec->predicates().empty()) << "Should have accepted all predicates";
    LOG_TIMING(INFO, descr) {
      ASSERT_OK(IterateToStringList(iter.get(), results));
    }
    BOOST_FOREACH(const string &str, *results) {
      VLOG(1) << str;
    }
  }
};

// Helper function for sorting returned results by the last twelve
// characters (the formatted date). This is needed as "2" is
// lexicographically greater than "12" which means that, e.g.,
// comparing "(int16 year=2001, int8 month=2, int8 day=7, string
// data=2001/02/07)" to "(int16 year=2001, int8 month=12, int8
// day=7, string data=2001/12/07)" would be semantically incorrect if
// the comparison was on the whole string vs the last portion of the
// string ("2001/02/01" vs. "2001/12/01")
struct SuffixComparator {
  bool operator()(const string &a, const string &b) {
    size_t len = a.size();
    string s_a = a.substr(len - 12, 11);
    string s_b = b.substr(len - 12, 11);
    return s_a < s_b;
  }
};

TEST_F(CompositePushdownTest, TestPushDownExactEquality) {
  ScanSpec spec;
  int16_t year = 2001;
  int8_t month = 9;
  int8_t day = 7;
  ColumnRangePredicate pred_year(schema_.column(0), &year, &year);
  ColumnRangePredicate pred_month(schema_.column(1), &month, &month);
  ColumnRangePredicate pred_day(schema_.column(2), &day, &day);
  spec.AddPredicate(pred_year);
  spec.AddPredicate(pred_month);
  spec.AddPredicate(pred_day);
  vector<string> results;

  ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results, "Exact match using compound key"));
  ASSERT_EQ(1, results.size());
  ASSERT_EQ("(int16 year=2001, int8 month=9, int8 day=7, string data=2001/09/07)",
            results.front());
}

TEST_F(CompositePushdownTest, TestPushDownPrefixEquality) {
  int16_t year = 2001;
  int8_t month = 9;
  ColumnRangePredicate pred_year(schema_.column(0), &year, &year);
  ColumnRangePredicate pred_month(schema_.column(1), &month, &month);

  {
    ScanSpec spec;
    spec.AddPredicate(pred_year);
    spec.AddPredicate(pred_month);
    vector<string> results;
    ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results,
                                       "Prefix match using 2/3 of a compound key"));
    ASSERT_EQ(28, results.size());
    ASSERT_EQ("(int16 year=2001, int8 month=9, int8 day=1, string data=2001/09/01)",
              results.front());
    ASSERT_EQ("(int16 year=2001, int8 month=9, int8 day=28, string data=2001/09/28)",
              results.back());
  }

  {
    ScanSpec spec;
    spec.AddPredicate(pred_year);
    vector<string> results;
    ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results,
                                       "Prefix match using 1/3 of a compound key"));
    ASSERT_EQ(28 * 12, results.size());
    ASSERT_EQ("(int16 year=2001, int8 month=1, int8 day=1, string data=2001/01/01)",
              results.front());
    ASSERT_EQ("(int16 year=2001, int8 month=2, int8 day=1, string data=2001/02/01)",
              results[28]);
    ASSERT_EQ("(int16 year=2001, int8 month=12, int8 day=28, string data=2001/12/28)",
              results.back());
  }
}

TEST_F(CompositePushdownTest, TestPushDownPrefixEqualitySuffixInequality) {
  int16_t year = 2001;
  int8_t month_l = 9;
  int8_t month_u = 11;
  int8_t day_l = 1;
  int8_t day_u = 15;

  ColumnRangePredicate pred_year(schema_.column(0), &year, &year);

  ColumnRangePredicate pred_month_eq(schema_.column(1), &month_l, &month_l);
  ColumnRangePredicate pred_month_ge_le(schema_.column(1), &month_l, &month_u);
  ColumnRangePredicate pred_month_le(schema_.column(1), NULL, &month_l);

  ColumnRangePredicate pred_day_ge_le(schema_.column(2), &day_l, &day_u);
  ColumnRangePredicate pred_day_ge(schema_.column(2), &day_l, NULL);
  ColumnRangePredicate pred_day_le(schema_.column(2), NULL, &day_u);

  {
    // year=2001, month=9, day >= 1 && day <= 15
    ScanSpec spec;
    spec.AddPredicate(pred_year);
    spec.AddPredicate(pred_month_eq);
    spec.AddPredicate(pred_day_ge_le);
    vector<string> results;
    ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results, "Prefix equality, suffix inequality"));
    ASSERT_EQ(15, results.size());
    ASSERT_EQ("(int16 year=2001, int8 month=9, int8 day=1, string data=2001/09/01)",
              results.front());
    ASSERT_EQ("(int16 year=2001, int8 month=9, int8 day=15, string data=2001/09/15)",
              results.back());
  }

  {
    // year=2001, month=9, day >= 1
    ScanSpec spec;
    spec.AddPredicate(pred_year);
    spec.AddPredicate(pred_month_eq);
    spec.AddPredicate(pred_day_ge);
    vector<string> results;
    ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results, "Prefix equality, suffix inequality"));
    ASSERT_EQ(28, results.size());
    ASSERT_EQ("(int16 year=2001, int8 month=9, int8 day=1, string data=2001/09/01)",
              results.front());
    ASSERT_EQ("(int16 year=2001, int8 month=9, int8 day=28, string data=2001/09/28)",
              results.back());
  }

  {
    // year=2001, month=9, day <= 15
    ScanSpec spec;
    spec.AddPredicate(pred_year);
    spec.AddPredicate(pred_month_eq);
    spec.AddPredicate(pred_day_le);
    vector<string> results;
    ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results, "Prefix equality, suffix inequality"));
    ASSERT_EQ(15, results.size());
    ASSERT_EQ("(int16 year=2001, int8 month=9, int8 day=1, string data=2001/09/01)",
              results.front());
    ASSERT_EQ("(int16 year=2001, int8 month=9, int8 day=15, string data=2001/09/15)",
              results.back());
  }

  {
    // year=2001, month >= 9 && month <= 11
    ScanSpec spec;
    spec.AddPredicate(pred_year);
    spec.AddPredicate(pred_month_ge_le);
    vector<string> results;
    ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results, "Prefix equality, suffix inequality"));
    ASSERT_EQ(3 * 28, results.size());
    ASSERT_EQ("(int16 year=2001, int8 month=9, int8 day=1, string data=2001/09/01)",
              results.front());
    ASSERT_EQ("(int16 year=2001, int8 month=11, int8 day=28, string data=2001/11/28)",
              results.back());
  }

  {
    // year=2001, month <= 9
    ScanSpec spec;
    spec.AddPredicate(pred_year);
    spec.AddPredicate(pred_month_le);
    vector<string> results;
    ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results, "Prefix equality, suffix inequality"));
    ASSERT_EQ(9 * 28, results.size());
    ASSERT_EQ("(int16 year=2001, int8 month=1, int8 day=1, string data=2001/01/01)",
              results.front());
    ASSERT_EQ("(int16 year=2001, int8 month=9, int8 day=28, string data=2001/09/28)",
              results.back());
  }
}

TEST_F(CompositePushdownTest, TestPushdownPrefixInequality) {

  int16_t year_2001 = 2001;
  int16_t year_2003 = 2003;
  {
    // year >= 2001 && year <= 2003
    ColumnRangePredicate pred_year(schema_.column(0), &year_2001, &year_2003);
    ScanSpec spec;
    spec.AddPredicate(pred_year);
    vector<string> results;
    ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results, "Prefix inequality"));
    ASSERT_EQ(3 * 12 * 28, results.size());
    ASSERT_EQ("(int16 year=2001, int8 month=1, int8 day=1, string data=2001/01/01)",
              results.front());
    ASSERT_EQ("(int16 year=2003, int8 month=12, int8 day=28, string data=2003/12/28)",
              results.back());
  }

  {
    // year >= 2001
    ColumnRangePredicate pred_year(schema_.column(0), &year_2001, NULL);
    ScanSpec spec;
    spec.AddPredicate(pred_year);
    vector<string> results;
    ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results, "Prefix inequality"));
    ASSERT_EQ(10 * 12 * 28, results.size());
    // Needed because results from memrowset are returned first and memrowset begins
    // with last 10% of the keys (e.g., last few years)
    std::sort(results.begin(), results.end(), SuffixComparator());
    ASSERT_EQ("(int16 year=2001, int8 month=1, int8 day=1, string data=2001/01/01)",
              results.front());
    ASSERT_EQ("(int16 year=2010, int8 month=12, int8 day=28, string data=2010/12/28)",
              results.back());
  }

  {
    // year <= 2003
    ColumnRangePredicate pred_year(schema_.column(0), NULL, &year_2003);
    ScanSpec spec;
    spec.AddPredicate(pred_year);
    vector<string> results;
    ASSERT_NO_FATAL_FAILURE(ScanTablet(&spec, &results, "Prefix inequality"));
    ASSERT_EQ(4 * 12 * 28, results.size());
    ASSERT_EQ("(int16 year=2000, int8 month=1, int8 day=1, string data=2000/01/01)",
              results.front());
    ASSERT_EQ("(int16 year=2003, int8 month=12, int8 day=28, string data=2003/12/28)",
              results.back());
  }
}



} // namespace tablet
} // namespace kudu
