// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/rolling_log.h"

#include <boost/foreach.hpp>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <string>
#include <vector>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/env.h"
#include "kudu/util/memenv/memenv.h"
#include "kudu/util/path_util.h"
#include "kudu/util/test_util.h"

using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {

class RollingLogTest : public KuduTest {
 public:
  RollingLogTest()
    : log_dir_(GetTestPath("log_dir")) {
  }

  virtual void SetUp() OVERRIDE {
    ASSERT_OK(env_->CreateDir(log_dir_));
  }

 protected:
  void AssertLogCount(int expected_count, vector<string>* children) {
    vector<string> dir_entries;
    ASSERT_OK(env_->GetChildren(log_dir_, &dir_entries));
    children->clear();

    BOOST_FOREACH(const string& child, dir_entries) {
      if (child == "." || child == "..") continue;
      children->push_back(child);
      ASSERT_TRUE(HasPrefixString(child, "rolling_log-test."));
      ASSERT_STR_CONTAINS(child, ".mylog.");
      ASSERT_TRUE(HasSuffixString(child, Substitute("$0", getpid())));
    }
    ASSERT_EQ(children->size(), expected_count) << *children;
  }

  const string log_dir_;
};

TEST_F(RollingLogTest, TestLog) {
  RollingLog log(env_.get(), log_dir_, "mylog");
  log.SetSizeLimitBytes(100);

  // Before writing anything, we shouldn't open a log file.
  vector<string> children;
  NO_FATALS(AssertLogCount(0, &children));

  // Appending some data should write a new segment.
  ASSERT_OK(log.Append("Hello world\n"));
  NO_FATALS(AssertLogCount(1, &children));

  for (int i = 0; i < 10; i++) {
    ASSERT_OK(log.Append("Hello world\n"));
  }
  NO_FATALS(AssertLogCount(2, &children));

  faststring data;
  string path = JoinPathSegments(log_dir_, children[0]);
  ASSERT_OK(ReadFileToString(env_.get(), path, &data));
  ASSERT_TRUE(HasPrefixString(data.ToString(), "Hello world\n"))
    << "Data missing";
  ASSERT_LE(data.size(), 100) << "Size limit not respected";
}

} // namespace kudu
