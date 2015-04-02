// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/os-util.h"

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <sys/prctl.h>
#include <sys/syscall.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/errno.h"
#include "kudu/util/test_macros.h"

using std::string;

namespace kudu {

void RunTest(const string& name, int user_ticks, int kernel_ticks, int io_wait) {
  string buf = strings::Substitute(string("0 ($0) S 0 0 0 0 0 0 0") +
                                   " 0 0 0 $1 $2 0 0 0 0 0"         +
                                   " 0 0 0 0 0 0 0 0 0 0 "          +
                                   " 0 0 0 0 0 0 0 0 0 0 "          +
                                   " 0 $3 0 0 0 0 0 0 0 0 "         +
                                   " 0 0",
                                   name, user_ticks, kernel_ticks, io_wait);
  ThreadStats stats;
  string extracted_name;
  ASSERT_OK(ParseStat(buf, &extracted_name, &stats));
  ASSERT_EQ(name, extracted_name);
  ASSERT_EQ(user_ticks * (1e9 / sysconf(_SC_CLK_TCK)), stats.user_ns);
  ASSERT_EQ(kernel_ticks * (1e9 / sysconf(_SC_CLK_TCK)), stats.kernel_ns);
  ASSERT_EQ(io_wait * (1e9 / sysconf(_SC_CLK_TCK)), stats.iowait_ns);
}

TEST(OsUtilTest, TestSelf) {
  RunTest("test", 111, 222, 333);
}

TEST(OsUtilTest, TestSelfNameWithSpace) {
  RunTest("a space", 111, 222, 333);
}

TEST(OsUtilTest, TestSelfNameWithParens) {
  RunTest("a(b(c((d))e)", 111, 222, 333);
}

} // namespace kudu
