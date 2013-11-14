// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <gtest/gtest.h>
#include <strstream>
#include <string>

#include "util/trace.h"
#include "util/test_util.h"

namespace kudu {

class TraceTest : public KuduTest {
};

// Replace all digits in 's' with the character 'X'.
static string XOutDigits(const string& s) {
  string ret;
  ret.reserve(s.size());
  BOOST_FOREACH(char c, s) {
    if (isdigit(c)) {
      ret.push_back('X');
    } else {
      ret.push_back(c);
    }
  }
  return ret;
}

TEST_F(TraceTest, TestBasic) {
  Trace t;
  t.SubstituteAndTrace("hello $0, $1", "world", 12345);
  t.SubstituteAndTrace("goodbye $0, $1", "cruel world", 54321);
  t.Message("simple string trace");

  std::stringstream stream;
  t.Dump(&stream);
  string result = XOutDigits(stream.str());
  ASSERT_EQ("XXXX XX:XX:XX.XXXXXX hello world, XXXXX\n"
            "XXXX XX:XX:XX.XXXXXX goodbye cruel world, XXXXX\n"
            "XXXX XX:XX:XX.XXXXXX simple string trace\n", result);
}

} // namespace kudu
