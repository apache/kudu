// Copyright (c) 2012, Cloudera, inc.

#include <gtest/gtest.h>

#include "util/string_case.h"

using std::string;

namespace kudu {

TEST(TestStringCase, TestSnakeToCamel) {
  string out;
  SnakeToCamelCase("foo_bar", &out);
  ASSERT_EQ("FooBar", out);


  SnakeToCamelCase("foo-bar", &out);
  ASSERT_EQ("FooBar", out);

  SnakeToCamelCase("foobar", &out);
  ASSERT_EQ("Foobar", out);
}

TEST(TestStringCase, TestToUpperCase) {
  string out;
  ToUpperCase(string("foo"), &out);
  ASSERT_EQ("FOO", out);
  ToUpperCase(string("foo bar-BaZ"), &out);
  ASSERT_EQ("FOO BAR-BAZ", out);
}

TEST(TestStringCase, TestToUpperCaseInPlace) {
  string in_out = "foo";
  ToUpperCase(in_out, &in_out);
  ASSERT_EQ("FOO", in_out);
}

} // namespace kudu
