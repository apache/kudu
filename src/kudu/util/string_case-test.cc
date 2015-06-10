// Copyright (c) 2012, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <gtest/gtest.h>

#include "kudu/util/string_case.h"

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

TEST(TestStringCase, TestCapitalize) {
  string word = "foo";
  Capitalize(&word);
  ASSERT_EQ("Foo", word);

  word = "HiBerNATe";
  Capitalize(&word);
  ASSERT_EQ("Hibernate", word);
}

} // namespace kudu
