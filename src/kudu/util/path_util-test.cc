// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <gtest/gtest.h>

#include "kudu/util/path_util.h"

namespace kudu {

TEST(TestPathUtil, BaseNameTest) {
  ASSERT_EQ(".", BaseName(""));
  ASSERT_EQ(".", BaseName("."));
  ASSERT_EQ("..", BaseName(".."));
  ASSERT_EQ("/", BaseName("/"));
  ASSERT_EQ("/", BaseName("//"));
  ASSERT_EQ("a", BaseName("a"));
  ASSERT_EQ("ab", BaseName("ab"));
  ASSERT_EQ("ab", BaseName("ab/"));
  ASSERT_EQ("cd", BaseName("ab/cd"));
  ASSERT_EQ("ab", BaseName("/ab"));
  ASSERT_EQ("ab", BaseName("/ab///"));
  ASSERT_EQ("cd", BaseName("/ab/cd"));
}

TEST(TestPathUtil, DirNameTest) {
  ASSERT_EQ(".", DirName(""));
  ASSERT_EQ(".", DirName("."));
  ASSERT_EQ(".", DirName(".."));
  ASSERT_EQ("/", DirName("/"));
  ASSERT_EQ("//", DirName("//"));
  ASSERT_EQ(".", DirName("a"));
  ASSERT_EQ(".", DirName("ab"));
  ASSERT_EQ(".", DirName("ab/"));
  ASSERT_EQ("ab", DirName("ab/cd"));
  ASSERT_EQ("/", DirName("/ab"));
  ASSERT_EQ("/", DirName("/ab///"));
  ASSERT_EQ("/ab", DirName("/ab/cd"));
}

} // namespace kudu
