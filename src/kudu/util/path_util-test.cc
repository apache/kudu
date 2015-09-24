// Copyright 2015 Cloudera, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
