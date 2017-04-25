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
#ifndef KUDU_UTIL_TEST_MACROS_H
#define KUDU_UTIL_TEST_MACROS_H

#include <gmock/gmock.h>
#include <string>

// ASSERT_NO_FATAL_FAILURE is just too long to type.
#define NO_FATALS(expr) \
  ASSERT_NO_FATAL_FAILURE(expr)

// Detect fatals in the surrounding scope. NO_FATALS() only checks for fatals
// in the expression passed to it.
#define NO_PENDING_FATALS() \
  if (testing::Test::HasFatalFailure()) { return; }

#define ASSERT_OK(status) do { \
  const Status& _s = status;        \
  if (_s.ok()) { \
    SUCCEED(); \
  } else { \
    FAIL() << "Bad status: " << _s.ToString();  \
  } \
} while (0);

#define EXPECT_OK(status) do { \
  const Status& _s = status; \
  if (_s.ok()) { \
    SUCCEED(); \
  } else { \
    ADD_FAILURE() << "Bad status: " << _s.ToString();  \
  } \
} while (0);

// Like the above, but doesn't record successful
// tests.
#define ASSERT_OK_FAST(status) do { \
  const Status& _s = status; \
  if (!_s.ok()) { \
    FAIL() << "Bad status: " << _s.ToString(); \
  } \
} while (0);

#define ASSERT_STR_CONTAINS(str, substr) do { \
  const std::string& _s = (str); \
  if (_s.find((substr)) == std::string::npos) { \
    FAIL() << "Expected to find substring '" << (substr) \
    << "'. Got: '" << _s << "'"; \
  } \
} while (0);

#define ASSERT_STR_NOT_CONTAINS(str, substr) do { \
  const std::string& _s = (str); \
  if (_s.find((substr)) != std::string::npos) { \
    FAIL() << "Expected not to find substring '" << (substr) \
    << "'. Got: '" << _s << "'"; \
  } \
} while (0);

// Substring regular expressions in extended regex (POSIX) syntax.
#define ASSERT_STR_MATCHES(str, pattern) \
  ASSERT_THAT(str, testing::ContainsRegex(pattern))

#define ASSERT_STR_NOT_MATCHES(str, pattern) \
  ASSERT_THAT(str, testing::Not(testing::ContainsRegex(pattern)))

// Batched substring regular expressions in extended regex (POSIX) syntax.
//
// All strings must match the pattern.
#define ASSERT_STRINGS_ALL_MATCH(strings, pattern) do { \
  const auto& _strings = (strings); \
  const auto& _pattern = (pattern); \
  int _str_idx = 0; \
  for (const auto& str : _strings) { \
    ASSERT_STR_MATCHES(str, _pattern) \
        << "string " << _str_idx << ": pattern " << _pattern \
        << " does not match string " << str; \
    _str_idx++; \
  } \
} while (0)

// Batched substring regular expressions in extended regex (POSIX) syntax.
//
// At least one string must match the pattern.
#define ASSERT_STRINGS_ANY_MATCH(strings, pattern) do { \
  const auto& _strings = (strings); \
  const auto& _pattern = (pattern); \
  bool matched = false; \
  for (const auto& str : _strings) { \
    if (testing::internal::RE::PartialMatch(str, testing::internal::RE(_pattern))) { \
      matched = true; \
      break; \
    } \
  } \
  ASSERT_TRUE(matched) \
      << "not one string matched pattern " << _pattern; \
} while (0)

#define ASSERT_FILE_EXISTS(env, path) do { \
  const std::string& _s = path; \
  ASSERT_TRUE(env->FileExists(_s)) \
    << "Expected file to exist: " << _s; \
} while (0);

#define ASSERT_FILE_NOT_EXISTS(env, path) do { \
  const std::string& _s = path; \
  ASSERT_FALSE(env->FileExists(_s)) \
    << "Expected file not to exist: " << _s; \
} while (0);

#define CURRENT_TEST_NAME() \
  ::testing::UnitTest::GetInstance()->current_test_info()->name()

#define CURRENT_TEST_CASE_NAME() \
  ::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()

#endif
