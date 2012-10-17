// Copyright 2010 Google Inc.  All Rights Reserved
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
//

#include "gutil/exception/failureor.h"

#include <string>
using std::string;

#include "gutil/scoped_ptr.h"

//#include "testing/base/public/googletest.h"
//#include "testing/base/public/gunit.h"
#include <gtest/gtest.h>

namespace common {

class Exception {
 public:
  explicit Exception(const string& message) : message_(message) {}
  const string& PrintStackTrace() const { return message_; }
 private:
  string message_;
};

class ExceptionTest : public testing::Test {};
class ExceptionDeathTest : public testing::Test {};

TEST_F(ExceptionTest, CheckedFlagResetsOnCopyingAndAssignment) {
  internal::CheckedFlag a;
  EXPECT_FALSE(a.get());
  a.set();
  EXPECT_TRUE(a.get());
  internal::CheckedFlag b = a;
  EXPECT_TRUE(a.get());
  EXPECT_FALSE(b.get());
  internal::CheckedFlag c;
  EXPECT_FALSE(c.get());
  c = a;
  EXPECT_FALSE(c.get());
  EXPECT_TRUE(a.get());
}

TEST_F(ExceptionTest, FailureOrWorksOnSuccess) {
  FailureOr<int, Exception> result = Success(5);
  ASSERT_TRUE(result.is_success());
  EXPECT_FALSE(result.is_failure());
  EXPECT_EQ(5, result.get());
  EXPECT_EQ(5, SucceedOrDie(result));
}

TEST_F(ExceptionTest, FailureOrReferenceWorksOnSuccess) {
  int a = 5;
  FailureOrReference<int, Exception> result = Success(a);
  ASSERT_TRUE(result.is_success());
  EXPECT_FALSE(result.is_failure());
  EXPECT_EQ(5, result.get());
  EXPECT_EQ(5, SucceedOrDie(result));
  a = 7;
  EXPECT_EQ(7, result.get());
}

TEST_F(ExceptionTest, FailureOrOwnedWorksOnSuccess) {
  int* result_content = new int(5);
  FailureOrOwned<int, Exception> result = Success(result_content);
  ASSERT_TRUE(result.is_success());
  EXPECT_FALSE(result.is_failure());
  EXPECT_EQ(5, *result.get());
  EXPECT_EQ(5, *result);
  EXPECT_EQ(result, result_content);
  scoped_ptr<int> not_result_content(new int(5));
  EXPECT_NE(result, not_result_content.get());
}

TEST_F(ExceptionTest, FailureOrOwnedReleasesResult) {
  scoped_ptr<int> value(
      SucceedOrDie(FailureOrOwned<int, Exception>(Success(new int(7)))));
  EXPECT_EQ(7, *value);
}

template<typename ResultType> void TestThatResultWorksOnFailure() {
  ResultType result = Failure(new Exception("foo"));
  ASSERT_FALSE(result.is_success());
  EXPECT_TRUE(result.is_failure());
  EXPECT_EQ("foo", result.exception().PrintStackTrace());
}

TEST_F(ExceptionTest, ResultWorksOnFailure) {
  TestThatResultWorksOnFailure<FailureOr<int, Exception> >();
  TestThatResultWorksOnFailure<FailureOrOwned<int, Exception> >();
  TestThatResultWorksOnFailure<FailureOrReference<int, Exception> >();
  TestThatResultWorksOnFailure<FailureOrVoid<Exception> >();
}

template<typename ResultType> void TestThatResultReleasesException() {
  ResultType result = Failure(new Exception("foo"));
  ASSERT_TRUE(result.is_failure());
  scoped_ptr<Exception> exception(result.release_exception());
  EXPECT_TRUE(result.is_failure());  // Releasing doesn't clear the status.
  EXPECT_EQ("foo", exception->PrintStackTrace());
}

TEST_F(ExceptionTest, ResultReleasesException) {
  TestThatResultReleasesException<FailureOr<int, Exception> >();
  TestThatResultReleasesException<FailureOrOwned<int, Exception> >();
  TestThatResultReleasesException<FailureOrReference<int, Exception> >();
  TestThatResultReleasesException<FailureOrVoid<Exception> >();
}

}  // namespace common
