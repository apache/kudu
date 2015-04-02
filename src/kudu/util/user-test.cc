// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
// All rights reserved.

#include <string>

#include <gtest/gtest.h>

#include "kudu/util/status.h"
#include "kudu/util/test_util.h"
#include "kudu/util/user.h"

namespace kudu {

using std::string;

class TestUser : public KuduTest {
};

// Validate that the current username is non-empty.
TEST_F(TestUser, TestNonEmpty) {
  string username;
  ASSERT_TRUE(username.empty());
  ASSERT_OK(GetLoggedInUser(&username));
  ASSERT_FALSE(username.empty());
  LOG(INFO) << "Name of the current user is: " << username;
}

} // namespace kudu
