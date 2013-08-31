// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <string>

#include <gtest/gtest.h>

#include "util/status.h"
#include "util/test_util.h"
#include "util/user.h"

namespace kudu {

using std::string;

class TestUser : public KuduTest {
};

// Validate that the current username is non-empty.
TEST_F(TestUser, TestNonEmpty) {
  string username;
  ASSERT_TRUE(username.empty());
  ASSERT_STATUS_OK(GetLoggedInUser(&username));
  ASSERT_FALSE(username.empty());
  LOG(INFO) << "Name of the current user is: " << username;
}

} // namespace kudu
