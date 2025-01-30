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

#include "kudu/util/scoped_cleanup.h"

#include <gtest/gtest.h>

namespace kudu {

TEST(ScopedCleanup, TestCleanup) {
  int var = 0;
  {
    auto saved = var;
    auto cleanup = MakeScopedCleanup([&] () { var = saved; });
    var = 42;
  }
  ASSERT_EQ(0, var);
}

TEST(ScopedCleanup, TestCleanupMacro) {
  int var = 0;
  {
    auto saved = var;
    SCOPED_CLEANUP({ var = saved; });
    var = 42;
  }
  ASSERT_EQ(0, var);
}

TEST(ScopedCleanup, TestCancelCleanup) {
  int var = 0;
  {
    auto saved = var;
    auto cleanup = MakeScopedCleanup([&] () { var = saved; });
    var = 42;
    cleanup.cancel();
  }
  ASSERT_EQ(42, var);
}

TEST(ScopedCleanup, ExplicitRun) {
  int var = 0;
  {
    auto saved = var;
    auto cleanup = MakeScopedCleanup([&] () { var = saved; });
    var = 42;
    cleanup.run();
    ASSERT_EQ(0, var);

    // Set 'saved' to 100 to distinguish between invoking the cleanup function
    // by the destructor and the explicit 'run' call.
    saved = 100;
  }
  // The function call in the destructor of the 'cleanup' shouldn't fire
  // after explicitly calling ScopedCleanup::run().
  ASSERT_EQ(0, var);
}

TEST(ScopedCleanup, CancelAndRun) {
  int var = 0;
  {
    auto saved = var;
    auto cleanup = MakeScopedCleanup([&] () { var = saved; });
    var = 42;
    cleanup.cancel();
    cleanup.run();
    ASSERT_EQ(42, var);

    // Set 'saved' to 100 to distinguish between invoking the cleanup function
    // by the destructor and the explicit 'run' call.
    saved = 100;
  }
  // The function call in the destructor of the 'cleanup' shouldn't have fired.
  ASSERT_EQ(42, var);
}

TEST(ScopedCleanup, ExplicitRunTwice) {
  int var = 0;
  {
    auto saved = var;
    auto cleanup = MakeScopedCleanup([&] () { var = saved; });
    var = 42;
    cleanup.run();
    ASSERT_EQ(0, var);

    // After explicitly running once, the cleanup function isn't called
    // upon subsequent invocations of ScopedCleanup::run().
    saved = 1;
    cleanup.run();
    ASSERT_EQ(0, var);

    // Set 'saved' to 100 to distinguish between invoking the cleanup function
    // by the destructor and the explicit 'run' call.
    saved = 100;
  }
  // The function call in the destructor of the 'cleanup' shouldn't have fired.
  ASSERT_EQ(0, var);
}

} // namespace kudu
