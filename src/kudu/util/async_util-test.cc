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

#include "kudu/util/async_util.h"

#include <unistd.h>

#include <functional>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/callback.h"
#include "kudu/util/monotime.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::thread;
using std::vector;

namespace kudu {

class AsyncUtilTest : public KuduTest {
 public:
  AsyncUtilTest() {
    // Set up an alarm to fail the test in case of deadlock.
    alarm(30);
  }
  virtual ~AsyncUtilTest() {
    // Disable the alarm on test exit.
    alarm(0);
  }
};

// Test completing the synchronizer through each of the APIs it exposes.
TEST_F(AsyncUtilTest, TestSynchronizerCompletion) {
  Synchronizer sync;

  {
    auto waiter = thread([sync] {
        ignore_result(sync.Wait());
    });
    SleepFor(MonoDelta::FromMilliseconds(5));
    sync.StatusCB(Status::OK());
    waiter.join();
  }
  sync.Reset();
  {
    auto cb = sync.AsStatusCallback();
    auto waiter = thread([sync] {
        ignore_result(sync.Wait());
    });
    SleepFor(MonoDelta::FromMilliseconds(5));
    cb.Run(Status::OK());
    waiter.join();
  }
  sync.Reset();
  {
    auto cb = sync.AsStdStatusCallback();
    auto waiter = thread([sync] {
        ignore_result(sync.Wait());
    });
    SleepFor(MonoDelta::FromMilliseconds(5));
    cb(Status::OK());
    waiter.join();
  }
}

TEST_F(AsyncUtilTest, TestSynchronizerMultiWait) {
  Synchronizer sync;
  vector<thread> waiters;
  for (int i = 0; i < 5; i++) {
    waiters.emplace_back([sync] {
        ignore_result(sync.Wait());
    });
  }
  SleepFor(MonoDelta::FromMilliseconds(5));
  sync.StatusCB(Status::OK());

  for (auto& waiter : waiters) {
    waiter.join();
  }
}

// Flavors of wait that Synchronizer is capable of: WaitFor() or WaitUntil().
enum class TimedWaitFlavor {
  WaitFor,
  WaitUntil,
};

class AsyncUtilTimedWaitTest:
    public AsyncUtilTest,
    public ::testing::WithParamInterface<TimedWaitFlavor> {
};

TEST_P(AsyncUtilTimedWaitTest, SynchronizerTimedWaitSuccess) {
  const auto kWaitInterval = MonoDelta::FromMilliseconds(1000);

  Synchronizer sync;
  auto cb = sync.AsStatusCallback();
  auto waiter = thread([cb] {
    SleepFor(MonoDelta::FromMilliseconds(5));
    cb.Run(Status::OK());
  });
  SCOPED_CLEANUP({
    waiter.join();
  });
  const auto mode = GetParam();
  switch (mode) {
    case TimedWaitFlavor::WaitFor:
      ASSERT_OK(sync.WaitFor(kWaitInterval));
      break;
    case TimedWaitFlavor::WaitUntil:
      ASSERT_OK(sync.WaitUntil(MonoTime::Now() + kWaitInterval));
      break;
    default:
      FAIL() << "unsupported wait mode " << static_cast<int>(mode);
      break;
  }
}

TEST_P(AsyncUtilTimedWaitTest, SynchronizerTimedWaitTimeout) {
  const auto kWaitInterval = MonoDelta::FromMilliseconds(5);

  Synchronizer sync;
  auto cb = sync.AsStatusCallback();
  auto waiter = thread([cb] {
    SleepFor(MonoDelta::FromMilliseconds(1000));
    cb.Run(Status::OK());
  });
  SCOPED_CLEANUP({
    waiter.join();
  });
  const auto mode = GetParam();
  switch (mode) {
    case TimedWaitFlavor::WaitFor:
      ASSERT_TRUE(sync.WaitFor(kWaitInterval).IsTimedOut());
      break;
    case TimedWaitFlavor::WaitUntil:
      ASSERT_TRUE(sync.WaitUntil(MonoTime::Now() + kWaitInterval).IsTimedOut());
      break;
    default:
      FAIL() << "unsupported wait mode " << static_cast<int>(mode);
      break;
  }
}

INSTANTIATE_TEST_CASE_P(WaitFlavors,
                        AsyncUtilTimedWaitTest,
                        ::testing::Values(TimedWaitFlavor::WaitFor,
                                          TimedWaitFlavor::WaitUntil));

} // namespace kudu
