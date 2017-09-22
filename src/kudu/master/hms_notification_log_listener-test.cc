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

#include "kudu/master/hms_notification_log_listener.h"

#include <cstdint>
#include <thread>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_uint32(hive_metastore_notification_log_poll_period_seconds);
DECLARE_uint32(hive_metastore_notification_log_poll_inject_latency_ms);

namespace kudu {
namespace master {

class HmsNotificationLogListenerTest : public KuduTest {
 public:
  uint32_t poll_period_ = FLAGS_hive_metastore_notification_log_poll_period_seconds;
};

// Test that an immediate shutdown will short-circuit the poll period.
TEST_F(HmsNotificationLogListenerTest, TestImmediateShutdown) {
  HmsNotificationLogListenerTask listener(nullptr);
  ASSERT_OK(listener.Init());

  // Wait a bit to ensure the task thread enters the poll wait.
  SleepFor(MonoDelta::FromMilliseconds(100));

  MonoTime start = MonoTime::Now();
  listener.Shutdown();
  ASSERT_LT(MonoTime::Now() - start, MonoDelta::FromSeconds(poll_period_ / 2));
}

// Test that WaitForCatchUp will short-circuit the poll period.
TEST_F(HmsNotificationLogListenerTest, TestPoll) {
  HmsNotificationLogListenerTask listener(nullptr);
  ASSERT_OK(listener.Init());

  // Wait a bit to ensure the task thread enters the poll wait.
  SleepFor(MonoDelta::FromMilliseconds(100));

  ASSERT_OK(listener.WaitForCatchUp(MonoTime::Now() + MonoDelta::FromSeconds(poll_period_ / 2)));
}

// Test that WaitForCatchUp will short-circuit the poll period, even when the
// task is in the middle of polling when the wait initiates.
TEST_F(HmsNotificationLogListenerTest, TestWaitWhilePolling) {
  FLAGS_hive_metastore_notification_log_poll_inject_latency_ms = 100;

  HmsNotificationLogListenerTask listener(nullptr);
  ASSERT_OK(listener.Init());

  ASSERT_OK(listener.WaitForCatchUp(MonoTime::Now() + MonoDelta::FromSeconds(poll_period_ / 2)));
}

// Test that shutting down with a waiter will result in the waiter receiving an error.
TEST_F(HmsNotificationLogListenerTest, TestWaitAndShutdown) {
  // Inject some latency to ensure that the wait occurs when the task is
  // polling, otherwise it could immediately begin servicing the wait and not
  // actually see the shutdown.
  FLAGS_hive_metastore_notification_log_poll_inject_latency_ms = 100;

  HmsNotificationLogListenerTask listener(nullptr);
  ASSERT_OK(listener.Init());

  auto waiter = std::thread([&] {
      Status s = listener.WaitForCatchUp(MonoTime::Now() +
                                         MonoDelta::FromSeconds(poll_period_ / 2));
      CHECK(s.IsServiceUnavailable());
  });

  // There's a race between the waiter thread checking the closed_ flag in
  // WaitForCatchUp and this thread setting the flag in Shutdown. This test is
  // trying to excercise the case where the waiter is able to enqueue the
  // callback, so to make that more likely we slow down the call to Shutdown.
  SleepFor(MonoDelta::FromMilliseconds(10));

  listener.Shutdown();
  waiter.join();
}
} // namespace master
} // namespace kudu
