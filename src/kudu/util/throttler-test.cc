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

#include "kudu/util/throttler.h"

#include <string>

#include <gtest/gtest.h>

#include "kudu/util/monotime.h"
#include "kudu/util/test_util.h"

namespace kudu {

class ThrottlerTest : public KuduTest {
};

TEST_F(ThrottlerTest, Basic) {
  Throttler t(1, 1, 1.0);
  ASSERT_TRUE(t.Take(0, 1));
  ASSERT_TRUE(t.Take(1, 0));
}

TEST_F(ThrottlerTest, OpThrottle) {
  // Check operation rate throttling
  MonoTime now = MonoTime::Now();
  Throttler t0(now, 1000, 1000*1000, 1);
  // Fill up bucket
  now += MonoDelta::FromMilliseconds(2000);
  // Check throttle behavior for 1 second.
  for (int p = 0; p < 10; p++) {
    for (int i = 0; i < 100; i++) {
      ASSERT_TRUE(t0.Take(now, 1, 1));
    }
    ASSERT_FALSE(t0.Take(now, 1, 1));
    now += MonoDelta::FromMicroseconds(Throttler::kRefillPeriodMicros);
  }
}

TEST_F(ThrottlerTest, IOThrottle) {
  // Check IO rate throttling
  MonoTime now = MonoTime::Now();
  Throttler t0(now, 50000, 1000*1000, 1);
  // Fill up bucket
  now += MonoDelta::FromMilliseconds(2000);
  // Check throttle behavior for 1 second.
  for (int p = 0; p < 10; p++) {
    for (int i = 0; i < 100; i++) {
      ASSERT_TRUE(t0.Take(now, 1, 1000));
    }
    ASSERT_FALSE(t0.Take(now, 1, 1000));
    now += MonoDelta::FromMilliseconds(Throttler::kRefillPeriodMicros);
  }
}

TEST_F(ThrottlerTest, Burst) {
  // Check throttling for bursty consuming
  MonoTime now = MonoTime::Now();
  Throttler t0(now, 2000, 1000*1000, 5);
  // Fill up bucket
  now += MonoDelta::FromMilliseconds(2000);
  for (int i = 0; i < 100; i++) {
    now += MonoDelta::FromMilliseconds(1);
    ASSERT_TRUE(t0.Take(now, 1, 5000));
  }
  ASSERT_TRUE(t0.Take(now, 1, 100000));
  ASSERT_FALSE(t0.Take(now, 1, 1));
}

} // namespace kudu
