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

#include <memory>
#include <string>

#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/port.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet.h"
#include "kudu/util/monotime.h"

DECLARE_int64(tablet_throttler_rpc_per_sec);
DECLARE_int64(tablet_throttler_bytes_per_sec);

namespace kudu {
namespace tablet {

class TestTabletThrottle : public KuduTabletTest {
 public:
  TestTabletThrottle()
   : KuduTabletTest(CreateBaseSchema()) {
  }

  virtual void SetUp() OVERRIDE {
    FLAGS_tablet_throttler_rpc_per_sec = 100;
    FLAGS_tablet_throttler_bytes_per_sec = 1000 * 1000;
    KuduTabletTest::SetUp();
  }

 private:
  Schema CreateBaseSchema() {
    return Schema({ ColumnSchema("key", INT32),
                    ColumnSchema("c1", INT32) }, 1);
  }
};

TEST_F(TestTabletThrottle, TestThrottle) {
  std::shared_ptr<Tablet> t = this->tablet();
  // Make sure token bucket filled up
  SleepFor(MonoDelta::FromMilliseconds(200));
  // Since we cannot change time here, it's expected
  // that the following 11 throttle call should complete
  // within 100ms, or the test will fail.
  for (int i = 0; i < 10; i++) {
    ASSERT_TRUE(t->ShouldThrottleAllow(1));
  }
  ASSERT_FALSE(t->ShouldThrottleAllow(1));
}

} // namespace tablet
} // namespace kudu

