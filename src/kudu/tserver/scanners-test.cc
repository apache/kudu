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
#include "kudu/tserver/scanners.h"

#include <memory>
#include <vector>

#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/remote_user.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/scanner_metrics.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

DECLARE_int32(scanner_ttl_ms);

namespace kudu {

using rpc::RemoteUser;
using std::vector;
using tablet::TabletReplica;

namespace tserver {

static const char* kUsername = "kudu-user";

TEST(ScannersTest, TestManager) {
  scoped_refptr<TabletReplica> null_replica(nullptr);
  ScannerManager mgr(nullptr);

  // Create two scanners, make sure their ids are different.
  RemoteUser user;
  user.SetUnauthenticated(kUsername);
  SharedScanner s1, s2;
  mgr.NewScanner(null_replica, user, RowFormatFlags::NO_FLAGS, &s1);
  mgr.NewScanner(null_replica, user, RowFormatFlags::NO_FLAGS, &s2);
  ASSERT_NE(s1->id(), s2->id());

  // Check that they're both registered.
  SharedScanner result;
  TabletServerErrorPB::Code error_code;
  ASSERT_OK(mgr.LookupScanner(s1->id(), kUsername, &error_code, &result));
  ASSERT_EQ(result.get(), s1.get());

  ASSERT_OK(mgr.LookupScanner(s2->id(), kUsername, &error_code, &result));
  ASSERT_EQ(result.get(), s2.get());

  // Check that looking up a bad scanner returns false.
  ASSERT_TRUE(mgr.LookupScanner("xxx", kUsername, &error_code, &result).IsNotFound());
  ASSERT_EQ(TabletServerErrorPB::SCANNER_EXPIRED, error_code);

  // Remove the scanners.
  ASSERT_TRUE(mgr.UnregisterScanner(s1->id()));
  ASSERT_TRUE(mgr.UnregisterScanner(s2->id()));

  // Removing a missing scanner should return false.
  ASSERT_FALSE(mgr.UnregisterScanner("xxx"));
}

TEST(ScannerTest, TestExpire) {
  scoped_refptr<TabletReplica> null_replica(nullptr);
  FLAGS_scanner_ttl_ms = 100;
  MetricRegistry registry;
  ScannerManager mgr(METRIC_ENTITY_server.Instantiate(&registry, "test"));
  SharedScanner s1, s2;
  mgr.NewScanner(null_replica, RemoteUser(), RowFormatFlags::NO_FLAGS, &s1);
  mgr.NewScanner(null_replica, RemoteUser(), RowFormatFlags::NO_FLAGS, &s2);
  SleepFor(MonoDelta::FromMilliseconds(200));
  {
    // Update the access time by locking and unlocking.
    auto access = s2->LockForAccess();
  }
  mgr.RemoveExpiredScanners();
  ASSERT_EQ(1, mgr.CountActiveScanners());
  ASSERT_EQ(1, mgr.metrics_->scanners_expired->value());
  vector<SharedScanner> active_scanners;
  mgr.ListScanners(&active_scanners);
  ASSERT_EQ(s2->id(), active_scanners[0]->id());
}

} // namespace tserver
} // namespace kudu
