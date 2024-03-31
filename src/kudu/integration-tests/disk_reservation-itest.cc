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

#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::cluster::ExternalMiniClusterOptions;
using kudu::itest::GetInt64Metric;
using std::string;
using std::vector;
using strings::Substitute;

METRIC_DECLARE_entity(server);
METRIC_DECLARE_gauge_int64(wal_dir_space_available_bytes);
METRIC_DECLARE_gauge_int64(data_dirs_space_available_bytes);
METRIC_DECLARE_gauge_uint64(data_dirs_full);

namespace kudu {

class DiskReservationITest : public ExternalMiniClusterITestBase {
};

// Test that when we fill up a disk beyond its configured reservation limit, we
// use other disks for data blocks until all disks are full, at which time we
// crash. This functionality is only implemented in the log block manager.
TEST_F(DiskReservationITest, TestFillMultipleDisks) {
  vector<string> ts_flags;
  // Don't preallocate very many bytes so we run the "full disk" check often.
  ts_flags.emplace_back("--log_container_preallocate_bytes=100000");
  // Set up the tablet so that flushes are constantly occurring.
  ts_flags.emplace_back("--flush_threshold_mb=0");
  ts_flags.emplace_back("--maintenance_manager_polling_interval_ms=50");
  ts_flags.emplace_back("--disable_core_dumps");
  // Reserve one byte so that when we simulate 0 bytes free below, we'll start
  // failing requests.
  ts_flags.emplace_back("--fs_data_dirs_reserved_bytes=1");

  ExternalMiniClusterOptions opts;
  opts.extra_tserver_flags = std::move(ts_flags);
  opts.num_data_dirs = 2;
  NO_FATALS(StartClusterWithOpts(opts));

  ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(0),
      "disk_reserved_override_prefix_1_path_for_testing",
                                cluster_->GetDataPath("ts-0", 0)));
  ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(0),
      "disk_reserved_override_prefix_2_path_for_testing",
                                cluster_->GetDataPath("ts-0", 1)));

  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(1);
  workload.set_num_write_threads(4);
  workload.set_write_batch_size(10);
  workload.set_payload_bytes(1024);
  workload.set_timeout_allowed(true);
  workload.set_write_timeout_millis(500);
  workload.Setup();
  workload.Start();

  // Simulate that /data-0 has 0 bytes free.
  ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(0),
                              "disk_reserved_override_prefix_1_bytes_free_for_testing", "0"));
  // Simulate that /data-1 has 1GB free.
  ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(0),
                              "disk_reserved_override_prefix_2_bytes_free_for_testing",
                              Substitute("$0", 1L * 1024 * 1024 * 1024)));

  // Wait until we have one full data dir.
  while (true) {
    int64_t num_full_data_dirs;
    ASSERT_OK(itest::GetTsCounterValue(cluster_->tablet_server(0),
                                       &METRIC_data_dirs_full,
                                       &num_full_data_dirs));
    if (num_full_data_dirs >= 1) break;
    SleepFor(MonoDelta::FromMilliseconds(10));
  }

  LOG(INFO) << "Have 1 full data dir";

  // Now simulate that all disks are full.
  ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(0),
                              "disk_reserved_override_prefix_2_bytes_free_for_testing", "0"));

  // Wait for crash due to inability to flush or compact.
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(cluster_->tablet_server(0)->WaitForFatal(MonoDelta::FromSeconds(1)));
  });
  workload.StopAndJoin();
}

// When the WAL disk goes beyond its configured reservation, attempts to write
// to the WAL should cause a fatal error.
TEST_F(DiskReservationITest, TestWalWriteToFullDiskAborts) {
  vector<string> ts_flags = {
    // Encourage log rolling to speed up the test.
    "--log_segment_size_mb=1",
    // We crash on purpose, so no need to dump core.
    "--disable_core_dumps",
    // Disable compression so that our data being written doesn't end up
    // compressed away.
    "--log_compression_codec=no_compression"
  };
  NO_FATALS(StartCluster(ts_flags, {}, 1));

  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(1);
  workload.set_timeout_allowed(true); // Allow timeouts because we expect the server to crash.
  workload.set_write_timeout_millis(500); // Keep test time low after crash.
  // Write lots of data to quickly fill up our 1mb log segment size.
  workload.set_num_write_threads(4);
  workload.set_write_batch_size(10);
  workload.set_payload_bytes(1000);
  workload.Setup();
  workload.Start();

  // Ensure the cluster is running, the client was able to look up the tablet
  // locations, etc.
  while (workload.rows_inserted() < 10) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }

  // Set the disk to "nearly full" which should eventually cause a crash at WAL
  // preallocation time.
  ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(0),
                              "fs_wal_dir_reserved_bytes", "10000000"));
  ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(0),
                              "disk_reserved_bytes_free_for_testing", "10000001"));

  ASSERT_OK(cluster_->tablet_server(0)->WaitForFatal(MonoDelta::FromSeconds(10)));
  workload.StopAndJoin();
}

// Make sure the metrics on the available space in the WAL and the data
// directories behave consistently with and without space reservations.
TEST_F(DiskReservationITest, AvailableSpaceMetrics) {
  constexpr const char* const kMetricValue = "value";

  // To speed up the test, do not cache the metrics on the available disk space.
  const vector<string> ts_flags = {
    "--fs_data_dirs_available_space_cache_seconds=0",
    "--fs_wal_dir_available_space_cache_seconds=0",
  };
  NO_FATALS(StartCluster(ts_flags, {}, 1));

  auto* ts = cluster_->tablet_server(0);
  DCHECK_NE(nullptr, ts);
  const auto& addr = ts->bound_http_hostport();

  auto space_getter_data_dirs = [&](int64_t* available_bytes) {
    return GetInt64Metric(addr,
                          &METRIC_ENTITY_server,
                          nullptr,
                          &METRIC_data_dirs_space_available_bytes,
                          kMetricValue,
                          available_bytes);
  };
  auto space_getter_wal_dir = [&](int64_t* available_bytes) {
    return GetInt64Metric(addr,
                          &METRIC_ENTITY_server,
                          nullptr,
                          &METRIC_wal_dir_space_available_bytes,
                          kMetricValue,
                          available_bytes);
  };

  constexpr const char* const kFlagDataReserved = "fs_data_dirs_reserved_bytes";
  constexpr const char* const kFlagWalReserved = "fs_wal_dir_reserved_bytes";

  // Make sure metrics capture non-negative numbers if the space reservations
  // use their default settings: 1% of the available disk space for the WAL
  // and the data directories.
  {
    int64_t data_dirs_space_bytes = -1;
    ASSERT_OK(space_getter_data_dirs(&data_dirs_space_bytes));
    ASSERT_GE(data_dirs_space_bytes, 0);

    int64_t wal_dir_space_bytes = -1;
    ASSERT_OK(space_getter_wal_dir(&wal_dir_space_bytes));
    ASSERT_GE(wal_dir_space_bytes, 0);
  }

  // Set space reservation to 0 bytes.
  ASSERT_OK(cluster_->SetFlag(ts, kFlagDataReserved, "0"));
  ASSERT_OK(cluster_->SetFlag(ts, kFlagWalReserved, "0"));

  int64_t data_dirs_space_bytes_no_reserve = -1;
  ASSERT_OK(space_getter_data_dirs(&data_dirs_space_bytes_no_reserve));
  ASSERT_GE(data_dirs_space_bytes_no_reserve, 0);

  int64_t wal_dir_space_bytes_no_reserve = -1;
  ASSERT_OK(space_getter_wal_dir(&wal_dir_space_bytes_no_reserve));
  ASSERT_GE(wal_dir_space_bytes_no_reserve, 0);

  // Set space reservation to 1027 GiB: this is to test for integer overflow
  // in the related code. Essentially, it would be enough to set the reservation
  // to anything beyond 2 GiB. However, to make this test scenario stable upon
  // concurrent activity at the test node, it's necessary to have a wide margin
  // for the comparison between the available space metrics for data/WAL
  // directories with&without space reservation.
  constexpr const char* const k1027GiB = "1102732853248";
  ASSERT_OK(cluster_->SetFlag(ts, kFlagDataReserved, k1027GiB));
  ASSERT_OK(cluster_->SetFlag(ts, kFlagWalReserved, k1027GiB));

  int64_t data_dirs_space_bytes_reserve = -1;
  ASSERT_OK(space_getter_data_dirs(&data_dirs_space_bytes_reserve));
  ASSERT_GE(data_dirs_space_bytes_reserve, 0);

  int64_t wal_dir_space_bytes_reserve = -1;
  ASSERT_OK(space_getter_wal_dir(&wal_dir_space_bytes_reserve));
  ASSERT_GE(wal_dir_space_bytes_reserve, 0);

  // The available space without reservation should not be less than
  // the available space with reservation.
  ASSERT_GE(data_dirs_space_bytes_no_reserve, data_dirs_space_bytes_reserve);
  ASSERT_GE(wal_dir_space_bytes_no_reserve, wal_dir_space_bytes_reserve);
}

} // namespace kudu
