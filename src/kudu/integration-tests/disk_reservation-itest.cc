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

#include <vector>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/util/metrics.h"

using std::string;
using std::vector;
using strings::Substitute;

METRIC_DECLARE_entity(server);
METRIC_DECLARE_gauge_uint64(data_dirs_full);

namespace kudu {

namespace {
Status GetTsCounterValue(ExternalTabletServer* ets, MetricPrototype* metric, int64_t* value) {
  return ets->GetInt64Metric(
             &METRIC_ENTITY_server,
             "kudu.tabletserver",
             metric,
             "value",
             value);
}
} // namespace

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

  NO_FATALS(StartCluster(ts_flags, {}, /* num_tablet_servers= */ 1, /* num_data_dirs= */ 2));

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
    ASSERT_OK(GetTsCounterValue(cluster_->tablet_server(0),
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
  Status s;
  for (int i = 0; i < 10; i++) {
    s = cluster_->tablet_server(0)->WaitForFatal(MonoDelta::FromSeconds(1));
    if (s.ok()) break;
    LOG(INFO) << "Rows inserted: " << workload.rows_inserted();
  }
  ASSERT_OK(s);
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
    "--log_compression_codec=none"
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

} // namespace kudu
