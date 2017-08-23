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

#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/schema.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/util/env.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"

METRIC_DECLARE_entity(server);
METRIC_DECLARE_gauge_uint64(log_block_manager_blocks_under_management);
METRIC_DECLARE_gauge_uint64(log_block_manager_bytes_under_management);
METRIC_DECLARE_gauge_uint64(log_block_manager_containers);
METRIC_DECLARE_gauge_uint64(log_block_manager_full_containers);
METRIC_DECLARE_gauge_uint64(threads_running);

DEFINE_bool(measure_startup_drop_caches, false,
            "Whether to drop kernel caches before measuring startup time. Must be root");
DEFINE_bool(measure_startup_sync, false,
            "Whether to call sync() before measuring startup time");
DEFINE_bool(measure_startup_wait_for_bootstrap, false,
            "Whether to wait for all tablets to finish bootstrapping when measuring startup time");
DEFINE_int32(num_columns, 100, "Number of columns in each tablet");
DEFINE_int32(num_seconds, 10, "Number of seconds to run the test");
DEFINE_int32(num_tablets, 100, "Number of tablets to create");
DEFINE_int32(max_blocks_per_container, 8, "Block number limit for each LBM container");
DEFINE_bool(enable_fsync, false, "Whether to enable fsync (disabled by default "
            "for all ExternalMiniCluster tests)");

namespace kudu {

using client::KuduColumnSchema;
using client::KuduSchema;
using client::KuduSchemaBuilder;
using std::pair;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

class DenseNodeTest : public ExternalMiniClusterITestBase {
};

// Integration test that simulates "dense" Kudu nodes.
//
// Storage heavy deployments can be created by running a data-intensive
// workload for a long time. But that's both time intensive and developer
// unfriendly. This test offers an alternative: a workload that produces a lot
// of metadata with a minimal amount of data. The scale of the metadata can
// proxy for data in areas we care about (such as start up time, thread count,
// memory usage, etc.).
TEST_F(DenseNodeTest, RunTest) {
  ExternalMiniClusterOptions opts;

  opts.extra_tserver_flags = {
      // Flush as fast as possible.
      "--flush_threshold_mb=1",
      "--flush_threshold_secs=1",

      // Don't preallocate anything, otherwise the massive number of LBM
      // containers and WAL segments are likely to fill the disk.
      "--log_preallocate_segments=false",
      "--log_async_preallocate_segments=false",
      "--log_container_preallocate_bytes=0",

      // No need for log retention, and this way we can save more disk space.
      "--log_min_segments_to_retain=1",

      // Drastically increase the number of LBM containers by limiting the
      // number of blocks in each.
      Substitute("--log_container_max_blocks=$0",
                 FLAGS_max_blocks_per_container),

      // UNDO delta block GC runs a lot to eagerly open newly created cfiles.
      // Disable it so we can maximize flushes and compactions.
      "--enable_undo_delta_block_gc=false",

      // Allow our single tserver to service many, many RPCs.
      "--rpc_service_queue_length=1000",

      // Inject steroids into the MM.
      "--maintenance_manager_num_threads=100",
      "--maintenance_manager_polling_interval_ms=1",

      // The tserver sometimes crashes with a SIGSEGV in the metrics logging
      // thread while trying to unwind a stack from within tcmalloc. It's
      // unclear as to why, but disabling the logging appears to fix it.
      "--metrics_log_interval_ms=0"
  };

  opts.extra_master_flags = {
      // The number of columns requested may be over the max. In case it is,
      // adjust the max upwards.
      Substitute("--max_num_columns=$0", FLAGS_num_columns)
  };

  if (FLAGS_enable_fsync) {
    opts.extra_tserver_flags.emplace_back("--never_fsync=false");
    opts.extra_master_flags.emplace_back("--never_fsync=false");
  }

  // With the amount of data we're going to write, we need to make sure the
  // tserver has enough time to start back up (startup is only considered to be
  // "complete" when the tserver has loaded all fs metadata from disk).
  opts.start_process_timeout = MonoDelta::FromSeconds(
      std::max(FLAGS_num_seconds * 10.0, opts.start_process_timeout.ToSeconds()));

  NO_FATALS(StartClusterWithOpts(std::move(opts)));

  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(1);

  // Use a custom schema with the number of requested columns.
  KuduSchema schema;
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  for (int i = 1; i < FLAGS_num_columns; i++) {
    b.AddColumn(Substitute("i$0", i))->Type(KuduColumnSchema::INT32)->NotNull();
  }
  CHECK_OK(b.Build(&schema));
  workload.set_schema(schema);

  workload.set_num_tablets(FLAGS_num_tablets);

  // These values are largely arbitrary. Experimentation revealed that they
  // tend to produce the greatest number of containers/blocks.
  workload.set_num_write_threads(1);
  workload.set_write_batch_size(1000);

  // The workload is likely to slow down and time out individual writes. Let's
  // not let that fail the test.
  workload.set_timeout_allowed(true);

  // Run the workload for the specified time period.
  workload.Setup();
  workload.Start();
  SleepFor(MonoDelta::FromSeconds(FLAGS_num_seconds));
  workload.StopAndJoin();

  // Collect some interesting metrics. The cluster is shut down before the
  // metrics are logged so that they're easier to find in the log output.
  vector<pair<string, int64_t>> metrics;
  for (const auto* m : { &METRIC_log_block_manager_blocks_under_management,
                         &METRIC_log_block_manager_bytes_under_management,
                         &METRIC_log_block_manager_containers,
                         &METRIC_log_block_manager_full_containers,
                         &METRIC_threads_running }) {
    int64_t value;
    ASSERT_OK(cluster_->tablet_server(0)->GetInt64Metric(&METRIC_ENTITY_server,
                                 "kudu.tabletserver",
                                 m,
                                 "value",
                                 &value));
    metrics.emplace_back(m->name(), value);
  }
  cluster_->Shutdown();

  // Start the cluster back up and measure how long it takes.

  // If requested, call sync() to force all dirty data to be written out.
  if (FLAGS_measure_startup_sync) {
    LOG_TIMING(INFO, "calling sync()") {
      sync();
    }
  }

  // If requested, force the kernel to drop its inode/dentry caches.
  if (FLAGS_measure_startup_drop_caches) {
    LOG_TIMING(INFO, "dropping kernel caches") {
      unique_ptr<WritableFile> f;
      WritableFileOptions opts;
      opts.mode = Env::OPEN_EXISTING;
      ASSERT_OK(env_->NewWritableFile(opts, "/proc/sys/vm/drop_caches", &f));
      ASSERT_OK(f->Append("3\n"));
      ASSERT_OK(f->Close());
    }
  }

  LOG_TIMING(INFO, "restarting master") {
    ASSERT_OK(cluster_->master()->Restart());
  }
  LOG_TIMING(INFO, "restarting tserver") {
    ASSERT_OK(cluster_->tablet_server(0)->Restart());
  }
  if (FLAGS_measure_startup_wait_for_bootstrap) {
    LOG_TIMING(INFO, "bootstrapping tablets") {
      LOG(INFO) << "waiting for tablets running";
      cluster_->WaitForTabletsRunning(cluster_->tablet_server(0),
                                      FLAGS_num_tablets,
                                      MonoDelta::FromSeconds(3600));
    }
  } else {
    LOG(INFO) << "not waiting for bootstrapping tablets (flag disabled)";
  }
  cluster_->Shutdown();

  for (const auto& p : metrics) {
    LOG(INFO) << p.first << ": " << p.second;
  }
}

} // namespace kudu
