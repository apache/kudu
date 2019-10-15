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
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;
using std::vector;

METRIC_DECLARE_entity(server);
METRIC_DECLARE_gauge_uint64(generic_current_allocated_bytes);
METRIC_DECLARE_gauge_uint64(tcmalloc_pageheap_free_bytes);

namespace kudu {

using cluster::ExternalMiniClusterOptions;
using cluster::ExternalTabletServer;

namespace {
void GetOverheadRatio(ExternalTabletServer* ets, double* ratio) {
  CHECK(ratio);

  int64_t generic_current_allocated_bytes = 0;
  int64_t tcmalloc_pageheap_free_bytes = 0;
  ASSERT_OK(itest::GetTsCounterValue(ets, &METRIC_generic_current_allocated_bytes,
                                     &generic_current_allocated_bytes));
  ASSERT_OK(itest::GetTsCounterValue(ets, &METRIC_tcmalloc_pageheap_free_bytes,
                                     &tcmalloc_pageheap_free_bytes));
  ASSERT_GT(generic_current_allocated_bytes, 0);
  ASSERT_GE(tcmalloc_pageheap_free_bytes, 0);
  *ratio = static_cast<double>(tcmalloc_pageheap_free_bytes) / generic_current_allocated_bytes;
}
} // namespace

class MemoryGcITest : public ExternalMiniClusterITestBase {
};

TEST_F(MemoryGcITest, TestPeriodicGc) {
  vector<string> ts_flags;
  // Set GC interval seconeds short enough, so the test case could compelte sooner.
  ts_flags.emplace_back("--gc_tcmalloc_memory_interval_seconds=5");

  ExternalMiniClusterOptions opts;
  opts.extra_tserver_flags = std::move(ts_flags);
  opts.num_tablet_servers = 3;
  NO_FATALS(StartClusterWithOpts(opts));

  // Enable tcmalloc memory GC periodically for tserver-1, and disabled for tserver-0 and tserver-2.
  ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(0),
                              "gc_tcmalloc_memory_interval_seconds", "0"));
  ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(1),
                              "gc_tcmalloc_memory_interval_seconds", "1"));
  ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(2),
                              "gc_tcmalloc_memory_interval_seconds", "0"));

  // Write some data for scan later.
  {
    TestWorkload workload(cluster_.get());
    workload.set_num_replicas(1);
    workload.set_num_write_threads(4);
    workload.set_write_batch_size(10);
    workload.set_payload_bytes(1024);
    workload.Setup();
    workload.Start();
    ASSERT_EVENTUALLY([&]() {
      ASSERT_GE(workload.rows_inserted(), 1000000);
    });
    workload.StopAndJoin();
  }

  // Start scan, then more memory will be allocated by tcmalloc.
  {
    TestWorkload workload(cluster_.get());
    workload.set_num_write_threads(0);
    workload.set_num_read_threads(4);
    workload.Setup();
    workload.Start();
    ASSERT_EVENTUALLY([&]() {
      NO_FATALS(
        double ratio;
        GetOverheadRatio(cluster_->tablet_server(0), &ratio);
        ASSERT_GE(ratio, 0.1) << "tserver-0";
        GetOverheadRatio(cluster_->tablet_server(1), &ratio);
        ASSERT_LE(ratio, 0.1) << "tserver-1";
        GetOverheadRatio(cluster_->tablet_server(2), &ratio);
        ASSERT_GE(ratio, 0.1) << "tserver-2";
      );
    });
    workload.StopAndJoin();
  }
}

} // namespace kudu
