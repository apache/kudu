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

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/raft_consensus-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/monotime.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

// Binaries built in the address- and thread-sanitizer build configurations
// run much slower than binaries built in debug and release configurations.
// The paramters are adjusted accordingly to avoid test flakiness.
#if defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER)
constexpr int kBuildCfgFactor = 2;
#else
constexpr int kBuildCfgFactor = 1;
#endif

DEFINE_bool(test_raft_prepare_replacement_before_eviction, true,
            "When enabled, failed replicas will only be evicted after a "
            "replacement has been prepared for them.");
DEFINE_double(test_tablet_copy_early_session_timeout_prob,
              0.25 / kBuildCfgFactor,
              "The probability that a tablet copy session will time out early, "
              "resulting in a tablet copy failure.");
DEFINE_int32(test_follower_unavailable_considered_failed_sec,
             1 * kBuildCfgFactor,
             "Seconds that a leader tablet replica is unable to successfully "
             "heartbeat to a follower after which the follower is considered "
             "to be failed and evicted from the config.");
DEFINE_int32(test_heartbeat_interval_ms,
             250 * kBuildCfgFactor,
             "Interval at which the TS heartbeats to the master.");
DEFINE_int32(test_max_ksck_failures, 50,
             "Maximum number of ksck failures in a row to tolerate before "
             "considering the test as failed.");
// GLOG_FATAL:    3
// GLOG_ERROR:    2
// GLOG_WARNING:  1
// GLOG_INFO:     0
DEFINE_int32(test_minloglevel, google::GLOG_ERROR,
             "Logging level for masters and tablet servers under test.");
DEFINE_int32(test_num_iterations,
             10 / kBuildCfgFactor,
             "Number of iterations, repeating the test scenario.");
DEFINE_int32(test_num_replicas_per_server,
             20 / kBuildCfgFactor,
             "Number of tablets per server to create.");
DEFINE_int32(test_raft_heartbeat_interval_ms,
             100 * kBuildCfgFactor,
             "The Raft heartbeat interval for tablet servers under the test.");
DEFINE_int32(test_replication_factor, 3,
             "The replication factor of the test table.");

DECLARE_int32(num_replicas);
DECLARE_int32(num_tablet_servers);

using kudu::cluster::ExternalTabletServer;
using kudu::itest::StartElection;
using kudu::itest::TServerDetails;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tserver {

class RaftConsensusStressITest : public RaftConsensusITestBase {
};

// Test scenario to verify the behavior of the system when tablet replicas fail
// and are replaced again and again. With high enough number of iterations, at
// some point all replacement replicas are placed on top of previously
// tombstoned ones.
TEST_F(RaftConsensusStressITest, RemoveReplaceInCycle) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  const bool is_343_scheme = FLAGS_test_raft_prepare_replacement_before_eviction;
  const int kReplicaUnavailableSec = FLAGS_test_follower_unavailable_considered_failed_sec;
  const int kReplicationFactor = FLAGS_test_replication_factor;
  const int kNumTabletServers = 2 * kReplicationFactor;
  const int kNumReplicasPerServer = FLAGS_test_num_replicas_per_server;
  const int kNumTablets = kNumTabletServers * kNumReplicasPerServer / kReplicationFactor;
  const MonoDelta kTimeout = MonoDelta::FromSeconds(60 * kBuildCfgFactor);
  const MonoDelta kShortTimeout = MonoDelta::FromSeconds(1 * kBuildCfgFactor);

  // This test scenario induces a lot of faults/errors and it runs multiple
  // iterations. Tablet servers and master are too chatty in this case, logging
  // a lot of information. Setting --minloglevel=2 allow for logging only of
  // error and fatal messages from tablet servers and masters.
  const vector<string> kMasterFlags = {
    Substitute("--minloglevel=$0", FLAGS_test_minloglevel),
    Substitute("--raft_prepare_replacement_before_eviction=$0", is_343_scheme),
    Substitute("--max_create_tablets_per_ts=$0", kNumTablets),
  };
  const vector<string> kTserverFlags = {
    Substitute("--minloglevel=$0", FLAGS_test_minloglevel),
    Substitute("--tablet_copy_early_session_timeout_prob=$0",
               FLAGS_test_tablet_copy_early_session_timeout_prob),
    Substitute("--raft_prepare_replacement_before_eviction=$0", is_343_scheme),
    Substitute("--follower_unavailable_considered_failed_sec=$0",
               kReplicaUnavailableSec),
    Substitute("--consensus_rpc_timeout_ms=$0", 1000 * kReplicaUnavailableSec),
    Substitute("--heartbeat_interval_ms=$0", FLAGS_test_heartbeat_interval_ms),
    Substitute("--raft_heartbeat_interval_ms=$0", FLAGS_test_raft_heartbeat_interval_ms),
  };

  FLAGS_num_replicas = kReplicationFactor;
  FLAGS_num_tablet_servers = kNumTabletServers;
  NO_FATALS(BuildAndStart(kTserverFlags, kMasterFlags));

  TestWorkload workload(cluster_.get());
  workload.set_table_name("RemoveReplaceInCycle");
  // Keep half of the total avaialable 'location space' for the replacement
  // replicas spawned by the scenario below.
  workload.set_num_tablets(kNumTablets);
  workload.set_num_replicas(kReplicationFactor);
  workload.set_num_write_threads(1);
  workload.set_write_timeout_millis(kTimeout.ToMilliseconds());
  workload.set_timeout_allowed(true);
  // TODO(KUDU-1188): start using at least one read thread once leader leases
  //                  are implemented. Without leader leases, keeping a reader
  //                  thread leads to intermittent failures due to the CHECK()
  //                  assertion in test_workload.cc:243 with messages like:
  //
  // Check failed: row_count >= expected_row_count (31049 vs. 31050)
  //
  workload.set_num_read_threads(0);
  workload.set_client_default_admin_operation_timeout_millis(kTimeout.ToMilliseconds());
  workload.Setup();
  workload.Start();
  while (workload.rows_inserted() < 100L * kNumTablets) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();

  std::atomic<bool> is_running(true);
  std::atomic<bool> do_elections(true);
  std::atomic<bool> do_pauses(true);
  std::thread pause_and_resume_thread([&] {
    // Select random tablet server and pause it for some time to make the system
    // spawn the replacement replica elsewhere.
    int prev_ts_idx = -1;
    while (is_running) {
      const int ts_idx = rand() % cluster_->num_tablet_servers();
      if (ts_idx == prev_ts_idx) {
        continue;
      }
      ExternalTabletServer* ext_ts = cluster_->tablet_server(ts_idx);
      TServerDetails* ts = tablet_servers_[ext_ts->uuid()];
      vector<string> tablet_ids;
      Status s = ListRunningTabletIds(ts, kShortTimeout, &tablet_ids);
      if (s.IsNetworkError() || tablet_ids.empty()) {
        continue;
      }
      CHECK_OK(s);
      prev_ts_idx = ts_idx;
      if (do_pauses) {
        CHECK_OK(ext_ts->Pause());
        SleepFor(MonoDelta::FromSeconds(3 * kReplicaUnavailableSec));
        CHECK_OK(ext_ts->Resume());
      }
      SleepFor(MonoDelta::FromMilliseconds(250));
    }
  });
  std::thread election_thread([&] {
    while (is_running) {
      if (do_elections) {
        const auto ts_idx = rand() % cluster_->num_tablet_servers();
        ExternalTabletServer* ext_ts = cluster_->tablet_server(ts_idx);
        TServerDetails* ts = tablet_servers_[ext_ts->uuid()];
        vector<string> tablet_ids;
        Status s = ListRunningTabletIds(ts, kShortTimeout, &tablet_ids);
        if (s.IsNetworkError() || s.IsTimedOut() || tablet_ids.empty()) {
          continue;
        }
        CHECK_OK(s);
        const auto tablet_idx = rand() % tablet_ids.size();
        // Best effort attempt: ignoring the result of StartElection() call.
        StartElection(ts, tablet_ids[tablet_idx], kShortTimeout);
      }
      SleepFor(kShortTimeout);
    }
  });
  SCOPED_CLEANUP({
    is_running = false;
    pause_and_resume_thread.join();
    election_thread.join();
  });

  auto ksck_failures_in_a_row = 0;
  int64_t rows_inserted = workload.rows_inserted();
  for (auto iteration = 0; iteration < FLAGS_test_num_iterations; ) {
    workload.Start();
    while (workload.rows_inserted() < rows_inserted + 10) {
      SleepFor(MonoDelta::FromMilliseconds(1));
    }
    workload.StopAndJoin();
    rows_inserted = workload.rows_inserted();

    ClusterVerifier v(cluster_.get());
    // Set shorter timeouts for the verification to abort earlier
    // and signal the actor threads to stop messing with the tablets.
    v.SetOperationsTimeout(kShortTimeout);
    v.SetVerificationTimeout(kShortTimeout);

    const auto& s = v.RunKsck();
    if (!s.ok()) {
      do_elections = false;
      do_pauses = false;
      if (!s.IsTimedOut()) {
        ++ksck_failures_in_a_row;
      }
      if (ksck_failures_in_a_row > FLAGS_test_max_ksck_failures) {
        break;
      }
      SleepFor(MonoDelta::FromMilliseconds(FLAGS_test_raft_heartbeat_interval_ms));
      continue;
    }
    ksck_failures_in_a_row = 0;
    do_elections = true;
    do_pauses = true;

    SleepFor(MonoDelta::FromSeconds(1));
    LOG(INFO) << "completed iteration " << iteration;
    ++iteration;
  }
  is_running = false;

  NO_FATALS(cluster_->AssertNoCrashes());

  ClusterVerifier v(cluster_.get());
  v.SetOperationsTimeout(kTimeout);
  v.SetVerificationTimeout(kTimeout);
  if (ksck_failures_in_a_row > FLAGS_test_max_ksck_failures) {
    // Suspecting a Raft consensus failure while running ksck with shorter
    // timeout (see above). Run an extra round of ksck with the regular timeout
    // to verify that replicas haven't really converged and, if so, just bail
    // right at this point.
    const auto& s = v.RunKsck();
    if (!s.ok()) {
      FAIL() << Substitute("$0: tablet replicas haven't converged", s.ToString());
    }
  }

  NO_FATALS(v.CheckCluster());
  // Using ClusterVerifier::AT_LEAST because the TestWorkload instance was run
  // with the 'timeout_allowed' option enabled. In that case, the actual actual
  // number of inserted rows may be greater than workload reports via its
  // rows_inserted() method.
  NO_FATALS(v.CheckRowCount(workload.table_name(),
                            ClusterVerifier::AT_LEAST,
                            workload.rows_inserted()));
}

}  // namespace tserver
}  // namespace kudu
