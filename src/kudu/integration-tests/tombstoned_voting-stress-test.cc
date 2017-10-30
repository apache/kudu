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
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/gutil/macros.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/external_mini_cluster_fs_inspector.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/util/condition_variable.h"
#include "kudu/util/monotime.h"
#include "kudu/util/mutex.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DEFINE_int32(test_num_iterations, 5,
             "Number of tombstoned voting stress test iterations");

using kudu::consensus::COMMITTED_OPID;
using kudu::consensus::OpId;
using kudu::itest::DeleteTablet;
using kudu::itest::TServerDetails;
using kudu::itest::WaitForServersToAgree;
using kudu::tablet::TABLET_DATA_TOMBSTONED;
using std::atomic;
using std::string;
using std::thread;
using std::unique_lock;
using std::vector;

namespace kudu {

static const MonoDelta kTimeout = MonoDelta::FromSeconds(30);

class TombstonedVotingStressTest : public ExternalMiniClusterITestBase {
 public:
  TombstonedVotingStressTest()
      : num_workers_(1),
        cond_all_workers_blocked_(&lock_),
        cond_workers_unblocked_(&lock_),
        current_term_(1) {
  }

 protected:
  enum State {
    kRunning,       // The tablet is running normally.
    kTombstoning,   // We are tombstoning the tablet.
    kTombstoned,    // The tombstoning is complete.
    kCopying,       // We are copying the tablet.
    kTestComplete,  // The test is complete and about to exit.
  };

  string State_Name(State state);

  // 1. Check if workers should block, block if required.
  // 2. Return current state.
  State GetState();

  // 1. Block worker threads.
  // 2. Wait for all workers to be blocked.
  // 3. Change state.
  // 4. Unblock workers.
  void SetState(State state);

  // Thread that loops and requests votes from TS1.
  void RunVoteRequestLoop();

  // Set-once shared state.
  string tablet_id_;
  OpId last_logged_opid_;

  Mutex lock_;
  const int num_workers_;
  int num_workers_blocked_ = 0;
  bool block_workers_ = false;
  ConditionVariable cond_all_workers_blocked_;  // Triggers once all worker threads are blocked.
  ConditionVariable cond_workers_unblocked_;    // Triggers when the workers become unblocked.

  // Protected by lock_.
  State state_ = kRunning;

  // State for the voter thread.
  atomic<int64_t> current_term_;
};

string TombstonedVotingStressTest::State_Name(State state) {
  switch (state) {
    case kRunning:
      return "kRunning";
    case kTombstoning:
      return "kTombstoning";
    case kTombstoned:
      return "kTombstoned";
    case kCopying:
      return "kCopying";
    case kTestComplete:
      return "kTestComplete";
    default:
      LOG(FATAL) << "Unknown state: " << state;
      __builtin_unreachable();
  }
}

TombstonedVotingStressTest::State TombstonedVotingStressTest::GetState() {
  unique_lock<Mutex> l(lock_);
  bool blocked = false;
  if (block_workers_) {
    num_workers_blocked_++;
    blocked = true;
    if (num_workers_blocked_ == num_workers_) {
      cond_all_workers_blocked_.Signal();
    }
  }
  while (block_workers_) {
    cond_workers_unblocked_.Wait();
  }
  if (blocked) num_workers_blocked_--;
  return state_;
}

void TombstonedVotingStressTest::SetState(State state) {
  // 1. Block worker threads.
  // 2. Wait for all workers to be blocked.
  // 3. Change state.
  // 4. Unblock workers.
  LOG(INFO) << "setting state to " << State_Name(state);
  unique_lock<Mutex> l(lock_);
  block_workers_ = true;
  while (num_workers_blocked_ != num_workers_) {
    cond_all_workers_blocked_.Wait();
  }
  state_ = state;
  block_workers_ = false;
  cond_workers_unblocked_.Broadcast();
}

void TombstonedVotingStressTest::RunVoteRequestLoop() {
  TServerDetails* ts1_ets = ts_map_[cluster_->tablet_server(1)->uuid()];
  while (true) {
    State state = GetState();
    if (state == kTestComplete) break;
    ++current_term_;
    Status s = itest::RequestVote(ts1_ets, tablet_id_, "A", current_term_, last_logged_opid_,
                                  /*ignore_live_leader=*/ true, /*is_pre_election=*/ false,
                                  kTimeout);
    switch (state) {
      case kRunning: FALLTHROUGH_INTENDED;
      case kTombstoned:
        // We should always be able to vote in this case.
        if (s.ok()) {
          LOG(INFO) << "Vote OK: state = " << state;
        } else {
          LOG(FATAL) << s.ToString() << ": tablet = " << tablet_id_ << ": state = " << state;
        }
        break;

      // The vote can fail while in the process of tombstoning a replica
      // because there is a small window of time where we have stopped
      // RaftConsensus but we haven't yet recorded the last-logged opid in the
      // tablet metadata.
      case kTombstoning: FALLTHROUGH_INTENDED;
      case kCopying:
        if (s.ok()) {
          LOG(INFO) << "Vote OK: state = " << state;
        } else {
          LOG(WARNING) << "Got bad vote while copying or tombstoning: " << s.ToString()
                       << ": state = " << state;
        }
        break;

      default:
        // We're shutting down.
        continue;
    }
    SleepFor(MonoDelta::FromMilliseconds(1)); // Don't run too hot.
  }
}

// Stress test for tombstoned voting, including tombstoning, deleting, and
// copying replicas.
TEST_F(TombstonedVotingStressTest, TestTombstonedVotingUnderStress) {
  // This test waits for several seconds, so only run it in slow mode.
  if (!AllowSlowTests()) return;

  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);

  // We want to control leader election manually and we only want 2 replicas.
  NO_FATALS(StartCluster({ "--enable_leader_failure_detection=false" },
                         { "--catalog_manager_wait_for_new_tablets_to_elect_leader=false",
                           "--allow_unsafe_replication_factor=true" },
                         /*num_tablet_servers=*/ 2));
  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(2); // Two servers and replicas makes the test easy to debug.
  workload.Setup();
  ASSERT_OK(inspect_->WaitForReplicaCount(2));

  // Figure out the tablet id.
  vector<string> tablets = inspect_->ListTabletsOnTS(1);
  ASSERT_EQ(1, tablets.size());
  tablet_id_ = tablets[0];

  for (int i = 1; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_OK(itest::WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()],
                                            tablet_id_, kTimeout));
    LOG(INFO) << "TabletReplica is RUNNING: T " << tablet_id_
              << " P " << cluster_->tablet_server(i)->uuid();
  }

  // Elect a leader and run some data through the cluster.
  LOG(INFO) << "electing a leader...";
  TServerDetails* ts0_ets = ts_map_[cluster_->tablet_server(0)->uuid()];
  TServerDetails* ts1_ets = ts_map_[cluster_->tablet_server(1)->uuid()];
  ASSERT_EVENTUALLY([&] {
    // The tablet can report that it's running but still be bootstrapping, so
    // retry until the election starts.
    ASSERT_OK(itest::StartElection(ts0_ets, tablet_id_, kTimeout));
  });

  LOG(INFO) << "loading data...";
  workload.Start();
  while (workload.rows_inserted() < 100) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();
  ASSERT_OK(WaitForServersToAgree(kTimeout, ts_map_, tablet_id_, workload.batches_completed()));
  ASSERT_OK(itest::GetLastOpIdForReplica(tablet_id_, ts0_ets, COMMITTED_OPID, kTimeout,
                                         &last_logged_opid_));

  // Have the leader step down so we can test voting on the other replica.
  // We don't shut this node down because it will serve as the tablet copy
  // "source" during the test.
  LOG(INFO) << "forcing leader to step down...";
  ASSERT_OK(itest::LeaderStepDown(ts0_ets, tablet_id_, kTimeout));

  // Now we are done with setup. Start the "stress" part of the test.
  // Startup the voting thread.
  LOG(INFO) << "starting stress thread...";
  thread voter_thread([this] { RunVoteRequestLoop(); });
  SCOPED_CLEANUP({
    SetState(kTestComplete);
    voter_thread.join();
  });

  int iter = 0;
  while (iter++ < FLAGS_test_num_iterations) {
    LOG(INFO) << "iteration " << (iter + 1) << " of " << FLAGS_test_num_iterations;
    // Loop on voting for a while in running state. We want to give an
    // opportunity for many votes during this time, and since voting involves
    // fsyncing to disk, we wait for plenty of time here (and below).
    SleepFor(MonoDelta::FromMilliseconds(500));

    // 1. Tombstone tablet.
    LOG(INFO) << "tombstoning tablet...";
    SetState(kTombstoning);
    ASSERT_OK(DeleteTablet(ts1_ets, tablet_id_, TABLET_DATA_TOMBSTONED, kTimeout));
    SetState(kTombstoned);

    // Loop on voting for a while in tombstoned state.
    SleepFor(MonoDelta::FromMilliseconds(500));

    // 2. Copy tablet.
    LOG(INFO) << "copying tablet...";
    HostPort source_hp;
    ASSERT_OK(HostPortFromPB(ts0_ets->registration.rpc_addresses(0), &source_hp));
    SetState(kCopying);
    ASSERT_OK(itest::StartTabletCopy(ts1_ets, tablet_id_, ts0_ets->uuid(), source_hp, current_term_,
                                     kTimeout));
    LOG(INFO) << "waiting for servers to agree...";
    ASSERT_OK(WaitForServersToAgree(kTimeout, ts_map_, tablet_id_, workload.batches_completed()));

    SetState(kRunning);
  }
}

} // namespace kudu
