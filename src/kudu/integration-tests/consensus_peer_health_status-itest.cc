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
#include <ostream>
#include <string>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/raft_consensus-itest-base.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int32(num_replicas);
DECLARE_int32(num_tablet_servers);

using kudu::consensus::ConsensusStatePB;
using kudu::consensus::HealthReportPB;
using kudu::consensus::INCLUDE_HEALTH_REPORT;
using kudu::consensus::RaftPeerPB;
using kudu::itest::TServerDetails;
using kudu::tablet::TABLET_DATA_TOMBSTONED;
using kudu::tserver::RaftConsensusITestBase;
using std::string;
using std::vector;

namespace kudu {

class ConsensusPeerHealthStatusITest : public RaftConsensusITestBase {
};

// This is a functional test that verifies that when a replica goes into a bad
// state, its health status is detected by the leader replica.
TEST_F(ConsensusPeerHealthStatusITest, TestPeerHealthStatusTransitions) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "Test disabled in fast test mode. Set KUDU_ALLOW_SLOW_TESTS=1 to enable.";
    return;
  }

  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  const vector<string> kMasterFlags = {
    // Disable re-replication and eviction for this test because we are
    // verifying that putting a follower into a particular state is detected by
    // the leader.
    "--master_add_server_when_underreplicated=false",
    "--catalog_manager_evict_excess_replicas=false",
    // Leader elections are handled manually in this test.
    "--catalog_manager_wait_for_new_tablets_to_elect_leader=false",
  };
  vector<string> ts_flags {
    // Disable failure detection to ensure a stable test environment for health
    // status testing.
    "--enable_leader_failure_detection=false",
    // We want to keep RPC timeouts short, to quickly detect downed servers,
    // which will put the health status into an UKNOWN state until the point
    // where they are considered FAILED, which we also control here.
    "--consensus_rpc_timeout_ms=2000",
    "--follower_unavailable_considered_failed_sec=6",
  };
  AddFlagsForLogRolls(&ts_flags); // For CauseFollowerToFallBehindLogGC().
  ASSERT_EQ(3, FLAGS_num_tablet_servers);
  ASSERT_EQ(3, FLAGS_num_replicas);
  NO_FATALS(BuildAndStart(ts_flags, kMasterFlags));

  vector<TServerDetails*> tservers;
  AppendValuesFromMap(tablet_servers_, &tservers);
  ASSERT_EQ(3, tservers.size());

  // We'll use TS 0 as the leader and TS 1 as the follower to "abuse" in this test.
  TServerDetails* leader_ts = tservers[0];
  TServerDetails* follower_ts = tservers[1];
  const string& follower_uuid = follower_ts->uuid();

  // Elect a leader and wait for log index 1 to propagate to all servers.
  ASSERT_OK(StartElection(leader_ts, tablet_id_, kTimeout));
  ASSERT_OK(WaitForServersToAgree(kTimeout, tablet_servers_, tablet_id_, /*minimum_index=*/ 1));

  // Wait for the specified follower to get into the specified health state.
  auto wait_for_health_state = [&](TServerDetails* leader_ts, const string& peer_uuid,
                                   HealthReportPB::HealthStatus expected_health_status) {
    ASSERT_EVENTUALLY([&]{
      ConsensusStatePB cstate;
      ASSERT_OK(GetConsensusState(leader_ts, tablet_id_, kTimeout, INCLUDE_HEALTH_REPORT, &cstate));
      bool found_replica = false;
      for (const auto& peer : cstate.committed_config().peers()) {
        if (peer.permanent_uuid() != peer_uuid) continue;
        found_replica = true;
        ASSERT_TRUE(peer.has_health_report());
        auto health_status = peer.health_report().overall_health();
        ASSERT_EQ(expected_health_status, health_status)
            << "expected: " << HealthReportPB::HealthStatus_Name(expected_health_status) << ", "
            << "actual: " << HealthReportPB::HealthStatus_Name(health_status);
        break;
      }
      ASSERT_TRUE(found_replica) << "No replica found with UUID " << peer_uuid;
    });
  };

  // Convenience constants.
  constexpr auto UNKNOWN = HealthReportPB::UNKNOWN;
  constexpr auto HEALTHY = HealthReportPB::HEALTHY;
  constexpr auto FAILED = HealthReportPB::FAILED;
  constexpr auto FAILED_UNRECOVERABLE = HealthReportPB::FAILED_UNRECOVERABLE;

  NO_FATALS(wait_for_health_state(leader_ts, follower_uuid, HEALTHY));

  LOG(INFO) << "Test: HEALTHY -> UNKNOWN -> HEALTHY";

  // Make the follower unavailable for long enough to be considered to have
  // UNKNOWN health status.
  cluster_->tablet_server_by_uuid(follower_uuid)->Shutdown();
  NO_FATALS(wait_for_health_state(leader_ts, follower_uuid, UNKNOWN));

  // Recover back to HEALTHY.
  ASSERT_OK(cluster_->tablet_server_by_uuid(follower_uuid)->Restart());
  NO_FATALS(wait_for_health_state(leader_ts, follower_uuid, HEALTHY));

  LOG(INFO) << "Test: HEALTHY -> UNKNOWN -> FAILED -> HEALTHY";

  enum ErrorType {
    PAUSE,
    SHUTDOWN
  };

  // Ensure that both SIGSTOP and process shutdown result first in UNKNOWN and
  // then in FAILED states.
  for (auto error_type : { PAUSE, SHUTDOWN }) {
    // Fail the follower. Track the transitions: first through UNKNOWN state,
    // then into FAILED.
    if (error_type == SHUTDOWN) {
      cluster_->tablet_server_by_uuid(follower_uuid)->Shutdown();
    } else {
      ASSERT_OK(cluster_->tablet_server_by_uuid(follower_uuid)->Pause());
    }
    NO_FATALS(wait_for_health_state(leader_ts, follower_uuid, UNKNOWN));
    NO_FATALS(wait_for_health_state(leader_ts, follower_uuid, FAILED));

    // Recover back to HEALTHY.
    if (error_type == SHUTDOWN) {
      ASSERT_OK(cluster_->tablet_server_by_uuid(follower_uuid)->Restart());
    } else {
      ASSERT_OK(cluster_->tablet_server_by_uuid(follower_uuid)->Resume());
    }
    NO_FATALS(wait_for_health_state(leader_ts, follower_uuid, HEALTHY));
  }

  LOG(INFO) << "Test: HEALTHY -> FAILED_UNRECOVERABLE";

  NO_FATALS(CauseSpecificFollowerToFallBehindLogGC(tablet_servers_, follower_uuid));
  NO_FATALS(wait_for_health_state(leader_ts, follower_uuid, FAILED_UNRECOVERABLE));

  LOG(INFO) << "Recovering test replica...";

  // The master isn't managing re-replication in this test (see gflags set at
  // the start of the test) so we manually simulate re-replication here to
  // recover from an "unrecoverable" replica state.
  //
  // Manually evict the failed follower from the config, wait for the master to
  // delete the replica, then manually add the same tablet server back to the
  // config in order to bring the replica on 'follower_ts' back to a HEALTHY
  // state.
  ASSERT_OK(RemoveServer(leader_ts, tablet_id_, follower_ts, kTimeout));
  ASSERT_EVENTUALLY([&] {
    vector<tserver::ListTabletsResponsePB::StatusAndSchemaPB> tablets;
    ASSERT_OK(WaitForNumTabletsOnTS(follower_ts, /*count=*/ 1, kTimeout, &tablets));
    ASSERT_EQ(1, tablets.size());
    ASSERT_EQ(TABLET_DATA_TOMBSTONED, tablets[0].tablet_status().tablet_data_state());
  });
  ASSERT_OK(AddServer(leader_ts, tablet_id_, follower_ts, RaftPeerPB::VOTER, kTimeout));
  // Make sure the tablet copying (initiated by AddServer() above) finishes.
  // Otherwise, the abandoned tablet copying session at the source tablet server
  // might anchor WAL segments and they would not be GCed as the scneario below
  // expects.
  ASSERT_OK(WaitUntilTabletRunning(follower_ts, tablet_id_, kTimeout));

  LOG(INFO) << "Test: HEALTHY -> UNKNOWN -> FAILED -> FAILED_UNRECOVERABLE";
  NO_FATALS(wait_for_health_state(leader_ts, follower_uuid, HEALTHY));
  cluster_->tablet_server_by_uuid(follower_uuid)->Shutdown();
  NO_FATALS(wait_for_health_state(leader_ts, follower_uuid, UNKNOWN));
  NO_FATALS(wait_for_health_state(leader_ts, follower_uuid, FAILED));
  NO_FATALS(CauseSpecificFollowerToFallBehindLogGC(tablet_servers_, follower_uuid,
                                                   /*leader_uuid=*/ nullptr, /*orig_term=*/ nullptr,
                                                   DO_NOT_TAMPER));
  NO_FATALS(wait_for_health_state(leader_ts, follower_uuid, FAILED_UNRECOVERABLE));
}

} // namespace kudu
