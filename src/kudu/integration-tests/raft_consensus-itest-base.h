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

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "kudu/integration-tests/ts_itest-base.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/status.h"

namespace kudu {

namespace cluster {
class ExternalTabletServer;
}

namespace tserver {

class TabletServerServiceProxy;

// Integration test for the raft consensus implementation.
// Uses the whole tablet server stack with ExternalMiniCluster.
class RaftConsensusITestBase : public TabletServerIntegrationTestBase {
 public:
  RaftConsensusITestBase();

  void SetUp() override;

  void ScanReplica(TabletServerServiceProxy* replica_proxy,
                   std::vector<std::string>* results);

  void InsertTestRowsRemoteThread(uint64_t first_row,
                                  uint64_t count,
                                  uint64_t num_batches,
                                  const std::vector<CountDownLatch*>& latches);
 protected:
  // Retrieve the current term of the first tablet on this tablet server.
  static Status GetTermMetricValue(cluster::ExternalTabletServer* ts,
                                   int64_t* term);

  // Flags needed for CauseFollowerToFallBehindLogGC() to work well.
  static void AddFlagsForLogRolls(std::vector<std::string>* extra_tserver_flags);

  // Pause one of the followers and write enough data to the remaining replicas
  // to cause log GC, then resume the paused follower. On success,
  // 'leader_uuid' will be set to the UUID of the leader, 'orig_term' will be
  // set to the term of the leader before un-pausing the follower, and
  // 'fell_behind_uuid' will be set to the UUID of the follower that was paused
  // and caused to fall behind. These can be used for verification purposes.
  //
  // Certain flags should be set. You can add the required flags with
  // AddFlagsForLogRolls() before starting the cluster.
  void CauseFollowerToFallBehindLogGC(std::string* leader_uuid,
                                      int64_t* orig_term,
                                      std::string* fell_behind_uuid);

  CountDownLatch inserters_;
};

}  // namespace tserver
}  // namespace kudu
