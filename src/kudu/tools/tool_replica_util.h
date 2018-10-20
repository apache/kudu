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
#include <memory>
#include <string>
#include <vector>

#include <boost/optional/optional.hpp>

#include "kudu/client/shared_ptr.h"
#include "kudu/consensus/consensus.pb.h"  // IWYU pragma: keep
#include "kudu/consensus/metadata.pb.h"
#include "kudu/util/status.h"

namespace kudu {

class HostPort;
class MonoDelta;

namespace client {
class KuduClient;
}

namespace consensus {
class ConsensusServiceProxy;
}

namespace tools {

// Fetch Raft consensus configuration from the 'replica_uuid' for the specified
// 'tablet_id'. The 'consensus_state' output parameter can be null.
Status GetConsensusState(
    const std::unique_ptr<consensus::ConsensusServiceProxy>& proxy,
    const std::string& tablet_id,
    const std::string& replica_uuid,
    const MonoDelta& timeout,
    consensus::ConsensusStatePB* consensus_state,
    bool* is_3_4_3_replication = nullptr);

// Request that the replica with UUID 'leader_uuid' step down.
// In GRACEFUL mode:
//   * If 'new_leader_uuid' is not boost::none, the leader will attempt
//     to gracefully transfer leadership to the replica with that UUID.
//   * If 'new_leader_uuid' is boost::none, the replica will choose its own
//     successor, preferring to transfer leadership ASAP.
// In ABRUPT mode, the replica will step down without arranging a successor.
// 'new_leader_uuid' has no effect in this mode and must be provided as
// boost::none.
// Note that in neither mode does this function guarantee that leadership will
// change, even if it returns OK. In ABRUPT mode, if the function succeeds,
// the leader will step down, but it may be reelected again. In GRACEFUL mode,
// the leader may not relinquish leadership at all, or it may and may be
// reelected, even if the function succeeds. The advantage of GRACEFUL mode is
// that it is on average less disruptive to tablet operations, particularly
// when the leadership transfer fails.
//
// If a caller wants to ensure leadership changes, it must wait and see if
// leadership changes as expected and, if not, retry.
Status DoLeaderStepDown(
    const std::string& tablet_id,
    const std::string& leader_uuid,
    const HostPort& leader_hp,
    consensus::LeaderStepDownMode mode,
    const boost::optional<std::string>& new_leader_uuid,
    const MonoDelta& timeout);

// Get information on the current leader replica for the specified tablet.
// The 'leader_uuid' and 'leader_hp' output parameters cannot be null. The
// 'is_no_leader' set to 'true' and the function returns Status::NotFound()
// if no replica is a leader for the tablet. The 'is_no_leader' output parameter
// can be null.
Status GetTabletLeader(
    const client::sp::shared_ptr<client::KuduClient>& client,
    const std::string& tablet_id,
    std::string* leader_uuid,
    HostPort* leader_hp,
    bool* is_no_leader = nullptr);

// Whether the replica move operation from 'from_ts_uuid' to 'to_ts_uuid'
// server is complete (i.e. succeeded of failed) for the tablet identified by
// 'tablet_id'. Neither 'is_complete' nor 'completion_status' output parameter
// can be null. If the function returns Status::OK() and the replica move is
// complete, the 'is_complete' parameter is set to 'true' and the
// 'completion_status' contains correspoding move status: Status::OK()
// if the move succeeded or non-OK status if it failed.
//
// The function returns Status::OK() if the 'is_complete' and
// 'completion_status' contain valid information, otherwise it returns first
// non-OK status encountered while trying to get status of the replica movement.
//
// If the source replica happens to be a leader, this function asks it to step
// down. Also, in case of 3-2-3 replica management mode, this function removes
// the source replica from the tablet's Raft consensus configuration.
Status CheckCompleteMove(
    const std::vector<std::string>& master_addresses,
    const client::sp::shared_ptr<client::KuduClient>& client,
    const std::string& tablet_id,
    const std::string& from_ts_uuid,
    const std::string& to_ts_uuid,
    bool* is_complete,
    Status* completion_status);

// Set the REPLACE attribute for the specified tablet replica. This is a no-op
// if the replica already has the REPLACE attribute set.
Status SetReplace(const client::sp::shared_ptr<client::KuduClient>& client,
                  const std::string& tablet_id,
                  const std::string& ts_uuid,
                  const boost::optional<int64_t>& cas_opid_idx,
                  bool* cas_failed = nullptr);

// Check if the replica of the tablet 'tablet_id' previously hosted by tserver
// identified by 'ts_uuid' is no longer hosted by the tablet server.
// If there was a problem checking if the replica is in the config, non-OK
// status is returned. On successful removal of the replica from the tablet
// server, Status::OK() is returned and 'is_complete' output parameter
// is set to 'true'. If the replica is still there but there was no error while
// checking for the status of the replica in the config, Status::OK() is
// returned and 'is_complete' is set to 'false'. The 'completion_status'
// parameter contains valid information only if 'is_complete' is set to 'true'.
Status CheckCompleteReplace(const client::sp::shared_ptr<client::KuduClient>& client,
                            const std::string& tablet_id,
                            const std::string& ts_uuid,
                            bool* is_complete,
                            Status* completion_status);

// Schedule replica move operation for tablet with 'tablet_id', moving replica
// from the tablet server 'from_ts_uuid' to tablet server 'to_ts_uuid'.
Status ScheduleReplicaMove(
    const std::vector<std::string>& master_addresses,
    const client::sp::shared_ptr<client::KuduClient>& client,
    const std::string& tablet_id,
    const std::string& from_ts_uuid,
    const std::string& to_ts_uuid);

// Run ksck for the tablet identified by 'tablet_id'.
Status DoKsckForTablet(
    const std::vector<std::string>& master_addresses,
    const std::string& tablet_id);

// Get RPC end-point for tablet server identifier by 'uuid'. The 'hp' output
// parameter cannot be null.
Status GetRpcAddressForTS(
    const client::sp::shared_ptr<client::KuduClient>& client,
    const std::string& uuid,
    HostPort* hp);

// Change Raft consensus configuration for the tablet with UUID 'replica_uuid'.
Status DoChangeConfig(const std::vector<std::string>& master_addresses,
    const std::string& tablet_id,
    const std::string& replica_uuid,
    const boost::optional<consensus::RaftPeerPB::MemberType>& member_type,
    consensus::ChangeConfigType cc_type,
    const boost::optional<int64_t>& cas_opid_idx = boost::none,
    bool* cas_failed = nullptr);

// Check whether the cluster with the specified master addresses supports
// the 3-4-3 replica management scheme. Returns Status::Incomplete() if
// there is not enough information available to determine the replica management
// scheme (e.g., not a single table exists in the cluster). If the identifier
// of an existing tablet is known, pass it via the 'tablet_id_in' parameter.
// The 'is_343_scheme' out parameter is set to 'true' if the cluster uses
// the 3-4-3 replica management scheme.
Status Is343SchemeCluster(const std::vector<std::string>& master_addresses,
                          const boost::optional<std::string>& tablet_id_in,
                          bool* is_343_scheme);

} // namespace tools
} // namespace kudu
