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

// Request current leader replica 'leader_uuid' to step down.
Status DoLeaderStepDown(
    const std::string& tablet_id,
    const std::string& leader_uuid,
    const HostPort& leader_hp,
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
// non-OK status encountered while trying to status of the replica move.
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

} // namespace tools
} // namespace kudu
