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
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>

#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet.pb.h"  // IWYU pragma: keep
#include "kudu/util/status.h"

namespace kudu {
namespace cluster_summary {

// The result of health check on a tablet.
// Also used to indicate the health of a table, since the health of a table is
// the health of its least healthy tablet.
enum class HealthCheckResult {
  // The tablet is healthy.
  HEALTHY,

  // The tablet has on-going tablet copies.
  RECOVERING,

  // The tablet has fewer replicas than its table's replication factor and
  // has no on-going tablet copies.
  UNDER_REPLICATED,

  // The tablet is missing a majority of its replicas and is unavailable for
  // writes. If a majority cannot be brought back online, then the tablet
  // requires manual intervention to recover.
  UNAVAILABLE,

  // There was a discrepancy among the tablets' consensus configs and the master's.
  CONSENSUS_MISMATCH,
};

const char* const HealthCheckResultToString(HealthCheckResult cr);

// Possible types of consensus configs.
enum class ConsensusConfigType {
  // A config reported by the master.
  MASTER,
  // A config that has been committed.
  COMMITTED,
  // A config that has not yet been committed.
  PENDING,
};

// Representation of a consensus state.
struct ConsensusState {
  ConsensusState() = default;
  ConsensusState(ConsensusConfigType type,
                        boost::optional<int64_t> term,
                        boost::optional<int64_t> opid_index,
                        boost::optional<std::string> leader_uuid,
                        const std::vector<std::string>& voters,
                        const std::vector<std::string>& non_voters)
    : type(type),
      term(std::move(term)),
      opid_index(std::move(opid_index)),
      leader_uuid(std::move(leader_uuid)),
      voter_uuids(voters.cbegin(), voters.cend()),
      non_voter_uuids(non_voters.cbegin(), non_voters.cend()) {
   // A consensus state must have a term unless it's one sourced from the master.
   CHECK(type == ConsensusConfigType::MASTER || term);
  }

  // Two ConsensusState structs match if they have the same
  // leader_uuid, the same set of peers, and one of the following holds:
  // - at least one of them is of type MASTER
  // - they are configs of the same type and they have the same term
  bool Matches(const ConsensusState &other) const {
    bool same_leader_and_peers =
        leader_uuid == other.leader_uuid &&
        voter_uuids == other.voter_uuids &&
        non_voter_uuids == other.non_voter_uuids;
    if (type == ConsensusConfigType::MASTER ||
        other.type == ConsensusConfigType::MASTER) {
      return same_leader_and_peers;
    }
    return type == other.type && term == other.term && same_leader_and_peers;
  }

  ConsensusConfigType type;
  boost::optional<int64_t> term;
  boost::optional<int64_t> opid_index;
  boost::optional<std::string> leader_uuid;
  std::set<std::string> voter_uuids;
  std::set<std::string> non_voter_uuids;
};

// Represents the health of a server.
enum class ServerHealth {
  // The server is healthy.
  HEALTHY,

  // The server rejected attempts to communicate as unauthorized.
  UNAUTHORIZED,

  // The server can't be contacted.
  UNAVAILABLE,

  // The server reported an unexpected UUID.
  WRONG_SERVER_UUID,
};

// Return a string representation of 'sh'.
const char* const ServerHealthToString(ServerHealth sh);

// A summary of a server health check.
struct ServerHealthSummary {
  std::string uuid;
  std::string address;
  std::string ts_location;
  boost::optional<std::string> version;
  ServerHealth health = ServerHealth::HEALTHY;
  Status status = Status::OK();
};

// A summary of the state of a table.
struct TableSummary {
  std::string id;
  std::string name;
  int replication_factor = 0;
  int healthy_tablets = 0;
  int recovering_tablets = 0;
  int underreplicated_tablets = 0;
  int consensus_mismatch_tablets = 0;
  int unavailable_tablets = 0;

  int TotalTablets() const {
    return healthy_tablets + recovering_tablets + underreplicated_tablets +
        consensus_mismatch_tablets + unavailable_tablets;
  }

  int UnhealthyTablets() const {
    return TotalTablets() - healthy_tablets;
  }

  // Summarize the table's status with a HealthCheckResult.
  // A table's status is determined by the health of the least healthy tablet.
  HealthCheckResult TableStatus() const {
    if (unavailable_tablets > 0) {
      return HealthCheckResult::UNAVAILABLE;
    }
    if (consensus_mismatch_tablets > 0) {
      return HealthCheckResult::CONSENSUS_MISMATCH;
    }
    if (underreplicated_tablets > 0) {
      return HealthCheckResult::UNDER_REPLICATED;
    }
    if (recovering_tablets > 0) {
      return HealthCheckResult::RECOVERING;
    }
    return HealthCheckResult::HEALTHY;
  }
};

// Types of Kudu servers.
enum class ServerType {
  MASTER,
  TABLET_SERVER,
};

// Return a string representation of 'type'.
const char* const ServerTypeToString(ServerType type);

// A summary of the state of a tablet replica.
struct ReplicaSummary {
  std::string ts_uuid;
  boost::optional<std::string> ts_address;
  bool ts_healthy = false;
  bool is_leader = false;
  bool is_voter = false;
  tablet::TabletStatePB state = tablet::UNKNOWN;
  boost::optional<tablet::TabletStatusPB> status_pb;
  boost::optional<ConsensusState> consensus_state;
};

// A summary of the state of a tablet.
struct TabletSummary {
  std::string id;
  std::string table_id;
  std::string table_name;
  HealthCheckResult result;
  std::string status;
  ConsensusState master_cstate;
  std::vector<ReplicaSummary> replicas;
};

typedef std::map<std::string, ConsensusState> ConsensusStateMap;

// Container for information of cluster status.
struct ClusterStatus {

  // Health summaries for master and tablet servers.
  std::vector<ServerHealthSummary> master_summaries;
  std::vector<ServerHealthSummary> tserver_summaries;

  // Information about the master consensus configuration.
  std::vector<std::string> master_uuids;
  bool master_consensus_conflict = false;
  ConsensusStateMap master_consensus_state_map;

  // Detailed information about each table and tablet.
  // Tablet information includes consensus state.
  std::vector<TabletSummary> tablet_summaries;
  std::vector<TableSummary> table_summaries;
};

} // namespace cluster_summary
} // namespace kudu
