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

#include <iosfwd>
#include <map>
#include <set>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>

#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet.pb.h"  // IWYU pragma: keep
#include "kudu/util/status.h"

namespace kudu {
namespace tools {

// The result of health check on a tablet.
// Also used to indicate the health of a table, since the health of a table is
// the health of its least healthy tablet.
enum class KsckCheckResult {
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

const char* const KsckCheckResultToString(KsckCheckResult cr);

// Possible types of consensus configs.
enum class KsckConsensusConfigType {
  // A config reported by the master.
  MASTER,
  // A config that has been committed.
  COMMITTED,
  // A config that has not yet been committed.
  PENDING,
};

// Representation of a consensus state.
struct KsckConsensusState {
  KsckConsensusState() = default;
  KsckConsensusState(KsckConsensusConfigType type,
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
   CHECK(type == KsckConsensusConfigType::MASTER || term);
  }

  // Two KsckConsensusState structs match if they have the same
  // leader_uuid, the same set of peers, and one of the following holds:
  // - at least one of them is of type MASTER
  // - they are configs of the same type and they have the same term
  bool Matches(const KsckConsensusState &other) const {
    bool same_leader_and_peers =
        leader_uuid == other.leader_uuid &&
        voter_uuids == other.voter_uuids &&
        non_voter_uuids == other.non_voter_uuids;
    if (type == KsckConsensusConfigType::MASTER ||
        other.type == KsckConsensusConfigType::MASTER) {
      return same_leader_and_peers;
    }
    return type == other.type && term == other.term && same_leader_and_peers;
  }

  KsckConsensusConfigType type;
  boost::optional<int64_t> term;
  boost::optional<int64_t> opid_index;
  boost::optional<std::string> leader_uuid;
  std::set<std::string> voter_uuids;
  std::set<std::string> non_voter_uuids;
};

// Represents the health of a server.
enum class KsckServerHealth {
  // The server is healthy.
  HEALTHY,

  // The server can't be contacted.
  UNAVAILABLE,

  // The server reported an unexpected UUID.
  WRONG_SERVER_UUID,
};

// Return a string representation of 'sh'.
const char* const ServerHealthToString(KsckServerHealth sh);

// Returns an int signifying the "unhealthiness level" of 'sh'.
// 0 means healthy; higher values are unhealthier.
// Useful for sorting or comparing.
int ServerHealthScore(KsckServerHealth sh);

// A summary of a server health check.
struct KsckServerHealthSummary {
  std::string uuid;
  std::string address;
  KsckServerHealth health = KsckServerHealth::HEALTHY;
  Status status = Status::OK();
};

// A summary of the state of a table.
struct KsckTableSummary {
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

  // Summarize the table's status with a KsckCheckResult.
  // A table's status is determined by the health of the least healthy tablet.
  KsckCheckResult TableStatus() const {
    if (unavailable_tablets > 0) {
      return KsckCheckResult::UNAVAILABLE;
    }
    if (consensus_mismatch_tablets > 0) {
      return KsckCheckResult::CONSENSUS_MISMATCH;
    }
    if (underreplicated_tablets > 0) {
      return KsckCheckResult::UNDER_REPLICATED;
    }
    if (recovering_tablets > 0) {
      return KsckCheckResult::RECOVERING;
    }
    return KsckCheckResult::HEALTHY;
  }
};

// Types of Kudu servers.
enum class KsckServerType {
  MASTER,
  TABLET_SERVER,
};

// Return a string representation of 'type'.
const char* const ServerTypeToString(KsckServerType type);

// A summary of the state of a tablet replica.
struct KsckReplicaSummary {
  std::string ts_uuid;
  boost::optional<std::string> ts_address;
  bool ts_healthy = false;
  bool is_leader = false;
  bool is_voter = false;
  tablet::TabletStatePB state = tablet::UNKNOWN;
  boost::optional<tablet::TabletStatusPB> status_pb;
  boost::optional<KsckConsensusState> consensus_state;
};

// A summary of the state of a tablet.
struct KsckTabletSummary {
  std::string id;
  std::string table_id;
  std::string table_name;
  KsckCheckResult result;
  std::string status;
  KsckConsensusState master_cstate;
  std::vector<KsckReplicaSummary> replica_infos;
};

// The result of a checksum on a tablet replica.
struct KsckReplicaChecksum {
  std::string ts_address;
  std::string ts_uuid;
  Status status;
  uint64_t checksum = 0;
};

// The result of a tablet checksum scan.
struct KsckTabletChecksum {
  bool mismatch = false;
  std::string tablet_id;
  std::map<std::string, KsckReplicaChecksum> replica_checksums;
};

// The results of a checksum operation on a whole table.
typedef std::map<std::string, KsckTabletChecksum> KsckTableChecksum;

typedef std::map<std::string, KsckTableChecksum> KsckTableChecksumMap;

struct KsckChecksumResults {
  boost::optional<uint64_t> snapshot_timestamp;
  KsckTableChecksumMap tables;
};

enum class PrintMode {
  DEFAULT,
  // Print all results, including for healthy tablets.
  VERBOSE,
};

typedef std::map<std::string, KsckConsensusState> KsckConsensusStateMap;

// Container for all the results of a series of ksck checks.
struct KsckResults {
  // Collection of error status for failed checks. Used to print out a final
  // summary of all failed checks.
  // All checks passed if and only if this vector is empty.
  std::vector<Status> error_messages;

  // Health summaries for master and tablet servers.
  std::vector<KsckServerHealthSummary> master_summaries;
  std::vector<KsckServerHealthSummary> tserver_summaries;

  // Information about the master consensus configuration.
  std::vector<std::string> master_uuids;
  bool master_consensus_conflict = false;
  KsckConsensusStateMap master_consensus_state_map;

  // Detailed information about each table and tablet.
  // Tablet information includes consensus state.
  std::vector<KsckTabletSummary> tablet_summaries;
  std::vector<KsckTableSummary> table_summaries;

  // Collected results of the checksum scan.
  KsckChecksumResults checksum_results;

  // Print this KsckResults to 'out', according to the PrintMode 'mode'.
  Status PrintTo(PrintMode mode, std::ostream& out);
};

// Print a formatted health summary to 'out', given a list `summaries`
// describing the health of servers of type 'type'.
Status PrintServerHealthSummaries(KsckServerType type,
                                  const std::vector<KsckServerHealthSummary>& summaries,
                                  std::ostream& out);

// Print a formatted summary of the tables in 'table_summaries' to 'out'.
Status PrintTableSummaries(const std::vector<KsckTableSummary>& table_summaries,
                           std::ostream& out);

// Print a formatted summary of the tablets in 'tablet_summaries' to 'out'.
Status PrintTabletSummaries(const std::vector<KsckTabletSummary>& tablet_summaries,
                            PrintMode mode,
                            std::ostream& out);

// Print to 'out' a "consensus matrix" that compares the consensus states of the
// replicas on servers with ids in 'server_uuids', given the set of consensus
// states in 'consensus_states'. If given, 'ref_cstate' will be used as the
// master's point of view of the consensus state of the tablet.
Status PrintConsensusMatrix(const std::vector<std::string>& server_uuids,
                            const boost::optional<KsckConsensusState> ref_cstate,
                            const KsckConsensusStateMap& consensus_states,
                            std::ostream& out);

Status PrintChecksumResults(const KsckChecksumResults& checksum_results,
                            std::ostream& out);

Status PrintTotalCounts(const KsckResults& results, std::ostream& out);

} // namespace tools
} // namespace kudu
