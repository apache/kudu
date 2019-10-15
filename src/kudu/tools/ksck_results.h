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
#include <iosfwd>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>

#include "kudu/master/master.pb.h"
#include "kudu/rebalance/cluster_status.h"
#include "kudu/tablet/tablet.pb.h"  // IWYU pragma: keep
#include "kudu/util/status.h"

namespace kudu {
namespace tools {

class KsckResultsPB;

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
  // Print results in pretty-printed JSON format.
  JSON_PRETTY,
  // Print results in compact JSON format. Differs from JSON_PRETTY only in
  // format, not content.
  JSON_COMPACT,
  // Print results in plain text, focusing on errors and omitting most
  // information about healthy tablets.
  PLAIN_CONCISE,
  // Print results in plain text.
  PLAIN_FULL,
};

// It's a convenient method to use `struct ... enum ...` here to keep
// in a standalone namespace and support some bit operators on this type.
struct PrintSections {
  enum Values {
    NONE = 0,
    MASTER_SUMMARIES = 1 << 0,
    TSERVER_STATES = 1 << 1,
    TSERVER_SUMMARIES = 1 << 2,
    VERSION_SUMMARIES = 1 << 3,
    TABLET_SUMMARIES = 1 << 4,
    TABLE_SUMMARIES = 1 << 5,
    CHECKSUM_RESULTS = 1 << 6,
    TOTAL_COUNT = 1 << 7,

    // Print all sections above.
    ALL_SECTIONS = 0b011111111
  };
};

// A flag and its value.
typedef std::pair<std::string, std::string> KsckFlag;

// Map (flag name, flag value) -> server uuids with --flag=value.
typedef std::map<KsckFlag, std::vector<std::string>> KsckFlagToServersMap;

// Convenience map flag name -> flag tags.
typedef std::unordered_map<std::string, std::string> KsckFlagTagsMap;

// Convenience map version -> servers.
typedef std::map<std::string, std::vector<std::string>> KsckVersionToServersMap;

typedef std::map<std::string, master::TServerStatePB> KsckTServerStateMap;

// Container for all the results of a series of ksck checks.
struct KsckResults {

  cluster_summary::ClusterStatus cluster_status;

  // Collection of error status for failed checks. Used to print out a final
  // summary of all failed checks.
  // All checks passed if and only if this vector is empty.
  std::vector<Status> error_messages;

  // Collection of warnings from checks.
  // These errors are not considered to indicate an unhealthy cluster,
  // so they do not cause ksck to report an error.
  std::vector<Status> warning_messages;

  // Version summaries for master and tablet servers.
  KsckVersionToServersMap version_summaries;

  // Information about the flags of masters and tablet servers.
  KsckFlagToServersMap master_flag_to_servers_map;
  KsckFlagTagsMap master_flag_tags_map;
  KsckFlagToServersMap tserver_flag_to_servers_map;
  KsckFlagTagsMap tserver_flag_tags_map;

  // Any special states that the tablet servers may be in.
  KsckTServerStateMap ts_states;

  // Collected results of the checksum scan.
  KsckChecksumResults checksum_results;

  // Print this KsckResults to 'out' according to the PrintMode 'mode'.
  // The sections printed will be limited according to the value of 'sections'.
  Status PrintTo(PrintMode mode, int sections, std::ostream& out);

  // Print this KsckResults to 'out' in JSON format.
  // 'mode' must be PrintMode::JSON_PRETTY or PrintMode::JSON_COMPACT.
  // The sections printed will be limited according to the value of 'sections'.
  Status PrintJsonTo(PrintMode mode, int sections, std::ostream& out) const;

  void ToPb(KsckResultsPB* pb, int sections) const;
};

// Print a formatted health summary to 'out', given a list `summaries`
// describing the health of servers of type 'type'.
Status PrintServerHealthSummaries(
    cluster_summary::ServerType type,
    const std::vector<cluster_summary::ServerHealthSummary>& summaries,
    std::ostream& out);

// Print a formatted summary of the flags in 'flag_to_servers_map', indicating
// which servers have which (flag, value) pairs set.
// Flag tag information is sourced from 'flag_tags_map'.
Status PrintFlagTable(cluster_summary::ServerType type,
                      int num_servers,
                      const KsckFlagToServersMap& flag_to_servers_map,
                      const KsckFlagTagsMap& flag_tags_map,
                      std::ostream& out);

Status PrintTServerStatesTable(const KsckTServerStateMap& ts_states,
                               std::ostream& out);

// Print a summary of the Kudu versions running across all servers from which
// information could be fetched. Servers are grouped by version to make the
// table compact.
Status PrintVersionTable(const KsckVersionToServersMap& version_summaries,
                         int num_servers,
                         std::ostream& out);

// Print a formatted summary of the tables in 'table_summaries' to 'out'.
Status PrintTableSummaries(
    const std::vector<cluster_summary::TableSummary>& table_summaries,
    std::ostream& out);

// Print a formatted summary of the tablets in 'tablet_summaries' to 'out'.
Status PrintTabletSummaries(
    const std::vector<cluster_summary::TabletSummary>& tablet_summaries,
    PrintMode mode,
    std::ostream& out);

// Print to 'out' a "consensus matrix" that compares the consensus states of the
// replicas on servers with ids in 'server_uuids', given the set of consensus
// states in 'consensus_states'. If given, 'ref_cstate' will be used as the
// master's point of view of the consensus state of the tablet.
Status PrintConsensusMatrix(
    const std::vector<std::string>& server_uuids,
    const boost::optional<cluster_summary::ConsensusState>& ref_cstate,
    const cluster_summary::ConsensusStateMap& cstates,
    std::ostream& out);

// Print to 'out' a table summarizing the counts of tablet replicas in the
// cluster. 'mode' must be PLAIN_FULL or PLAIN_CONCISE. In PLAIN_CONCISE mode,
// summary information will be printed, while in PLAIN_FULL mode, the counts for
// every tablet server will be printed as well.
Status PrintReplicaCountByTserverSummary(PrintMode mode,
                                         const KsckResults& results,
                                         std::ostream& out);

Status PrintChecksumResults(const KsckChecksumResults& checksum_results,
                            std::ostream& out);

Status PrintTotalCounts(const KsckResults& results, std::ostream& out);

} // namespace tools
} // namespace kudu
