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
#include "kudu/tools/ksck_results.h"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <numeric>
#include <set>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rebalance/cluster_status.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tools/color.h"
#include "kudu/tools/tool.pb.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

DEFINE_uint32(truncate_server_csv_length, 3,
              "Maximum length of server CSVs before truncation. Raise this to "
              "see more servers with, e.g., unusual flags, at the cost of "
              "output with long lines.");

using kudu::cluster_summary::HealthCheckResult;
using kudu::cluster_summary::HealthCheckResultToString;
using kudu::cluster_summary::ConsensusConfigType;
using kudu::cluster_summary::ConsensusState;
using kudu::cluster_summary::ConsensusStateMap;
using kudu::cluster_summary::ReplicaSummary;
using kudu::cluster_summary::ServerHealth;
using kudu::cluster_summary::ServerHealthSummary;
using kudu::cluster_summary::ServerType;
using kudu::cluster_summary::TableSummary;
using kudu::cluster_summary::TabletSummary;
using kudu::cluster_summary::ServerHealthToString;
using kudu::cluster_summary::ServerTypeToString;

using std::endl;
using std::left;
using std::map;
using std::ostream;
using std::ostringstream;
using std::setw;
using std::shared_ptr;
using std::string;
using std::to_string;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

namespace {
// Return a formatted string version of 'config', mapping UUIDs to single-character
// labels using the mapping 'label_by_uuid'.
string format_replicas(const map<string, char>& label_by_uuid,
                       const ConsensusState& config) {
  constexpr int kPeerWidth = 4;
  ostringstream result;
  // Sort the output by label for readability.
  std::set<std::pair<char, string>> labeled_replicas;
  for (const auto& entry : label_by_uuid) {
    labeled_replicas.emplace(entry.second, entry.first);
  }
  for (const auto &entry : labeled_replicas) {
    if (!ContainsKey(config.voter_uuids, entry.second) &&
        !ContainsKey(config.non_voter_uuids, entry.second)) {
      result << setw(kPeerWidth) << left << "";
      continue;
    }
    if (config.leader_uuid && config.leader_uuid == entry.second) {
      result << setw(kPeerWidth) << left << Substitute("$0*", entry.first);
    } else {
      if (ContainsKey(config.non_voter_uuids, entry.second)) {
        result << setw(kPeerWidth) << left << Substitute("$0~", entry.first);
      } else {
        result << setw(kPeerWidth) << left << Substitute("$0", entry.first);
      }
    }
  }
  return result.str();
}

void AddToUuidLabelMapping(const std::set<string>& uuids,
                           map<string, char>* uuid_label_mapping) {
  CHECK(uuid_label_mapping);
  const char* const labels = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  int i = uuid_label_mapping->size() % (sizeof(labels) - 1);
  for (const auto& uuid : uuids) {
    if (InsertIfNotPresent(uuid_label_mapping, uuid, labels[i])) {
      i = (i + 1) % (sizeof(labels) - 1);
    }
  }
}

void AddRowForCState(const string& label,
                     const ConsensusState& cstate,
                     const map<string, char>& replica_labels,
                     DataTable* table) {
  const string replicas = format_replicas(replica_labels, cstate);
  const string opid_index_str = cstate.opid_index ?
                                std::to_string(*cstate.opid_index) :
                                "";
  const string term = cstate.term ? std::to_string(*cstate.term) : "";
  const string committed = cstate.type == ConsensusConfigType::PENDING ?
                           "No" : "Yes";
  table->AddRow({ label, replicas, term, opid_index_str, committed });
}

// Sort servers by health, from most healthy to least.
// Useful for making unhealthy servers appear at the bottom of output when
// printing out a collection of server health summaries.
bool ServerByHealthComparator(const ServerHealthSummary& left,
                              const ServerHealthSummary& right) {
  return std::make_tuple(left.health, left.uuid, left.address) <
         std::make_tuple(right.health, right.uuid, right.address);
}

// Produces a possibly-abbreviated CSV of 'servers':
// - If 'servers.size() == server_count', returns a special message indicating
//   all servers.
// - If servers.size() > FLAGS_truncate_server_csv_length, returns a csv with
//   the first FLAGS_truncate_server_csv_length servers and a final component
//   indicating how many are elided.
// Requires that 'servers.size() <= server_count'
string ServerCsv(int server_count, const vector<string>& servers) {
  DCHECK_LE(servers.size(), server_count);
  if (servers.size() == server_count) {
    return Substitute("all $0 server(s) checked", server_count);
  }
  uint32_t n = FLAGS_truncate_server_csv_length;
  if (servers.size() <= n) {
    return JoinStrings(servers, ", ");
  }
  vector<string> first_n;
  std::copy_n(servers.begin(), n, std::back_inserter(first_n));
  first_n.push_back(Substitute("and $0 other server(s)", servers.size() - n));
  return JoinStrings(first_n, ", ");
}

} // anonymous namespace

Status KsckResults::PrintTo(PrintMode mode, int sections, ostream& out) {
  if (mode == PrintMode::JSON_PRETTY || mode == PrintMode::JSON_COMPACT) {
    return PrintJsonTo(mode, sections, out);
  }

  // First, report on the masters and master tablet.
  if (sections & PrintSections::MASTER_SUMMARIES) {
    std::sort(cluster_status.master_summaries.begin(),
              cluster_status.master_summaries.end(),
              ServerByHealthComparator);
    RETURN_NOT_OK(PrintServerHealthSummaries(ServerType::MASTER,
                                             cluster_status.master_summaries,
                                             out));
    if (mode == PrintMode::PLAIN_FULL || cluster_status.master_consensus_conflict) {
      RETURN_NOT_OK(PrintConsensusMatrix(cluster_status.master_uuids,
                                         boost::none,
                                         cluster_status.master_consensus_state_map,
                                         out));
    }
    out << endl;

    RETURN_NOT_OK(PrintFlagTable(ServerType::MASTER,
                                 cluster_status.master_summaries.size(),
                                 master_flag_to_servers_map,
                                 master_flag_tags_map,
                                 out));
    if (!master_flag_to_servers_map.empty()) {
      out << endl;
    }
  }

  // Then, on any special tablet server states.
  if (sections & PrintSections::TSERVER_STATES &&
      !ts_states.empty()) {
    RETURN_NOT_OK(PrintTServerStatesTable(ts_states, out));
    out << endl;
  }

  // Then, on the health of the tablet servers.
  if (sections & PrintSections::TSERVER_SUMMARIES) {
    std::sort(cluster_status.tserver_summaries.begin(),
              cluster_status.tserver_summaries.end(),
              ServerByHealthComparator);
    RETURN_NOT_OK(PrintServerHealthSummaries(ServerType::TABLET_SERVER,
                                             cluster_status.tserver_summaries,
                                             out));
    if (!cluster_status.tserver_summaries.empty()) {
      out << endl;
    }

    RETURN_NOT_OK(PrintFlagTable(ServerType::TABLET_SERVER,
                                 cluster_status.tserver_summaries.size(),
                                 tserver_flag_to_servers_map,
                                 tserver_flag_tags_map,
                                 out));
    if (!tserver_flag_to_servers_map.empty()) {
      out << endl;
    }
  }

  // Finally, in the "server section", print the version summary.
  if (sections & PrintSections::VERSION_SUMMARIES) {
    RETURN_NOT_OK(PrintVersionTable(version_summaries,
                                    cluster_status.master_summaries.size()
                                      + cluster_status.tserver_summaries.size(),
                                    out));
    out << endl;
  }

  // Then, on each tablet.
  if (sections & PrintSections::TABLET_SUMMARIES) {
    RETURN_NOT_OK(PrintTabletSummaries(cluster_status.tablet_summaries, mode, out));
  }

  // Then, summarize the tablets by table.
  // Sort the tables so unhealthy tables are easy to see at the bottom.
  if (sections & PrintSections::TABLE_SUMMARIES) {
    std::sort(cluster_status.table_summaries.begin(),
              cluster_status.table_summaries.end(),
              [](const TableSummary &left,
                 const TableSummary &right) {
                  return std::make_pair(left.TableStatus() != HealthCheckResult::HEALTHY,
                                        left.name) <
                         std::make_pair(right.TableStatus() != HealthCheckResult::HEALTHY,
                                        right.name);
              });
    RETURN_NOT_OK(PrintTableSummaries(cluster_status.table_summaries, out));
    if (!cluster_status.table_summaries.empty()) {
      out << endl;
    }

    // Add in a summary of the tablet replicas by tablet server, so it's easy to
    // see if some tablet servers have too many replicas.
    RETURN_NOT_OK(PrintReplicaCountByTserverSummary(mode, *this, out));
  }

  // Next, report on checksum scans.
  if (sections & PrintSections::CHECKSUM_RESULTS) {
    RETURN_NOT_OK(PrintChecksumResults(checksum_results, out));
    if (!checksum_results.tables.empty()) {
      out << endl;
    }
  }

  // And, add a summary of all the things we checked.
  if (sections & PrintSections::TOTAL_COUNT) {
    RETURN_NOT_OK(PrintTotalCounts(*this, out));
    out << endl;
  }

  // Penultimately, print the warnings.
  if (!warning_messages.empty()) {
    out << "==================" << endl;
    out << "Warnings:" << endl;
    out << "==================" << endl;
    for (const auto& s : warning_messages) {
      out << s.message().ToString() << endl;
    }
    out << endl;
  }

  // Finally, print a summary of all errors.
  if (error_messages.empty()) {
    // All good.
    out << "OK" << endl;
  } else {
    // Something went wrong.
    out << "==================" << endl;
    out << "Errors:" << endl;
    out << "==================" << endl;
    for (const auto& s : error_messages) {
      out << s.ToString() << endl;
    }
    out << endl;
    out << "FAILED" << endl;
  }

  // Remember, we only return non-OK if printing failed, not if ksck checks failed.
  return Status::OK();
}

Status PrintConsensusMatrix(const vector<string>& server_uuids,
                            const boost::optional<ConsensusState>& ref_cstate,
                            const ConsensusStateMap& cstates,
                            ostream& out) {
  map<string, char> replica_labels;
  for (const auto& uuid : server_uuids) {
    AddToUuidLabelMapping({ uuid }, &replica_labels);
  }
  for (const auto& entry : cstates) {
    AddToUuidLabelMapping(entry.second.voter_uuids, &replica_labels);
    AddToUuidLabelMapping(entry.second.non_voter_uuids, &replica_labels);
  }
  out << "All reported replicas are:" << endl;
  // Sort the output by label for readability.
  std::set<std::pair<char, string>> reported_servers;
  for (const auto& entry : replica_labels) {
    reported_servers.emplace(entry.second, entry.first);
  }
  for (const auto& entry : reported_servers) {
    out << "  " << entry.first << " = " << entry.second << endl;
  }
  DataTable cmatrix({ "Config source", "Replicas", "Current term",
                      "Config index", "Committed?"});
  if (ref_cstate) {
    AddRowForCState("master", *ref_cstate, replica_labels, &cmatrix);
  }
  for (const auto& uuid : server_uuids) {
    const string label(1, FindOrDie(replica_labels, uuid));
    if (!ContainsKey(cstates, uuid)) {
      cmatrix.AddRow({ label, "[config not available]", "", "", "" });
      continue;
    }
    const auto& cstate = FindOrDie(cstates, uuid);
    AddRowForCState(label, cstate, replica_labels, &cmatrix);
  }
  out << "The consensus matrix is:" << endl;
  return cmatrix.PrintTo(out);
}

Status PrintServerHealthSummaries(ServerType type,
                                  const vector<ServerHealthSummary>& summaries,
                                  ostream& out) {
  out << ServerTypeToString(type) << " Summary" << endl;
  if (summaries.empty()) return Status::OK();

  if (type == ServerType::TABLET_SERVER) {
    DataTable table({ "UUID", "Address", "Status", "Location" });
    unordered_map<string, int> location_counts;
    for (const auto& s : summaries) {
      string location = s.ts_location.empty() ? "<none>" : s.ts_location;
      location_counts[location]++;
      table.AddRow({ s.uuid, s.address, ServerHealthToString(s.health), std::move(location) });
    }
    RETURN_NOT_OK(table.PrintTo(out));

    // Print location count table.
    out << std::endl;
    out << "Tablet Server Location Summary" << endl;
    DataTable loc_stats_table({ "Location", "Count" });
    for (const auto& loc_count : location_counts) {
      loc_stats_table.AddRow({ loc_count.first, IntToString(loc_count.second) });
    }
    RETURN_NOT_OK(loc_stats_table.PrintTo(out));
  } else {
    DCHECK_EQ(ServerTypeToString(type), ServerTypeToString(ServerType::MASTER));
    DataTable table({ "UUID", "Address", "Status" });
    for (const auto& s : summaries) {
      table.AddRow({ s.uuid, s.address, ServerHealthToString(s.health) });
    }
    RETURN_NOT_OK(table.PrintTo(out));
  }

  // Print out the status message from each server with bad health.
  // This isn't done as part of the table because the messages can be quite long.
  for (const auto& s : summaries) {
    if (s.health == ServerHealth::HEALTHY) continue;
    out << Substitute("Error from $0: $1 ($2)", s.address, s.status.ToString(),
                      ServerHealthToString(s.health)) << endl;
  }
  return Status::OK();
}

Status PrintFlagTable(ServerType type,
                      int num_servers,
                      const KsckFlagToServersMap& flag_to_servers_map,
                      const KsckFlagTagsMap& flag_tags_map,
                      ostream& out) {
  if (flag_to_servers_map.empty()) {
    return Status::OK();
  }
  DataTable flags_table({"Flag", "Value", "Tags", ServerTypeToString(type)});
  for (const auto& flag : flag_to_servers_map) {
    const string& name = flag.first.first;
    const string& value = flag.first.second;
    flags_table.AddRow({name,
                        value,
                        FindOrDie(flag_tags_map, name),
                        ServerCsv(num_servers, flag.second)});
  }
  return flags_table.PrintTo(out);
}

Status PrintTServerStatesTable(const KsckTServerStateMap& ts_states, ostream& out) {
  out << "Tablet Server States" << endl;
  // We expect tserver states to be relatively uncommon, so print one per row.
  DataTable table({"Server", "State"});
  for (const auto& id_and_state : ts_states) {
    table.AddRow({ id_and_state.first,
                   TServerStatePB_Name(id_and_state.second) });
  }
  return table.PrintTo(out);
}

Status PrintVersionTable(const KsckVersionToServersMap& version_summaries,
                         int num_servers,
                         ostream& out) {
  out << "Version Summary" << endl;
  DataTable table({"Version", "Servers"});
  for (const auto& entry : version_summaries) {
    table.AddRow({entry.first, ServerCsv(num_servers, entry.second)});
  }
  return table.PrintTo(out);
}

Status PrintTableSummaries(const vector<TableSummary>& table_summaries,
                           ostream& out) {
  if (table_summaries.empty()) {
    out << "The cluster doesn't have any matching tables" << endl;
    return Status::OK();
  }

  out << "Summary by table" << endl;
  DataTable table({ "Name", "RF", "Status", "Total Tablets",
                    "Healthy", "Recovering", "Under-replicated", "Unavailable"});
  for (const TableSummary& ts : table_summaries) {
    table.AddRow({ ts.name,
                   to_string(ts.replication_factor),
                             HealthCheckResultToString(ts.TableStatus()),
                   to_string(ts.TotalTablets()),
                   to_string(ts.healthy_tablets), to_string(ts.recovering_tablets),
                   to_string(ts.underreplicated_tablets),
                   to_string(ts.consensus_mismatch_tablets + ts.unavailable_tablets) });
  }
  return table.PrintTo(out);
}

Status PrintTabletSummaries(const vector<TabletSummary>& tablet_summaries,
                            PrintMode mode,
                            ostream& out) {
  if (tablet_summaries.empty()) {
    out << "The cluster doesn't have any matching tablets" << endl << endl;
    return Status::OK();
  }
  out << "Tablet Summary" << endl;
  for (const auto& tablet_summary : tablet_summaries) {
    if (mode != PrintMode::PLAIN_FULL &&
        tablet_summary.result == HealthCheckResult::HEALTHY) {
      continue;
    }
    out << tablet_summary.status << endl;
    for (const ReplicaSummary& r : tablet_summary.replicas) {
      const char* spec_str = r.is_leader
          ? " [LEADER]" : (!r.is_voter ? " [NONVOTER]" : "");

      const string ts_string = r.ts_address ?
                               Substitute("$0 ($1)", r.ts_uuid, *r.ts_address) :
                               r.ts_uuid;
      out << "  " << ts_string << ": ";
      if (!r.ts_healthy) {
        out << Color(AnsiCode::YELLOW, "TS unavailable") << spec_str << endl;
        continue;
      }
      if (r.state == tablet::RUNNING) {
        out << Color(AnsiCode::GREEN, "RUNNING") << spec_str << endl;
        continue;
      }
      if (r.status_pb == boost::none) {
        out << Color(AnsiCode::YELLOW, "missing") << spec_str << endl;
        continue;
      }

      out << Color(AnsiCode::YELLOW, "not running") << spec_str << endl;
      out << Substitute(
          "    State:       $0\n"
          "    Data state:  $1\n"
          "    Last status: $2\n",
          Color(AnsiCode::BLUE, tablet::TabletStatePB_Name(r.state)),
          Color(AnsiCode::BLUE,
                tablet::TabletDataState_Name(r.status_pb->tablet_data_state())),
          Color(AnsiCode::BLUE, r.status_pb->last_status()));
    }

    auto& master_cstate = tablet_summary.master_cstate;
    vector<string> ts_uuids;
    for (const ReplicaSummary& rs : tablet_summary.replicas) {
      ts_uuids.push_back(rs.ts_uuid);
    }
    ConsensusStateMap consensus_state_map;
    for (const ReplicaSummary& rs : tablet_summary.replicas) {
      if (rs.consensus_state) {
        InsertOrDie(&consensus_state_map, rs.ts_uuid, *rs.consensus_state);
      }
    }
    RETURN_NOT_OK(PrintConsensusMatrix(ts_uuids, master_cstate,
                                       consensus_state_map, out));
    out << endl;
  }
  return Status::OK();
}

Status PrintReplicaCountByTserverSummary(PrintMode mode,
                                         const KsckResults& results,
                                         std::ostream& out) {
  DCHECK(mode == PrintMode::PLAIN_FULL || mode == PrintMode::PLAIN_CONCISE);

  // Collate the counts by tablet servers. We populate the map with the UUIDs
  // from 'tserver_summaries' so we don't miss empty tablet servers.
  map<string, std::pair<string, int>> host_and_replica_count_by_tserver;
  for (const auto& tserver : results.cluster_status.tserver_summaries) {
    EmplaceIfNotPresent(&host_and_replica_count_by_tserver,
                        tserver.uuid,
                        std::make_pair(tserver.address, 0));
  }
  for (const auto& tablet : results.cluster_status.tablet_summaries) {
    for (const auto& replica : tablet.replicas) {
      const string address = replica.ts_address ? *replica.ts_address :
                                                  "unavailable";
      LookupOrEmplace(&host_and_replica_count_by_tserver,
                      replica.ts_uuid,
                      std::make_pair(address, 0)).second++;
    }
  }

  if (host_and_replica_count_by_tserver.empty()) {
    return Status::OK();
  }

  // Compute a 5-number summary and print that plus outliers.
  typedef std::tuple<string, string, int> UuidHostCount;
  vector<UuidHostCount> tservers_sorted_by_replica_count;
  tservers_sorted_by_replica_count.reserve(host_and_replica_count_by_tserver.size());
  for (const auto& entry : host_and_replica_count_by_tserver) {
    tservers_sorted_by_replica_count.emplace_back(entry.first,
                                                  entry.second.first,
                                                  entry.second.second);
  }

  DataTable table({ "Statistic", "Replica Count" });
  std::sort(tservers_sorted_by_replica_count.begin(),
            tservers_sorted_by_replica_count.end(),
            [](const UuidHostCount& left, const UuidHostCount& right) {
              return std::get<2>(left) < std::get<2>(right);
            });
  const auto num_tservers = tservers_sorted_by_replica_count.size();
  const auto min = std::get<2>(tservers_sorted_by_replica_count[0]);
  const auto first_quartile =
      std::get<2>(tservers_sorted_by_replica_count[num_tservers / 4]);
  const auto median = std::get<2>(tservers_sorted_by_replica_count[num_tservers / 2]);
  const auto third_quartile =
      std::get<2>(tservers_sorted_by_replica_count[3 * num_tservers / 4]);
  const auto max = std::get<2>(tservers_sorted_by_replica_count[num_tservers - 1]);
  table.AddRow({ "Minimum", std::to_string(min) });
  table.AddRow({ "First Quartile", std::to_string(first_quartile) });
  table.AddRow({ "Median", std::to_string(median) });
  table.AddRow({ "Third Quartile", std::to_string(third_quartile) });
  table.AddRow({ "Maximum", std::to_string(max) });

  out << "Tablet Replica Count Summary" << endl;
  RETURN_NOT_OK(table.PrintTo(out));
  out << endl;

  // Now outliers, if there are any. We use a standard definition: an outlier
  // is a value more than 1.5 * (third - first quartile) below the first or
  // above the third quartile.
  const auto interquartile_range = third_quartile - first_quartile;
  const auto below_is_outlier = first_quartile - 1.5 * interquartile_range;
  const auto above_is_outlier = third_quartile + 1.5 * interquartile_range;
  auto small_outliers = std::partition_point(
      tservers_sorted_by_replica_count.rbegin(),
      tservers_sorted_by_replica_count.rend(),
      [&](const UuidHostCount val) {
        return std::get<2>(val) >= below_is_outlier;
      });
  auto big_outliers = std::partition_point(
      tservers_sorted_by_replica_count.begin(),
      tservers_sorted_by_replica_count.end(),
      [&](const UuidHostCount val) {
        return std::get<2>(val) <= above_is_outlier;
      });

  int num_outliers = 0;
  DataTable outliers({ "Type", "UUID", "Host", "Replica Count" });
  for (auto it = big_outliers; it != tservers_sorted_by_replica_count.end(); ++it) {
    num_outliers++;
    const auto& uuid = std::get<0>(*it);
    const auto& host = std::get<1>(*it);
    const auto& count = std::get<2>(*it);
    outliers.AddRow({ "Big", uuid, host, std::to_string(count) });
  }
  for (auto it = small_outliers; it != tservers_sorted_by_replica_count.rend(); ++it) {
    num_outliers++;
    const auto& uuid = std::get<0>(*it);
    const auto& host = std::get<1>(*it);
    const auto& count = std::get<2>(*it);
    outliers.AddRow({ "Small", uuid, host, std::to_string(count) });
  }

  if (num_outliers > 0) {
    out << "Tablet Replica Count Outliers" << endl;
    RETURN_NOT_OK(outliers.PrintTo(out));
    out << endl;
  }

  // If we're in verbose mode, also dump all the info.
  if (mode == PrintMode::PLAIN_FULL) {
    out << "Tablet Replica Count by Tablet Server" << endl;
    DataTable table({ "UUID", "Host", "Replica Count" });
    for (const auto& entry : host_and_replica_count_by_tserver) {
      table.AddRow({ entry.first,
                     entry.second.first,
                     std::to_string(entry.second.second) });
    }
    RETURN_NOT_OK(table.PrintTo(out));
    out << endl;
  }

  return Status::OK();
}

Status PrintChecksumResults(const KsckChecksumResults& checksum_results,
                            std::ostream& out) {
  if (checksum_results.tables.empty()) {
    return Status::OK();
  }
  out << "Checksum Summary" << endl;
  if (checksum_results.snapshot_timestamp) {
    out << "Using snapshot timestamp: " << *checksum_results.snapshot_timestamp << endl;
  }
  for (const auto& table_entry : checksum_results.tables) {
    const auto& table_name = table_entry.first;
    const auto& table_checksums = table_entry.second;
    out << "-----------------------" << endl;
    out << table_name << endl;
    out << "-----------------------" << endl;
    for (const auto& tablet_entry : table_checksums) {
      const auto& tablet_id = tablet_entry.first;
      const auto& tablet_checksum = tablet_entry.second;
      for (const auto& replica_entry : tablet_checksum.replica_checksums) {
        const auto& ts_uuid = replica_entry.first;
        const auto& replica_checksum = replica_entry.second;
        const string status_str = replica_checksum.status.ok() ?
            Substitute("Checksum: $0", replica_checksum.checksum) :
            Substitute("Error: $0", replica_checksum.status.ToString());
        out << Substitute("T $0 P $1 ($2): $3",
                          tablet_id, ts_uuid, replica_checksum.ts_address, status_str)
            << endl;
      }
      if (tablet_checksum.mismatch) {
        out << ">> Mismatch found in table " << table_name
            << " tablet " << tablet_id << endl;
      }
    }
  }
  return Status::OK();
}

Status PrintTotalCounts(const KsckResults& results, std::ostream& out) {
  // Don't print the results if there's no matching tables.
  if (results.cluster_status.table_summaries.empty()) {
    return Status::OK();
  }
  int num_replicas = std::accumulate(results.cluster_status.tablet_summaries.begin(),
                                     results.cluster_status.tablet_summaries.end(),
                                     0,
                                     [](int acc, const TabletSummary& ts) {
                                       return acc + ts.replicas.size();
                                     });
  out << "Total Count Summary" << endl;
  DataTable totals({ "", "Total Count" });
  totals.AddRow({ "Masters", to_string(results.cluster_status.master_summaries.size()) });
  totals.AddRow({ "Tablet Servers", to_string(results.cluster_status.tserver_summaries.size()) });
  totals.AddRow({ "Tables", to_string(results.cluster_status.table_summaries.size()) });
  totals.AddRow({ "Tablets", to_string(results.cluster_status.tablet_summaries.size()) });
  totals.AddRow({ "Replicas", to_string(num_replicas) });
  return totals.PrintTo(out);
}

void ServerHealthSummaryToPb(const ServerHealthSummary& summary,
                             ServerHealthSummaryPB* pb) {
  switch (summary.health) {
    case ServerHealth::HEALTHY:
      pb->set_health(ServerHealthSummaryPB_ServerHealth_HEALTHY);
      break;
    case ServerHealth::UNAUTHORIZED:
      pb->set_health(ServerHealthSummaryPB_ServerHealth_UNAUTHORIZED);
      break;
    case ServerHealth::UNAVAILABLE:
      pb->set_health(ServerHealthSummaryPB_ServerHealth_UNAVAILABLE);
      break;
    case ServerHealth::WRONG_SERVER_UUID:
      pb->set_health(ServerHealthSummaryPB_ServerHealth_WRONG_SERVER_UUID);
      break;
    default:
      pb->set_health(ServerHealthSummaryPB_ServerHealth_UNKNOWN);
  }
  pb->set_uuid(summary.uuid);
  pb->set_address(summary.address);
  if (summary.version) {
    pb->set_version(*summary.version);
  }
  pb->set_status(summary.status.ToString());
  if (!summary.ts_location.empty()) {
    pb->set_location(summary.ts_location);
  }
}

void ConsensusStateToPb(const ConsensusState& cstate,
                        ConsensusStatePB* pb) {
  switch (cstate.type) {
    case ConsensusConfigType::MASTER:
      pb->set_type(ConsensusStatePB_ConfigType_MASTER);
      break;
    case ConsensusConfigType::COMMITTED:
      pb->set_type(ConsensusStatePB_ConfigType_COMMITTED);
      break;
    case ConsensusConfigType::PENDING:
      pb->set_type(ConsensusStatePB_ConfigType_PENDING);
      break;
    default:
      pb->set_type(ConsensusStatePB_ConfigType_UNKNOWN);
  }
  if (cstate.term) {
    pb->set_term(*cstate.term);
  }
  if (cstate.opid_index) {
    pb->set_opid_index(*cstate.opid_index);
  }
  if (cstate.leader_uuid) {
    pb->set_leader_uuid(*cstate.leader_uuid);
  }
  for (const auto& voter_uuid : cstate.voter_uuids) {
    pb->add_voter_uuids(voter_uuid);
  }
  for (const auto& non_voter_uuid : cstate.non_voter_uuids) {
    pb->add_non_voter_uuids(non_voter_uuid);
  }
}

void ReplicaSummaryToPb(const ReplicaSummary& replica,
                        ReplicaSummaryPB* pb) {
  pb->set_ts_uuid(replica.ts_uuid);
  if (replica.ts_address) {
    pb->set_ts_address(*replica.ts_address);
  }
  pb->set_ts_healthy(replica.ts_healthy);
  pb->set_is_leader(replica.is_leader);
  pb->set_is_voter(replica.is_voter);
  pb->set_state(replica.state);
  if (replica.status_pb) {
    pb->mutable_status_pb()->CopyFrom(*replica.status_pb);
  }
  if (replica.consensus_state) {
    ConsensusStateToPb(*replica.consensus_state, pb->mutable_consensus_state());
  }
}

KsckTabletHealthPB KsckTabletHealthToPB(HealthCheckResult c) {
  switch (c) {
    case HealthCheckResult::HEALTHY:
      return KsckTabletHealthPB::HEALTHY;
    case HealthCheckResult::RECOVERING:
      return KsckTabletHealthPB::RECOVERING;
    case HealthCheckResult::UNDER_REPLICATED:
      return KsckTabletHealthPB::UNDER_REPLICATED;
    case HealthCheckResult::UNAVAILABLE:
      return KsckTabletHealthPB::UNAVAILABLE;
    case HealthCheckResult::CONSENSUS_MISMATCH:
      return KsckTabletHealthPB::CONSENSUS_MISMATCH;
    default:
      return KsckTabletHealthPB::UNKNOWN;
  }
}

void TabletSummaryToPb(const TabletSummary& tablet, TabletSummaryPB* pb) {
  pb->set_id(tablet.id);
  pb->set_table_id(tablet.table_id);
  pb->set_table_name(tablet.table_name);
  pb->set_health(KsckTabletHealthToPB(tablet.result));
  pb->set_status(tablet.status);
  ConsensusStateToPb(tablet.master_cstate, pb->mutable_master_cstate());
  for (const auto& replica : tablet.replicas) {
    ReplicaSummaryToPb(replica, pb->add_replicas());
  }
}

void TableSummaryToPb(const TableSummary& table, TableSummaryPB* pb) {
  pb->set_id(table.id);
  pb->set_name(table.name);
  pb->set_health(KsckTabletHealthToPB(table.TableStatus()));
  pb->set_replication_factor(table.replication_factor);
  pb->set_total_tablets(table.TotalTablets());
  pb->set_healthy_tablets(table.healthy_tablets);
  pb->set_recovering_tablets(table.recovering_tablets);
  pb->set_underreplicated_tablets(table.underreplicated_tablets);
  pb->set_unavailable_tablets(table.unavailable_tablets);
  pb->set_consensus_mismatch_tablets(table.consensus_mismatch_tablets);
}

void KsckReplicaChecksumToPb(const string& ts_uuid,
                             const KsckReplicaChecksum& replica,
                             KsckReplicaChecksumPB* pb) {
  pb->set_ts_uuid(ts_uuid);
  pb->set_ts_address(replica.ts_address);
  pb->set_checksum(replica.checksum);
  pb->set_status(replica.status.ToString());
}

void KsckTabletChecksumToPb(const string& tablet_id,
                            const KsckTabletChecksum& tablet,
                            KsckTabletChecksumPB* pb) {
  pb->set_tablet_id(tablet_id);
  pb->set_mismatch(tablet.mismatch);
  for (const auto& entry : tablet.replica_checksums) {
    KsckReplicaChecksumToPb(entry.first, entry.second, pb->add_replica_checksums());
  }
}

void KsckTableChecksumToPb(const string& name,
                           const KsckTableChecksum& table,
                           KsckTableChecksumPB* pb) {
  pb->set_name(name);
  for (const auto& entry : table) {
    KsckTabletChecksumToPb(entry.first, entry.second, pb->add_tablets());
  }
}

void KsckChecksumResultsToPb(const KsckChecksumResults& results,
                             KsckChecksumResultsPB* pb) {
  if (results.snapshot_timestamp) {
    pb->set_snapshot_timestamp(*results.snapshot_timestamp);
  }
  for (const auto& entry : results.tables) {
    KsckTableChecksumToPb(entry.first, entry.second, pb->add_tables());
  }
}

void KsckVersionSummaryToPb(const std::string& version,
                            const std::vector<std::string>& uuids,
                            KsckVersionSummaryPB* pb) {
  pb->set_version(version);
  for (const auto& uuid : uuids) {
    pb->add_servers(uuid);
  }
}

void KsckCountSummaryToPb(int master_count,
                          int tserver_count,
                          int table_count,
                          const std::vector<TabletSummary>& tablet_summaries,
                          KsckCountSummaryPB* pb) {
  pb->set_masters(master_count);
  pb->set_tservers(tserver_count);
  pb->set_tables(table_count);
  pb->set_tablets(tablet_summaries.size());
  int replica_count = std::accumulate(tablet_summaries.begin(),
                                     tablet_summaries.end(),
                                     0,
                                     [](int acc, const TabletSummary& ts) {
                                       return acc + ts.replicas.size();
                                     });
  pb->set_replicas(replica_count);
}

void KsckResults::ToPb(KsckResultsPB* pb, int sections) const {
  for (const auto& error : error_messages) {
    pb->add_errors(error.ToString());
  }

  if (sections & PrintSections::MASTER_SUMMARIES) {
    for (const auto &master_summary : cluster_status.master_summaries) {
      ServerHealthSummaryToPb(master_summary, pb->add_master_summaries());
    }
    for (const auto& master_uuid : cluster_status.master_uuids) {
      pb->add_master_uuids(master_uuid);
    }
    pb->set_master_consensus_conflict(cluster_status.master_consensus_conflict);
    for (const auto& entry : cluster_status.master_consensus_state_map) {
      ConsensusStateToPb(entry.second, pb->add_master_consensus_states());
    }
  }

  if (sections & PrintSections::TSERVER_SUMMARIES) {
    for (const auto &tserver_summary : cluster_status.tserver_summaries) {
      ServerHealthSummaryToPb(tserver_summary, pb->add_tserver_summaries());
    }
  }

  if (sections & PrintSections::TABLET_SUMMARIES) {
    for (const auto &tablet : cluster_status.tablet_summaries) {
      TabletSummaryToPb(tablet, pb->add_tablet_summaries());
    }
  }

  if (sections & PrintSections::VERSION_SUMMARIES) {
    for (const auto &version_summary : version_summaries) {
      KsckVersionSummaryToPb(version_summary.first,
                             version_summary.second,
                             pb->add_version_summaries());
    }
  }

  if (sections & PrintSections::TABLE_SUMMARIES) {
    for (const auto &table : cluster_status.table_summaries) {
      TableSummaryToPb(table, pb->add_table_summaries());
    }
  }

  if (sections & PrintSections::CHECKSUM_RESULTS) {
    if (!checksum_results.tables.empty()) {
      KsckChecksumResultsToPb(checksum_results, pb->mutable_checksum_results());
    }
  }

  if (sections & PrintSections::TOTAL_COUNT) {
    KsckCountSummaryToPb(cluster_status.master_summaries.size(),
                         cluster_status.tserver_summaries.size(),
                         cluster_status.table_summaries.size(),
                         cluster_status.tablet_summaries,
                         pb->add_count_summaries());
  }
}

Status KsckResults::PrintJsonTo(PrintMode mode, int sections, ostream& out) const {
  CHECK(mode == PrintMode::JSON_PRETTY || mode == PrintMode::JSON_COMPACT);
  JsonWriter::Mode jw_mode = JsonWriter::Mode::PRETTY;
  if (mode == PrintMode::JSON_COMPACT) {
    jw_mode = JsonWriter::Mode::COMPACT;
  }

  KsckResultsPB pb;
  ToPb(&pb, sections);
  out << JsonWriter::ToJson(pb, jw_mode) << endl;
  return Status::OK();
}

} // namespace tools
} // namespace kudu
