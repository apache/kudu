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
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
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
string format_replicas(const map<string, char>& label_by_uuid, const KsckConsensusState& config) {
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
                     const KsckConsensusState& cstate,
                     const map<string, char> replica_labels,
                     DataTable* table) {
  const string replicas = format_replicas(replica_labels, cstate);
  const string opid_index_str = cstate.opid_index ?
                                std::to_string(*cstate.opid_index) :
                                "";
  const string term = cstate.term ? std::to_string(*cstate.term) : "";
  const string committed = cstate.type == KsckConsensusConfigType::PENDING ?
                           "No" : "Yes";
  table->AddRow({ label, replicas, term, opid_index_str, committed });
}

// Sort servers by health, from most healthy to least.
// Useful for making unhealthy servers appear at the bottom of output when
// printing out a collection of server health summaries.
bool ServerByHealthComparator(const KsckServerHealthSummary& left,
                              const KsckServerHealthSummary& right) {
  return std::make_tuple(ServerHealthScore(left.health), left.uuid, left.address) <
         std::make_tuple(ServerHealthScore(right.health), right.uuid, right.address);
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

const char* const KsckCheckResultToString(KsckCheckResult cr) {
  switch (cr) {
    case KsckCheckResult::HEALTHY:
      return "HEALTHY";
    case KsckCheckResult::RECOVERING:
      return "RECOVERING";
    case KsckCheckResult::UNDER_REPLICATED:
      return "UNDER_REPLICATED";
    case KsckCheckResult::CONSENSUS_MISMATCH:
      return "CONSENSUS_MISMATCH";
    case KsckCheckResult::UNAVAILABLE:
      return "UNAVAILABLE";
    default:
      LOG(FATAL) << "Unknown CheckResult";
  }
}

const char* const ServerTypeToString(KsckServerType type) {
  switch (type) {
    case KsckServerType::MASTER:
      return "Master";
    case KsckServerType::TABLET_SERVER:
      return "Tablet Server";
    default:
      LOG(FATAL) << "Unknown ServerType";
  }
}

const char* const ServerHealthToString(KsckServerHealth sh) {
  switch (sh) {
    case KsckServerHealth::HEALTHY:
      return "HEALTHY";
    case KsckServerHealth::UNAUTHORIZED:
      return "UNAUTHORIZED";
    case KsckServerHealth::UNAVAILABLE:
      return "UNAVAILABLE";
    case KsckServerHealth::WRONG_SERVER_UUID:
      return "WRONG_SERVER_UUID";
    default:
      LOG(FATAL) << "Unknown KsckServerHealth";
  }
}

int ServerHealthScore(KsckServerHealth sh) {
  switch (sh) {
    case KsckServerHealth::HEALTHY:
      return 0;
    case KsckServerHealth::UNAUTHORIZED:
      return 1;
    case KsckServerHealth::UNAVAILABLE:
      return 2;
    case KsckServerHealth::WRONG_SERVER_UUID:
      return 3;
    default:
      LOG(FATAL) << "Unknown KsckServerHealth";
  }
}

Status KsckResults::PrintTo(PrintMode mode, int sections, ostream& out) {
  if (mode == PrintMode::JSON_PRETTY || mode == PrintMode::JSON_COMPACT) {
    return PrintJsonTo(mode, sections, out);
  }

  // First, report on the masters and master tablet.
  if (sections & (int)PrintSections::MASTER_SUMMARIES) {
    std::sort(master_summaries.begin(), master_summaries.end(), ServerByHealthComparator);
    RETURN_NOT_OK(PrintServerHealthSummaries(KsckServerType::MASTER,
                                             master_summaries,
                                             out));
    if (mode == PrintMode::PLAIN_FULL || master_consensus_conflict) {
      RETURN_NOT_OK(PrintConsensusMatrix(master_uuids,
                                         boost::none,
                                         master_consensus_state_map,
                                         out));
    }
    out << endl;

    RETURN_NOT_OK(PrintFlagTable(KsckServerType::MASTER,
                                 master_summaries.size(),
                                 master_flag_to_servers_map,
                                 master_flag_tags_map,
                                 out));
    if (!master_flag_to_servers_map.empty()) {
      out << endl;
    }
  }

  // Then, on the health of the tablet servers.
  if (sections & (int)PrintSections::TSERVER_SUMMARIES) {
    std::sort(tserver_summaries.begin(), tserver_summaries.end(), ServerByHealthComparator);
    RETURN_NOT_OK(PrintServerHealthSummaries(KsckServerType::TABLET_SERVER,
                                             tserver_summaries,
                                             out));
    if (!tserver_summaries.empty()) {
      out << endl;
    }

    RETURN_NOT_OK(PrintFlagTable(KsckServerType::TABLET_SERVER,
                                 tserver_summaries.size(),
                                 tserver_flag_to_servers_map,
                                 tserver_flag_tags_map,
                                 out));
    if (!tserver_flag_to_servers_map.empty()) {
      out << endl;
    }
  }

  // Finally, in the "server section", print the version summary.
  if (sections & (int)PrintSections::VERSION_SUMMARIES) {
    RETURN_NOT_OK(PrintVersionTable(master_summaries, tserver_summaries, out));
    out << endl;
  }

  // Then, on each tablet.
  if (sections & (int)PrintSections::TABLET_SUMMARIES) {
    RETURN_NOT_OK(PrintTabletSummaries(tablet_summaries, mode, out));
  }

  // Then, summarize the tablets by table.
  // Sort the tables so unhealthy tables are easy to see at the bottom.
  if (sections & (int)PrintSections::TABLE_SUMMARIES) {
    std::sort(table_summaries.begin(), table_summaries.end(),
              [](const KsckTableSummary &left, const KsckTableSummary &right) {
                  return std::make_pair(left.TableStatus() != KsckCheckResult::HEALTHY, left.name) <
                         std::make_pair(right.TableStatus() != KsckCheckResult::HEALTHY, right.name);
              });
    RETURN_NOT_OK(PrintTableSummaries(table_summaries, out));
    if (!table_summaries.empty()) {
      out << endl;
    }
  }

  // Next, report on checksum scans.
  if (sections & (int)PrintSections::CHECKSUM_RESULTS) {
    RETURN_NOT_OK(PrintChecksumResults(checksum_results, out));
    if (!checksum_results.tables.empty()) {
      out << endl;
    }
  }

  // And, add a summary of all the things we checked.
  if (sections & (int)PrintSections::TOTAL_COUNT) {
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
                            const boost::optional<KsckConsensusState> ref_cstate,
                            const KsckConsensusStateMap& cstates,
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

Status PrintServerHealthSummaries(KsckServerType type,
                                  const vector<KsckServerHealthSummary>& summaries,
                                  ostream& out) {
  out << ServerTypeToString(type) << " Summary" << endl;
  DataTable table({ "UUID", "Address", "Status" });
  for (const auto& s : summaries) {
    table.AddRow({ s.uuid, s.address, ServerHealthToString(s.health) });
  }
  RETURN_NOT_OK(table.PrintTo(out));
  // Print out the status message from each server with bad health.
  // This isn't done as part of the table because the messages can be quite long.
  for (const auto& s : summaries) {
    if (s.health == KsckServerHealth::HEALTHY) continue;
    out << Substitute("Error from $0: $1 ($2)", s.address, s.status.ToString(),
                      ServerHealthToString(s.health)) << endl;
  }
  return Status::OK();
}

Status PrintFlagTable(KsckServerType type,
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

Status PrintVersionTable(const vector<KsckServerHealthSummary>& masters,
                         const vector<KsckServerHealthSummary>& tservers,
                         ostream& out) {
  map<string, vector<string>> version_map;
  for (const auto& s : masters) {
    if (!s.version) continue;
    auto& servers = LookupOrInsert(&version_map, *s.version, {});
    servers.push_back(Substitute("master@$0", s.address));
  }
  for (const auto& s : tservers) {
    if (!s.version) continue;
    auto& servers = LookupOrInsert(&version_map, *s.version, {});
    servers.push_back(Substitute("tserver@$0", s.address));
  }
  out << "Version Summary" << endl;
  DataTable table({"Version", "Servers"});
  int num_servers = masters.size() + tservers.size();
  for (const auto& entry : version_map) {
    table.AddRow({entry.first, ServerCsv(num_servers, entry.second)});
  }
  return table.PrintTo(out);
}

Status PrintTableSummaries(const vector<KsckTableSummary>& table_summaries,
                           ostream& out) {
  if (table_summaries.empty()) {
    out << "The cluster doesn't have any matching tables" << endl;
    return Status::OK();
  }

  out << "Summary by table" << endl;
  DataTable table({ "Name", "RF", "Status", "Total Tablets",
                    "Healthy", "Recovering", "Under-replicated", "Unavailable"});
  for (const KsckTableSummary& ts : table_summaries) {
    table.AddRow({ ts.name,
                   to_string(ts.replication_factor), KsckCheckResultToString(ts.TableStatus()),
                   to_string(ts.TotalTablets()),
                   to_string(ts.healthy_tablets), to_string(ts.recovering_tablets),
                   to_string(ts.underreplicated_tablets),
                   to_string(ts.consensus_mismatch_tablets + ts.unavailable_tablets) });
  }
  return table.PrintTo(out);
}

Status PrintTabletSummaries(const vector<KsckTabletSummary>& tablet_summaries,
                            PrintMode mode,
                            ostream& out) {
  if (tablet_summaries.empty()) {
    out << "The cluster doesn't have any matching tablets" << endl << endl;
    return Status::OK();
  }
  for (const auto& tablet_summary : tablet_summaries) {
    if (mode != PrintMode::PLAIN_FULL &&
        tablet_summary.result == KsckCheckResult::HEALTHY) {
      continue;
    }
    out << tablet_summary.status << endl;
    for (const KsckReplicaSummary& r : tablet_summary.replicas) {
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
    for (const KsckReplicaSummary& rs : tablet_summary.replicas) {
      ts_uuids.push_back(rs.ts_uuid);
    }
    KsckConsensusStateMap consensus_state_map;
    for (const KsckReplicaSummary& rs : tablet_summary.replicas) {
      if (rs.consensus_state) {
        InsertOrDie(&consensus_state_map, rs.ts_uuid, *rs.consensus_state);
      }
    }
    RETURN_NOT_OK(PrintConsensusMatrix(ts_uuids, master_cstate, consensus_state_map, out));
    out << endl;
  }
  return Status::OK();
}

Status PrintChecksumResults(const KsckChecksumResults& checksum_results,
                            std::ostream& out) {
  if (checksum_results.tables.empty()) {
    return Status::OK();
  }
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
  if (results.table_summaries.empty()) {
    return Status::OK();
  }
  int num_replicas = std::accumulate(results.tablet_summaries.begin(),
                                     results.tablet_summaries.end(),
                                     0,
                                     [](int acc, const KsckTabletSummary& ts) {
                                       return acc + ts.replicas.size();
                                     });
  DataTable totals({ "", "Total Count" });
  totals.AddRow({ "Masters", to_string(results.master_summaries.size()) });
  totals.AddRow({ "Tablet Servers", to_string(results.tserver_summaries.size()) });
  totals.AddRow({ "Tables", to_string(results.table_summaries.size()) });
  totals.AddRow({ "Tablets", to_string(results.tablet_summaries.size()) });
  totals.AddRow({ "Replicas", to_string(num_replicas) });
  return totals.PrintTo(out);
}

void KsckServerHealthSummaryToPb(const KsckServerHealthSummary& summary,
                                 KsckServerHealthSummaryPB* pb) {
  switch (summary.health) {
    case KsckServerHealth::HEALTHY:
      pb->set_health(KsckServerHealthSummaryPB_ServerHealth_HEALTHY);
      break;
    case KsckServerHealth::UNAUTHORIZED:
      pb->set_health(KsckServerHealthSummaryPB_ServerHealth_UNAUTHORIZED);
      break;
    case KsckServerHealth::UNAVAILABLE:
      pb->set_health(KsckServerHealthSummaryPB_ServerHealth_UNAVAILABLE);
      break;
    case KsckServerHealth::WRONG_SERVER_UUID:
      pb->set_health(KsckServerHealthSummaryPB_ServerHealth_WRONG_SERVER_UUID);
      break;
    default:
      pb->set_health(KsckServerHealthSummaryPB_ServerHealth_UNKNOWN);
  }
  pb->set_uuid(summary.uuid);
  pb->set_address(summary.address);
  if (summary.version) {
    pb->set_version(*summary.version);
  }
  pb->set_status(summary.status.ToString());
}

void KsckConsensusStateToPb(const KsckConsensusState& cstate,
                            KsckConsensusStatePB* pb) {
  switch (cstate.type) {
    case KsckConsensusConfigType::MASTER:
      pb->set_type(KsckConsensusStatePB_ConfigType_MASTER);
      break;
    case KsckConsensusConfigType::COMMITTED:
      pb->set_type(KsckConsensusStatePB_ConfigType_COMMITTED);
      break;
    case KsckConsensusConfigType::PENDING:
      pb->set_type(KsckConsensusStatePB_ConfigType_PENDING);
      break;
    default:
      pb->set_type(KsckConsensusStatePB_ConfigType_UNKNOWN);
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

void KsckReplicaSummaryToPb(const KsckReplicaSummary& replica,
                            KsckReplicaSummaryPB* pb) {
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
    KsckConsensusStateToPb(*replica.consensus_state, pb->mutable_consensus_state());
  }
}

KsckTabletHealthPB KsckTabletHealthToPB(KsckCheckResult c) {
  switch (c) {
    case KsckCheckResult::HEALTHY:
      return KsckTabletHealthPB::HEALTHY;
    case KsckCheckResult::RECOVERING:
      return KsckTabletHealthPB::RECOVERING;
    case KsckCheckResult::UNDER_REPLICATED:
      return KsckTabletHealthPB::UNDER_REPLICATED;
    case KsckCheckResult::UNAVAILABLE:
      return KsckTabletHealthPB::UNAVAILABLE;
    case KsckCheckResult::CONSENSUS_MISMATCH:
      return KsckTabletHealthPB::CONSENSUS_MISMATCH;
    default:
      return KsckTabletHealthPB::UNKNOWN;
  }
}

void KsckTabletSummaryToPb(const KsckTabletSummary& tablet,
                           KsckTabletSummaryPB* pb) {
  pb->set_id(tablet.id);
  pb->set_table_id(tablet.table_id);
  pb->set_table_name(tablet.table_name);
  pb->set_health(KsckTabletHealthToPB(tablet.result));
  pb->set_status(tablet.status);
  KsckConsensusStateToPb(tablet.master_cstate, pb->mutable_master_cstate());
  for (const auto& replica : tablet.replicas) {
    KsckReplicaSummaryToPb(replica, pb->add_replicas());
  }
}

void KsckTableSummaryToPb(const KsckTableSummary& table, KsckTableSummaryPB* pb) {
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

void KsckResults::ToPb(KsckResultsPB* pb, int sections) const {
  for (const auto& error : error_messages) {
    pb->add_errors(error.ToString());
  }

  if (sections & (int)PrintSections::MASTER_SUMMARIES) {
    for (const auto &master_summary : master_summaries) {
      KsckServerHealthSummaryToPb(master_summary, pb->add_master_summaries());
    }
    for (const auto& master_uuid : master_uuids) {
      pb->add_master_uuids(master_uuid);
    }
    pb->set_master_consensus_conflict(master_consensus_conflict);
    for (const auto& entry : master_consensus_state_map) {
      KsckConsensusStateToPb(entry.second, pb->add_master_consensus_states());
    }
  }

  if (sections & (int)PrintSections::TSERVER_SUMMARIES) {
    for (const auto &tserver_summary : tserver_summaries) {
      KsckServerHealthSummaryToPb(tserver_summary, pb->add_tserver_summaries());
    }
  }

  if (sections & (int)PrintSections::TABLET_SUMMARIES) {
    for (const auto &tablet : tablet_summaries) {
      KsckTabletSummaryToPb(tablet, pb->add_tablet_summaries());
    }
  }

  if (sections & (int)PrintSections::TABLE_SUMMARIES) {
    for (const auto &table : table_summaries) {
      KsckTableSummaryToPb(table, pb->add_table_summaries());
    }
  }

  if (sections & (int)PrintSections::CHECKSUM_RESULTS) {
    if (!checksum_results.tables.empty()) {
      KsckChecksumResultsToPb(checksum_results, pb->mutable_checksum_results());
    }
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
