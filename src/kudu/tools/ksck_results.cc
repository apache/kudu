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
#include <map>
#include <memory>
#include <numeric>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/tools/color.h"
#include "kudu/util/status.h"

using std::cout;
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

} // anonymous namespace

const char* const KsckCheckResultToString(KsckCheckResult cr) {
  switch (cr) {
    case KsckCheckResult::HEALTHY:
      return "HEALTHY";
    case KsckCheckResult::RECOVERING:
      return "RECOVERING";
    case KsckCheckResult::UNDER_REPLICATED:
      return "UNDER-REPLICATED";
    case KsckCheckResult::CONSENSUS_MISMATCH:
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
    case KsckServerHealth::UNAVAILABLE:
      return 1;
    case KsckServerHealth::WRONG_SERVER_UUID:
      return 2;
    default:
      LOG(FATAL) << "Unknown KsckServerHealth";
  }
}

Status KsckResults::PrintTo(PrintMode mode, ostream& out) {
  // First, report on the masters and master tablet.
  std::sort(master_summaries.begin(), master_summaries.end(), ServerByHealthComparator);
  RETURN_NOT_OK(PrintServerHealthSummaries(KsckServerType::MASTER,
                                           master_summaries,
                                           out));
  if (mode == PrintMode::VERBOSE || master_consensus_conflict) {
    RETURN_NOT_OK(PrintConsensusMatrix(master_uuids,
                                       boost::none,
                                       master_consensus_state_map,
                                       out));
  }
  out << endl;

  // Then, on the health of the tablet servers.
  std::sort(tserver_summaries.begin(), tserver_summaries.end(), ServerByHealthComparator);
  RETURN_NOT_OK(PrintServerHealthSummaries(KsckServerType::TABLET_SERVER,
                                           tserver_summaries,
                                           out));
  if (!tserver_summaries.empty()) {
    out << endl;
  }

  // Then, on each tablet.
  RETURN_NOT_OK(PrintTabletSummaries(tablet_summaries, mode, out));
  if (!tablet_summaries.empty()) {
    out << endl;
  }

  // Then, summarize the tablets by table.
  // Sort the tables so unhealthy tables are easy to see at the bottom.
  std::sort(table_summaries.begin(), table_summaries.end(),
            [](const KsckTableSummary& left, const KsckTableSummary& right) {
              return std::make_pair(left.TableStatus() != KsckCheckResult::HEALTHY, left.name) <
                     std::make_pair(right.TableStatus() != KsckCheckResult::HEALTHY, right.name);
            });
  RETURN_NOT_OK(PrintTableSummaries(table_summaries, out));
  if (!table_summaries.empty()) {
    out << endl;
  }

  // Next, report on checksum scans.
  RETURN_NOT_OK(PrintChecksumResults(checksum_results, out));

  // And, add a summary of all the things we checked.
  RETURN_NOT_OK(PrintTotalCounts(*this, out));

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
    out << Substitute("Error from $1: $2", s.uuid, s.address, s.status.ToString()) << endl;
  }
  return Status::OK();
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
    out << "The cluster doesn't have any matching tablets" << endl;
    return Status::OK();
  }
  for (const auto& tablet_summary : tablet_summaries) {
    if (mode != PrintMode::VERBOSE && tablet_summary.result == KsckCheckResult::HEALTHY) {
      continue;
    }
    out << tablet_summary.status << endl;
    for (const KsckReplicaSummary& r : tablet_summary.replica_infos) {
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
    for (const KsckReplicaSummary& rs : tablet_summary.replica_infos) {
      ts_uuids.push_back(rs.ts_uuid);
    }
    KsckConsensusStateMap consensus_state_map;
    for (const KsckReplicaSummary& rs : tablet_summary.replica_infos) {
      if (rs.consensus_state) {
        InsertOrDie(&consensus_state_map, rs.ts_uuid, *rs.consensus_state);
      }
    }
    RETURN_NOT_OK(PrintConsensusMatrix(ts_uuids, master_cstate, consensus_state_map, out));
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
  out << endl;
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
                                       return acc + ts.replica_infos.size();
                                     });
  DataTable totals({ "", "Total Count" });
  totals.AddRow({ "Masters", to_string(results.master_summaries.size()) });
  totals.AddRow({ "Tablet Servers", to_string(results.tserver_summaries.size()) });
  totals.AddRow({ "Tables", to_string(results.table_summaries.size()) });
  totals.AddRow({ "Tablets", to_string(results.tablet_summaries.size()) });
  totals.AddRow({ "Replicas", to_string(num_replicas) });
  return totals.PrintTo(out);
}

} // namespace tools
} // namespace kudu
