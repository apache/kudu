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

#include "kudu/tserver/tserver-path-handlers.h"

#include <algorithm>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "kudu/common/scan_spec.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/server/webui_util.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/tablet_bootstrap.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/scanners.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/maintenance_manager.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/url-coding.h"

using kudu::consensus::GetConsensusRole;
using kudu::consensus::ConsensusStatePB;
using kudu::consensus::RaftPeerPB;
using kudu::consensus::TransactionStatusPB;
using kudu::MaintenanceManagerStatusPB;
using kudu::MaintenanceManagerStatusPB_CompletedOpPB;
using kudu::MaintenanceManagerStatusPB_MaintenanceOpPB;
using kudu::tablet::Tablet;
using kudu::tablet::TabletReplica;
using kudu::tablet::TabletStatePB;
using kudu::tablet::TabletStatusPB;
using kudu::tablet::Transaction;
using std::endl;
using std::shared_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tserver {

TabletServerPathHandlers::~TabletServerPathHandlers() {
}

Status TabletServerPathHandlers::Register(Webserver* server) {
  server->RegisterPrerenderedPathHandler(
    "/scans", "Scans",
    boost::bind(&TabletServerPathHandlers::HandleScansPage, this, _1, _2),
    true /* styled */, false /* is_on_nav_bar */);
  server->RegisterPrerenderedPathHandler(
    "/tablets", "Tablets",
    boost::bind(&TabletServerPathHandlers::HandleTabletsPage, this, _1, _2),
    true /* styled */, true /* is_on_nav_bar */);
  server->RegisterPrerenderedPathHandler(
    "/tablet", "",
    boost::bind(&TabletServerPathHandlers::HandleTabletPage, this, _1, _2),
    true /* styled */, false /* is_on_nav_bar */);
  server->RegisterPrerenderedPathHandler(
    "/transactions", "",
    boost::bind(&TabletServerPathHandlers::HandleTransactionsPage, this, _1, _2),
    true /* styled */, false /* is_on_nav_bar */);
  server->RegisterPrerenderedPathHandler(
    "/tablet-rowsetlayout-svg", "",
    boost::bind(&TabletServerPathHandlers::HandleTabletSVGPage, this, _1, _2),
    true /* styled */, false /* is_on_nav_bar */);
  server->RegisterPrerenderedPathHandler(
    "/tablet-consensus-status", "",
    boost::bind(&TabletServerPathHandlers::HandleConsensusStatusPage, this, _1, _2),
    true /* styled */, false /* is_on_nav_bar */);
  server->RegisterPrerenderedPathHandler(
    "/log-anchors", "",
    boost::bind(&TabletServerPathHandlers::HandleLogAnchorsPage, this, _1, _2),
    true /* styled */, false /* is_on_nav_bar */);
  server->RegisterPrerenderedPathHandler(
    "/dashboards", "Dashboards",
    boost::bind(&TabletServerPathHandlers::HandleDashboardsPage, this, _1, _2),
    true /* styled */, true /* is_on_nav_bar */);
  server->RegisterPrerenderedPathHandler(
    "/maintenance-manager", "",
    boost::bind(&TabletServerPathHandlers::HandleMaintenanceManagerPage, this, _1, _2),
    true /* styled */, false /* is_on_nav_bar */);

  return Status::OK();
}

void TabletServerPathHandlers::HandleTransactionsPage(const Webserver::WebRequest& req,
                                                      std::ostringstream* output) {
  bool as_text = ContainsKey(req.parsed_args, "raw");

  vector<scoped_refptr<TabletReplica> > replicas;
  tserver_->tablet_manager()->GetTabletReplicas(&replicas);

  string arg = FindWithDefault(req.parsed_args, "include_traces", "false");
  Transaction::TraceType trace_type = ParseLeadingBoolValue(
      arg.c_str(), false) ? Transaction::TRACE_TXNS : Transaction::NO_TRACE_TXNS;

  if (!as_text) {
    *output << "<h1>Transactions</h1>\n";
    *output << "<table class='table table-striped'>\n";
    *output << "   <thead><tr><th>Tablet id</th><th>Op Id</th>"
      "<th>Transaction Type</th><th>"
      "Total time in-flight</th><th>Description</th></tr></thead>\n";
    *output << "<tbody>\n";
  }

  for (const scoped_refptr<TabletReplica>& replica : replicas) {
    vector<TransactionStatusPB> inflight;

    if (replica->tablet() == nullptr) {
      continue;
    }

    replica->GetInFlightTransactions(trace_type, &inflight);
    for (const TransactionStatusPB& inflight_tx : inflight) {
      string total_time_str = Substitute("$0 us.", inflight_tx.running_for_micros());
      string description;
      if (trace_type == Transaction::TRACE_TXNS) {
        description = Substitute("$0, Trace: $1",
                                  inflight_tx.description(), inflight_tx.trace_buffer());
      } else {
        description = inflight_tx.description();
      }

      if (!as_text) {
        (*output) << Substitute(
          "<tr><th>$0</th><th>$1</th><th>$2</th><th>$3</th><th>$4</th></tr>\n",
          EscapeForHtmlToString(replica->tablet_id()),
          EscapeForHtmlToString(SecureShortDebugString(inflight_tx.op_id())),
          OperationType_Name(inflight_tx.tx_type()),
          total_time_str,
          EscapeForHtmlToString(description));
      } else {
        (*output) << "Tablet: " << replica->tablet_id() << endl;
        (*output) << "Op ID: " << SecureShortDebugString(inflight_tx.op_id()) << endl;
        (*output) << "Type: " << OperationType_Name(inflight_tx.tx_type()) << endl;
        (*output) << "Running: " << total_time_str;
        (*output) << description << endl;
        (*output) << endl;
      }
    }
  }

  if (!as_text) {
    *output << "</tbody></table>\n";
  }
}

namespace {
string TabletLink(const string& id) {
  return Substitute("<a href=\"/tablet?id=$0\">$1</a>",
                    UrlEncodeToString(id),
                    EscapeForHtmlToString(id));
}

} // anonymous namespace

void TabletServerPathHandlers::HandleTabletsPage(const Webserver::WebRequest& req,
                                                 std::ostringstream *output) {
  vector<scoped_refptr<TabletReplica>> replicas;
  tserver_->tablet_manager()->GetTabletReplicas(&replicas);

  // Sort by (table_name, tablet_id) tuples.
  std::sort(replicas.begin(), replicas.end(),
            [](const scoped_refptr<TabletReplica>& rep_a,
               const scoped_refptr<TabletReplica>& rep_b) {
              return std::make_pair(rep_a->tablet_metadata()->table_name(), rep_a->tablet_id()) <
                     std::make_pair(rep_b->tablet_metadata()->table_name(), rep_b->tablet_id());
            });

  // For assigning ids to table divs;
  int i = 0;
  auto generate_table = [this, &i](const vector<scoped_refptr<TabletReplica>>& replicas,
                                   ostream* output) {
    i++;

    *output << "<h4>Summary</h4>\n";
    map<string, int> tablet_statuses;
    for (const scoped_refptr<TabletReplica>& replica : replicas) {
      tablet_statuses[TabletStatePB_Name(replica->state())]++;
    }
    *output << "<table class='table table-striped table-hover'>\n";
    *output << "<thead><tr><th>Status</th><th>Count</th><th>Percentage</th></tr></thead>\n";
    *output << "<tbody>\n";
    for (const auto& entry : tablet_statuses) {
      double percent = replicas.size() == 0 ? 0 : (100.0 * entry.second) / replicas.size();
      *output << Substitute("<tr><td>$0</td><td>$1</td><td>$2</td></tr>\n",
                            entry.first,
                            entry.second,
                            StringPrintf("%.2f", percent));
    }
    *output << "</tbody>\n";
    *output << Substitute("<tfoot><tr><td>Total</td><td>$0</td><td></td></tr></tfoot>\n",
                          replicas.size());
    *output << "</table>\n";

    *output << "<h4>Detail</h4>";
    *output << Substitute("<a href='#detail$0' data-toggle='collapse'>(toggle)</a>\n", i);
    *output << Substitute("<div id='detail$0' class='collapse'>\n", i);
    *output << "<table class='table table-striped table-hover'>\n";
    *output << "<thead><tr><th>Table name</th><th>Tablet ID</th>"
        "<th>Partition</th><th>State</th><th>Write buffer memory usage</th>"
        "<th>On-disk size</th><th>RaftConfig</th><th>Last status</th></tr></thead>\n";
    *output << "<tbody>\n";
    for (const scoped_refptr<TabletReplica>& replica : replicas) {
      TabletStatusPB status;
      replica->GetTabletStatusPB(&status);
      string id = status.tablet_id();
      string table_name = status.table_name();
      string tablet_id_or_link;
      if (replica->tablet() != nullptr) {
        tablet_id_or_link = TabletLink(id);
      } else {
        tablet_id_or_link = EscapeForHtmlToString(id);
      }
      string mem_bytes = "";
      if (replica->tablet() != nullptr) {
        mem_bytes = HumanReadableNumBytes::ToString(
            replica->tablet()->mem_tracker()->consumption());
      }
      string n_bytes = "";
      if (status.has_estimated_on_disk_size()) {
        n_bytes = HumanReadableNumBytes::ToString(status.estimated_on_disk_size());
      }
      string partition = replica->tablet_metadata()
                                ->partition_schema()
                                 .PartitionDebugString(replica->tablet_metadata()->partition(),
                                                       replica->tablet_metadata()->schema());

      scoped_refptr<consensus::RaftConsensus> consensus = replica->shared_consensus();
      (*output) << Substitute(
          // Table name, tablet id, partition
          "<tr><td>$0</td><td>$1</td><td>$2</td>"
          // State, on-disk size, consensus configuration, last status
          "<td>$3</td><td>$4</td><td>$5</td><td>$6</td><td>$7</td></tr>\n",
          EscapeForHtmlToString(table_name), // $0
          tablet_id_or_link, // $1
          EscapeForHtmlToString(partition), // $2
          EscapeForHtmlToString(replica->HumanReadableState()), mem_bytes, n_bytes, // $3, $4, $5
          consensus ? ConsensusStatePBToHtml(consensus->ConsensusState()) : "", // $6
          EscapeForHtmlToString(status.last_status())); // $7
    }
    *output << "<tbody></table>\n</div>\n";
  };

  vector<scoped_refptr<TabletReplica>> live_replicas;
  vector<scoped_refptr<TabletReplica>> tombstoned_replicas;
  for (const scoped_refptr<TabletReplica>& replica : replicas) {
    if (replica->HumanReadableState() != "TABLET_DATA_TOMBSTONED") {
      live_replicas.push_back(replica);
    } else {
      tombstoned_replicas.push_back(replica);
    }
  }

  if (!live_replicas.empty()) {
    *output << "<h3>Live Tablets</h3>\n";
    generate_table(live_replicas, output);
  }
  if (!tombstoned_replicas.empty()) {
    *output << "<h3>Tombstoned Tablets</h3>\n";
    *output << "<p><small>Tombstoned tablets are tablets that previously "
               "stored a replica on this server.</small></p>";
    generate_table(tombstoned_replicas, output);
  }
}

namespace {

bool CompareByMemberType(const RaftPeerPB& a, const RaftPeerPB& b) {
  if (!a.has_member_type()) return false;
  if (!b.has_member_type()) return true;
  return a.member_type() < b.member_type();
}

} // anonymous namespace

string TabletServerPathHandlers::ConsensusStatePBToHtml(const ConsensusStatePB& cstate) const {
  std::ostringstream html;

  html << "<ul>\n";
  std::vector<RaftPeerPB> sorted_peers;
  sorted_peers.assign(cstate.committed_config().peers().begin(),
                      cstate.committed_config().peers().end());
  std::sort(sorted_peers.begin(), sorted_peers.end(), &CompareByMemberType);
  for (const RaftPeerPB& peer : sorted_peers) {
    string peer_addr_or_uuid =
        peer.has_last_known_addr() ? Substitute("$0:$1",
                                                peer.last_known_addr().host(),
                                                peer.last_known_addr().port())
                                   : peer.permanent_uuid();
    peer_addr_or_uuid = EscapeForHtmlToString(peer_addr_or_uuid);
    string role_name = RaftPeerPB::Role_Name(GetConsensusRole(peer.permanent_uuid(), cstate));
    string formatted = Substitute("$0: $1", role_name, peer_addr_or_uuid);
    // Make the local peer bold.
    if (peer.permanent_uuid() == tserver_->instance_pb().permanent_uuid()) {
      formatted = Substitute("<b>$0</b>", formatted);
    }

    html << Substitute(" <li>$0</li>\n", formatted);
  }
  html << "</ul>\n";
  return html.str();
}

namespace {

bool GetTabletID(const Webserver::WebRequest& req, string* id, std::ostringstream* out) {
  if (!FindCopy(req.parsed_args, "id", id)) {
    // TODO: webserver should give a way to return a non-200 response code
    (*out) << "Tablet missing 'id' argument";
    return false;
  }
  return true;
}

bool GetTabletReplica(TabletServer* tserver, const Webserver::WebRequest& req,
                      scoped_refptr<TabletReplica>* replica, const string& tablet_id,
                      std::ostringstream* out) {
  if (!tserver->tablet_manager()->LookupTablet(tablet_id, replica)) {
    (*out) << "Tablet " << EscapeForHtmlToString(tablet_id) << " not found";
    return false;
  }
  return true;
}

bool TabletBootstrapping(const scoped_refptr<TabletReplica>& replica, const string& tablet_id,
                         std::ostringstream* out) {
  if (replica->state() == tablet::BOOTSTRAPPING) {
    (*out) << "Tablet " << EscapeForHtmlToString(tablet_id) << " is still bootstrapping";
    return false;
  }
  return true;
}

// Returns true if the tablet_id was properly specified, the
// tablet is found, and is in a non-bootstrapping state.
bool LoadTablet(TabletServer* tserver,
                const Webserver::WebRequest& req,
                string* tablet_id, scoped_refptr<TabletReplica>* replica,
                std::ostringstream* out) {
  if (!GetTabletID(req, tablet_id, out)) return false;
  if (!GetTabletReplica(tserver, req, replica, *tablet_id, out)) return false;
  if (!TabletBootstrapping(*replica, *tablet_id, out)) return false;
  return true;
}

} // anonymous namespace

void TabletServerPathHandlers::HandleTabletPage(const Webserver::WebRequest& req,
                                                std::ostringstream *output) {
  string tablet_id;
  scoped_refptr<TabletReplica> replica;
  if (!LoadTablet(tserver_, req, &tablet_id, &replica, output)) return;

  string table_name = replica->tablet_metadata()->table_name();
  RaftPeerPB::Role role = RaftPeerPB::UNKNOWN_ROLE;
  auto consensus = replica->consensus();
  if (consensus) {
    role = consensus->role();
  }

  *output << "<h1>Tablet " << EscapeForHtmlToString(tablet_id)
          << " (" << replica->HumanReadableState()
          << "/" << RaftPeerPB::Role_Name(role) << ")</h1>\n";
  *output << "<h3>Table " << EscapeForHtmlToString(table_name) << "</h3>";

  // Output schema in tabular format.
  *output << "<h2>Schema</h2>\n";
  const Schema& schema = replica->tablet_metadata()->schema();
  HtmlOutputSchemaTable(schema, output);

  *output << "<h2>Other Tablet Info Pages</h2>" << endl;

  // List of links to various tablet-specific info pages
  *output << "<ul>";

  // Link to output svg of current DiskRowSet layout over keyspace.
  *output << "<li>" << Substitute("<a href=\"/tablet-rowsetlayout-svg?id=$0\">$1</a>",
                                  UrlEncodeToString(tablet_id),
                                  "Rowset Layout Diagram")
          << "</li>" << endl;

  // Link to consensus status page.
  *output << "<li>" << Substitute("<a href=\"/tablet-consensus-status?id=$0\">$1</a>",
                                  UrlEncodeToString(tablet_id),
                                  "Consensus Status")
          << "</li>" << endl;

  // Log anchors info page.
  *output << "<li>" << Substitute("<a href=\"/log-anchors?id=$0\">$1</a>",
                                  UrlEncodeToString(tablet_id),
                                  "Tablet Log Anchors")
          << "</li>" << endl;

  // End list
  *output << "</ul>\n";
}

void TabletServerPathHandlers::HandleTabletSVGPage(const Webserver::WebRequest& req,
                                                   std::ostringstream* output) {
  string id;
  scoped_refptr<TabletReplica> replica;
  if (!LoadTablet(tserver_, req, &id, &replica, output)) return;
  shared_ptr<Tablet> tablet = replica->shared_tablet();
  if (!tablet) {
    *output << "Tablet " << EscapeForHtmlToString(id) << " not running";
    return;
  }

  *output << "<h1>Rowset Layout Diagram for Tablet "
          << TabletLink(id) << "</h1>\n";
  tablet->PrintRSLayout(output);

}

void TabletServerPathHandlers::HandleLogAnchorsPage(const Webserver::WebRequest& req,
                                                    std::ostringstream* output) {
  string tablet_id;
  scoped_refptr<TabletReplica> replica;
  if (!LoadTablet(tserver_, req, &tablet_id, &replica, output)) return;

  *output << "<h1>Log Anchors for Tablet " << EscapeForHtmlToString(tablet_id) << "</h1>"
          << std::endl;

  string dump = replica->log_anchor_registry()->DumpAnchorInfo();
  *output << "<pre>" << EscapeForHtmlToString(dump) << "</pre>" << std::endl;
}

void TabletServerPathHandlers::HandleConsensusStatusPage(const Webserver::WebRequest& req,
                                                         std::ostringstream* output) {
  string id;
  scoped_refptr<TabletReplica> replica;
  if (!LoadTablet(tserver_, req, &id, &replica, output)) return;
  scoped_refptr<consensus::RaftConsensus> consensus = replica->shared_consensus();
  if (!consensus) {
    *output << "Tablet " << EscapeForHtmlToString(id) << " not running";
    return;
  }
  consensus->DumpStatusHtml(*output);
}

void TabletServerPathHandlers::HandleScansPage(const Webserver::WebRequest& req,
                                               std::ostringstream* output) {
  *output << "<h1>Scans</h1>\n";
  *output << "<table class='table table-striped'>\n";
  *output << "<thead><tr><th>Tablet id</th><th>Scanner id</th><th>Total time in-flight</th>"
      "<th>Time since last update</th><th>Requestor</th><th>Iterator Stats</th>"
      "<th>Pushed down key predicates</th><th>Other predicates</th></tr></thead>\n";
  *output << "<tbody>\n";

  vector<SharedScanner> scanners;
  tserver_->scanner_manager()->ListScanners(&scanners);
  for (const SharedScanner& scanner : scanners) {
    *output << ScannerToHtml(*scanner);
  }
  *output << "</tbody></table>";
}

string TabletServerPathHandlers::ScannerToHtml(const Scanner& scanner) const {
  std::ostringstream html;
  uint64_t time_in_flight_us =
      (MonoTime::Now() - scanner.start_time()).ToMicroseconds();
  uint64_t time_since_last_access_us =
      scanner.TimeSinceLastAccess(MonoTime::Now()).ToMicroseconds();

  html << Substitute("<tr><td>$0</td><td>$1</td><td>$2 us.</td><td>$3 us.</td><td>$4</td>",
                     EscapeForHtmlToString(scanner.tablet_id()), // $0
                     EscapeForHtmlToString(scanner.id()), // $1
                     time_in_flight_us, time_since_last_access_us, // $2, $3
                     EscapeForHtmlToString(scanner.requestor_string())); // $4


  if (!scanner.IsInitialized()) {
    html << "<td colspan=\"3\">&lt;not yet initialized&gt;</td></tr>";
    return html.str();
  }

  const Schema* projection = &scanner.iter()->schema();

  vector<IteratorStats> stats;
  scanner.GetIteratorStats(&stats);
  CHECK_EQ(stats.size(), projection->num_columns());
  html << Substitute("<td>$0</td>", IteratorStatsToHtml(*projection, stats));
  scoped_refptr<TabletReplica> tablet_replica;
  if (!tserver_->tablet_manager()->LookupTablet(scanner.tablet_id(), &tablet_replica)) {
    html << Substitute("<td colspan=\"2\"><b>Tablet $0 is no longer valid.</b></td></tr>\n",
                       scanner.tablet_id());
  } else {
    string range_pred_str;
    vector<string> other_preds;
    const ScanSpec& spec = scanner.spec();
    if (spec.lower_bound_key() || spec.exclusive_upper_bound_key()) {
      range_pred_str = EncodedKey::RangeToString(spec.lower_bound_key(),
                                                 spec.exclusive_upper_bound_key());
    }
    for (const auto& col_pred : scanner.spec().predicates()) {
      int32_t col_idx = projection->find_column(col_pred.first);
      if (col_idx == Schema::kColumnNotFound) {
        other_preds.emplace_back("unknown column");
      } else {
        other_preds.push_back(col_pred.second.ToString());
      }
    }
    string other_pred_str = JoinStrings(other_preds, "\n");
    html << Substitute("<td>$0</td><td>$1</td></tr>\n",
                       EscapeForHtmlToString(range_pred_str),
                       EscapeForHtmlToString(other_pred_str));
  }
  return html.str();
}

string TabletServerPathHandlers::IteratorStatsToHtml(const Schema& projection,
                                                     const vector<IteratorStats>& stats) const {
  std::ostringstream html;
  html << "<table>\n";
  html << "<tr><th>Column</th>"
       << "<th>Blocks read from disk</th>"
       << "<th>Bytes read from disk</th>"
       << "<th>Cells read from disk</th>"
       << "</tr>\n";
  for (size_t idx = 0; idx < stats.size(); idx++) {
    // We use 'title' attributes so that if the user hovers over the value, they get a
    // human-readable tooltip.
    html << Substitute("<tr>"
                       "<td>$0</td>"
                       "<td title=\"$1\">$2</td>"
                       "<td title=\"$3\">$4</td>"
                       "<td title=\"$5\">$6</td>"
                       "</tr>\n",
                       EscapeForHtmlToString(projection.column(idx).name()), // $0
                       HumanReadableInt::ToString(stats[idx].data_blocks_read_from_disk), // $1
                       stats[idx].data_blocks_read_from_disk, // $2
                       HumanReadableNumBytes::ToString(stats[idx].bytes_read_from_disk), // $3
                       stats[idx].bytes_read_from_disk, // $4
                       HumanReadableInt::ToString(stats[idx].cells_read_from_disk), // $5
                       stats[idx].cells_read_from_disk); // $6
  }
  html << "</table>\n";
  return html.str();
}

void TabletServerPathHandlers::HandleDashboardsPage(const Webserver::WebRequest& req,
                                                    std::ostringstream* output) {

  *output << "<h3>Dashboards</h3>\n";
  *output << "<table class='table table-striped'>\n";
  *output << "  <thead><tr><th>Dashboard</th><th>Description</th></tr></thead>\n";
  *output << "  <tbody\n";
  *output << GetDashboardLine("scans", "Scans", "List of scanners that are currently running.");
  *output << GetDashboardLine("transactions", "Transactions", "List of transactions that are "
                                                              "currently running.");
  *output << GetDashboardLine("maintenance-manager", "Maintenance Manager",
                              "List of operations that are currently running and those "
                              "that are registered.");
  *output << "</tbody></table>\n";
}

string TabletServerPathHandlers::GetDashboardLine(const std::string& link,
                                                  const std::string& text,
                                                  const std::string& desc) {
  return Substitute("  <tr><td><a href=\"$0\">$1</a></td><td>$2</td></tr>\n",
                    EscapeForHtmlToString(link),
                    EscapeForHtmlToString(text),
                    EscapeForHtmlToString(desc));
}

void TabletServerPathHandlers::HandleMaintenanceManagerPage(const Webserver::WebRequest& req,
                                                            std::ostringstream* output) {
  MaintenanceManager* manager = tserver_->maintenance_manager();
  MaintenanceManagerStatusPB pb;
  manager->GetMaintenanceManagerStatusDump(&pb);
  if (ContainsKey(req.parsed_args, "raw")) {
    *output << SecureDebugString(pb);
    return;
  }

  int ops_count = pb.registered_operations_size();

  *output << "<h1>Maintenance Manager state</h1>\n";
  *output << "<h3>Running operations</h3>\n";
  *output << "<table class='table table-striped'>\n";
  *output << "  <thead><tr><th>Name</th><th>Instances running</th></tr></thead>\n";
  *output << "<tbody>\n";
  for (int i = 0; i < ops_count; i++) {
    MaintenanceManagerStatusPB_MaintenanceOpPB op_pb = pb.registered_operations(i);
    if (op_pb.running() > 0) {
      *output <<  Substitute("<tr><td>$0</td><td>$1</td></tr>\n",
                             EscapeForHtmlToString(op_pb.name()),
                             op_pb.running());
    }
  }
  *output << "</tbody></table>\n";

  *output << "<h3>Recent completed operations</h3>\n";
  *output << "<table class='table table-striped'>\n";
  *output << "  <thead><tr><th>Name</th><th>Duration</th>"
      "<th>Time since op started</th></tr></thead>\n";
  *output << "<tbody>\n";
  for (int i = 0; i < pb.completed_operations_size(); i++) {
    MaintenanceManagerStatusPB_CompletedOpPB op_pb = pb.completed_operations(i);
    *output <<  Substitute("<tr><td>$0</td><td>$1</td><td>$2</td></tr>\n",
                           EscapeForHtmlToString(op_pb.name()),
                           HumanReadableElapsedTime::ToShortString(
                               op_pb.duration_millis() / 1000.0),
                           HumanReadableElapsedTime::ToShortString(
                               op_pb.secs_since_start()));
  }
  *output << "</tbody></table>\n";

  *output << "<h3>Non-running operations</h3>\n";
  *output << "<table class='table table-striped'>\n";
  *output << "  <thead><tr><th>Name</th><th>Runnable</th><th>RAM anchored</th>\n"
          << "       <th>Logs retained</th><th>Perf</th></tr></thead>\n";
  *output << "<tbody>\n";
  for (int i = 0; i < ops_count; i++) {
    MaintenanceManagerStatusPB_MaintenanceOpPB op_pb = pb.registered_operations(i);
    if (op_pb.running() == 0) {
      *output << Substitute("<tr><td>$0</td><td>$1</td><td>$2</td><td>$3</td><td>$4</td></tr>\n",
                            EscapeForHtmlToString(op_pb.name()),
                            op_pb.runnable(),
                            HumanReadableNumBytes::ToString(op_pb.ram_anchored_bytes()),
                            HumanReadableNumBytes::ToString(op_pb.logs_retained_bytes()),
                            op_pb.perf_improvement());
    }
  }
  *output << "</tbody></table>\n";
}

} // namespace tserver
} // namespace kudu
