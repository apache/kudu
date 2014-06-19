// Copyright (c) 2013, Cloudera, inc.

#include "tserver/tserver-path-handlers.h"

#include <algorithm>
#include <sstream>
#include <string>
#include <tr1/memory>
#include <vector>

#include "gutil/map-util.h"
#include "gutil/strings/human_readable.h"
#include "gutil/strings/join.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/substitute.h"
#include "server/webui_util.h"
#include "tablet/tablet.pb.h"
#include "tablet/tablet_bootstrap.h"
#include "tablet/tablet_peer.h"
#include "tserver/scanners.h"
#include "tserver/tablet_server.h"
#include "tserver/ts_tablet_manager.h"
#include "util/url-coding.h"

using kudu::consensus::TransactionStatusPB;
using kudu::metadata::QuorumPB;
using kudu::metadata::QuorumPeerPB;
using kudu::tablet::TabletPeer;
using kudu::tablet::TabletStatusPB;
using kudu::tablet::Transaction;
using std::tr1::shared_ptr;
using std::endl;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tserver {

TabletServerPathHandlers::~TabletServerPathHandlers() {
}

Status TabletServerPathHandlers::Register(Webserver* server) {
  server->RegisterPathHandler(
    "/scanz",
    boost::bind(&TabletServerPathHandlers::HandleScansPage, this, _1, _2),
    true /* styled */, true /* is_on_nav_bar */);
  server->RegisterPathHandler(
    "/tablets",
    boost::bind(&TabletServerPathHandlers::HandleTabletsPage, this, _1, _2),
    true /* styled */, true /* is_on_nav_bar */);
  server->RegisterPathHandler(
    "/tablet",
    boost::bind(&TabletServerPathHandlers::HandleTabletPage, this, _1, _2),
    true /* styled */, false /* is_on_nav_bar */);
  server->RegisterPathHandler(
    "/transactionz",
    boost::bind(&TabletServerPathHandlers::HandleTransactionsPage, this, _1, _2),
    true /* styled */, true /* is_on_nav_bar */);
  server->RegisterPathHandler(
    "/tablet-rowsetlayout-svg",
    boost::bind(&TabletServerPathHandlers::HandleTabletSVGPage, this, _1, _2),
    true /* styled */, false /* is_on_nav_bar */);

  return Status::OK();
}


void TabletServerPathHandlers::HandleTransactionsPage(const Webserver::ArgumentMap& args,
                                                      std::stringstream* output) {
  bool as_text = ContainsKey(args, "raw");

  vector<shared_ptr<TabletPeer> > peers;
  tserver_->tablet_manager()->GetTabletPeers(&peers);

  string arg = FindWithDefault(args, "include_traces", "false");
  Transaction::TraceType trace_type = ParseLeadingBoolValue(
      arg.c_str(), false) ? Transaction::TRACE_TXNS : Transaction::NO_TRACE_TXNS;

  if (!as_text) {
    *output << "<h1>Transactions</h1>\n";
    *output << "<table class='table table-striped'>\n";
    *output << "   <tr><th>Tablet id</th><th>Op Id</th>"
      "<th>Transaction Type</th><th>Driver Type</th><th>"
      "Total time in-flight</th><th>Description</th></tr>\n";
  }

  BOOST_FOREACH(const shared_ptr<TabletPeer>& peer, peers) {
    vector<TransactionStatusPB> inflight;

    if (peer->tablet() == NULL) {
      continue;
    }

    peer->GetInFlightTransactions(trace_type, &inflight);
    BOOST_FOREACH(const TransactionStatusPB& inflight_tx, inflight) {
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
          "<tr><th>$0</th><th>$1</th><th>$2</th><th>$3</th><th>$4</th><th>$5</th></tr>\n",
          EscapeForHtmlToString(peer->tablet_id()),
          EscapeForHtmlToString(inflight_tx.op_id().ShortDebugString()),
          OperationType_Name(inflight_tx.tx_type()),
          DriverType_Name(inflight_tx.driver_type()),
          total_time_str,
          EscapeForHtmlToString(description));
      } else {
        (*output) << "Tablet: " << peer->tablet_id() << endl;
        (*output) << "Op ID: " << inflight_tx.op_id().ShortDebugString() << endl;
        (*output) << "Type: " << OperationType_Name(inflight_tx.tx_type()) << endl;
        (*output) << "Driver: " << DriverType_Name(inflight_tx.driver_type()) << endl;
        (*output) << "Running: " << total_time_str;
        (*output) << description << endl;
        (*output) << endl;
      }
    }
  }

  if (!as_text) {
    *output << "</table>\n";
  }
}

namespace {
string TabletLink(const string& id) {
  return Substitute("<a href=\"/tablet?id=$0\">$1</a>",
                    UrlEncodeToString(id),
                    EscapeForHtmlToString(id));
}
} // anonymous namespace

void TabletServerPathHandlers::HandleTabletsPage(const Webserver::ArgumentMap &args,
                                                 std::stringstream *output) {
  vector<shared_ptr<TabletPeer> > peers;
  tserver_->tablet_manager()->GetTabletPeers(&peers);

  *output << "<h1>Tablets</h1>\n";
  *output << "<table class='table table-striped'>\n";
  *output << "  <tr><th>Table name</th><th>Tablet ID</th>"
      "<th>Start key</th><th>End key</th>"
      "<th>State</th><th>On-disk size</th><th>Quorum</th><th>Last status</th></tr>\n";
  BOOST_FOREACH(const shared_ptr<TabletPeer>& peer, peers) {
    TabletStatusPB status;
    peer->GetTabletStatusPB(&status);
    string id = status.tablet_id();
    string table_name = status.table_name();
    string tablet_id_or_link;
    const Schema& schema = peer->status_listener()->schema();
    if (peer->tablet() != NULL) {
      tablet_id_or_link = TabletLink(id);
    } else {
      tablet_id_or_link = EscapeForHtmlToString(id);
    }
    string n_bytes = "";
    if (status.has_estimated_on_disk_size()) {
      n_bytes = HumanReadableNumBytes::ToString(status.estimated_on_disk_size());
    }
    string state = metadata::TabletStatePB_Name(status.state());
    if (status.state() == metadata::FAILED) {
      StrAppend(&state, ": ", EscapeForHtmlToString(peer->error().ToString()));
    }
    // TODO: would be nice to include some other stuff like memory usage
    (*output) << Substitute(
        // Table name, tablet id, start key, end key
        "<tr><th>$0</th><th>$1</th><th>$2</th><th>$3</th>"
        // State, on-disk size, quorum, last status
        "<th>$4</th><th>$5</th><th>$6</th><th>$7</tr>\n",
        EscapeForHtmlToString(table_name), // $0
        tablet_id_or_link, // $1
        EscapeForHtmlToString(schema.DebugEncodedRowKey(status.start_key())), // $2
        EscapeForHtmlToString(schema.DebugEncodedRowKey(status.end_key())), // $3
        state, n_bytes, // $4, $5
        QuorumPBToHtml(peer->Quorum()), // $6
        EscapeForHtmlToString(status.last_status())); // $7
  }
  *output << "</table>\n";
}

namespace {

bool CompareByRole(const QuorumPeerPB& a, const QuorumPeerPB& b) {
  if (a.has_role()) {
    if (b.has_role()) {
      return a.role() < b.role();
    } else {
      return true;
    }
  }
  return false;
};

} // anonymous namespace

string TabletServerPathHandlers::QuorumPBToHtml(const QuorumPB& quorum) const {
  std::stringstream html;

  html << "<ul>\n";
  std::vector<QuorumPeerPB> sorted_peers;
  sorted_peers.assign(quorum.peers().begin(), quorum.peers().end());
  std::sort(sorted_peers.begin(), sorted_peers.end(), &CompareByRole);
  BOOST_FOREACH(const QuorumPeerPB& peer, sorted_peers) {
    string peer_addr_or_uuid =
        peer.has_last_known_addr() ? peer.last_known_addr().host() : peer.permanent_uuid();
    peer_addr_or_uuid = EscapeForHtmlToString(peer_addr_or_uuid);
    if (peer.has_role() && peer.role() == QuorumPeerPB::LEADER) {
        html << Substitute("  <li><b>LEADER: $0</b></li>\n",
                           peer_addr_or_uuid);
    } else {
        html << Substitute(" <li>$0: $1</li>\n",
                           peer.has_role() ? QuorumPeerPB::Role_Name(peer.role()) : "UNKNOWN",
                           peer_addr_or_uuid);
      }
  }
  html << "</ul>\n";
  return html.str();
}

namespace {

bool GetTabletID(const Webserver::ArgumentMap& args, string* id, std::stringstream *out) {
  if (!FindCopy(args, "id", id)) {
    // TODO: webserver should give a way to return a non-200 response code
    (*out) << "Tablet missing 'id' argument";
    return false;
  }
  return true;
}

bool GetTabletPeer(TabletServer* tserver, const Webserver::ArgumentMap& args,
                   shared_ptr<TabletPeer>* peer, const string& tablet_id,
                   std::stringstream *out) {
  if (!tserver->tablet_manager()->LookupTablet(tablet_id, peer)) {
    (*out) << "Tablet " << EscapeForHtmlToString(tablet_id) << " not found";
    return false;
  }
  return true;
}

bool TabletBootstrapping(const shared_ptr<TabletPeer>& peer, const string& tablet_id,
                         std::stringstream* out) {
  if (peer->state() == metadata::BOOTSTRAPPING) {
    (*out) << "Tablet " << EscapeForHtmlToString(tablet_id) << " is still bootstrapping";
    return false;
  }
  return true;
}

// Returns true if the tablet_id was properly specified, the
// tablet is found, and is in a non-bootstrapping state.
bool LoadTablet(TabletServer* tserver,
                const Webserver::ArgumentMap& args,
                string* tablet_id, shared_ptr<TabletPeer>* peer,
                std::stringstream* out) {
  if (!GetTabletID(args, tablet_id, out)) return false;
  if (!GetTabletPeer(tserver, args, peer, *tablet_id, out)) return false;
  if (!TabletBootstrapping(*peer, *tablet_id, out)) return false;
  return true;
}

} // anonymous namespace

void TabletServerPathHandlers::HandleTabletPage(const Webserver::ArgumentMap &args,
                                                std::stringstream *output) {
  string tablet_id;
  shared_ptr<TabletPeer> peer;
  if (!LoadTablet(tserver_, args, &tablet_id, &peer, output)) return;

  string table_name = peer->tablet()->metadata()->table_name();

  *output << "<h1>Tablet " << EscapeForHtmlToString(tablet_id) << "</h1>\n";

  // Output schema in tabular format.
  *output << "<h2>Schema</h2>\n";
  shared_ptr<Schema> schema(peer->tablet()->schema());
  HtmlOutputSchemaTable(*schema.get(), output);

  // List of links to various tablet-specific info pages
  *output << "<ul>";
  // Link to output svg of current DiskRowSet layout over keyspace.
  string drsl_link = Substitute("<a href=\"/tablet-rowsetlayout-svg?id=$0\">$1</a>",
                                UrlEncodeToString(tablet_id),
                                "Rowset Layout Diagram");
  *output << "<li>" << drsl_link << "</li>";
  // End list
  *output << "</ul>\n";
}

void TabletServerPathHandlers::HandleTabletSVGPage(const Webserver::ArgumentMap &args,
                                                   std::stringstream* output) {
  string id;
  shared_ptr<TabletPeer> peer;
  if (!LoadTablet(tserver_, args, &id, &peer, output)) return;

  *output << "<h1>Rowset Layout Diagram "
          << TabletLink(id) << "</h1>\n";
  peer->tablet()->PrintRSLayout(output);

}

void TabletServerPathHandlers::HandleScansPage(const Webserver::ArgumentMap &args,
                                               std::stringstream* output) {
  *output << "<h1>Scans</h1>\n";
  *output << "<table class='table table-striped'>\n";
  *output << "<tr><th>Tablet id</th><th>Scanner id</th><th>Total time in-flight</th>"
      "<th>Time since last update</th><th>Requestor</th><th>Iterator Stats</th>"
      "<th>Pushed down key predicates</th><th>Other predicates</th></tr>\n";

  vector<SharedScanner> scanners;
  tserver_->scanner_manager()->ListScanners(&scanners);
  BOOST_FOREACH(const SharedScanner& scanner, scanners) {
    *output << ScannerToHtml(*scanner);
  }
  *output << "</table>";
}

string TabletServerPathHandlers::ScannerToHtml(const Scanner& scanner) const {
  std::stringstream html;
  uint64_t time_in_flight_us =
      MonoTime::Now(MonoTime::COARSE).GetDeltaSince(scanner.start_time()).ToMicroseconds();
  uint64_t time_since_last_access_us =
      scanner.TimeSinceLastAccess(MonoTime::Now(MonoTime::COARSE)).ToMicroseconds();

  vector<IteratorStats> stats;
  scanner.GetIteratorStats(&stats);
  html << Substitute("<tr><td>$0</td><td>$1</td><td>$2 us.</td><td>$3 us.</td><td>$4</td>"
                     "<td>$5</td>",
                     EscapeForHtmlToString(scanner.tablet_id()), // $0
                     EscapeForHtmlToString(scanner.id()), // $1
                     time_in_flight_us, time_since_last_access_us, // $2, $3
                     EscapeForHtmlToString(scanner.requestor_string()), // $4
                     IteratorStatsToHtml(stats)); // $5
  shared_ptr<TabletPeer> tablet_peer;
  if (!tserver_->tablet_manager()->LookupTablet(scanner.tablet_id(), &tablet_peer)) {
    html << Substitute("<td><b>Tablet $0 is no longer valid.</b></td></tr>\n",
                       scanner.tablet_id());
  } else {
    vector<string> pushed_preds;
    vector<string> other_preds;
    const Schema& schema = *tablet_peer->tablet()->schema();
    BOOST_FOREACH(const EncodedKeyRange* key_range, scanner.spec().encoded_ranges()) {
      string lower_bound_str = "''";
      string upper_bound_str = "''";
      if (key_range->has_lower_bound()) {
        lower_bound_str = key_range->lower_bound().Stringify(schema);
      }
      if (key_range->has_upper_bound()) {
        upper_bound_str = key_range->upper_bound().Stringify(schema);
      }
      pushed_preds.push_back(Substitute("lower bound: '$0', upper bound: '$1'",
                                        lower_bound_str, upper_bound_str));
    }
    BOOST_FOREACH(const ColumnRangePredicate& pred, scanner.spec().predicates()) {
      other_preds.push_back(pred.ToString());
    }
    string pushed_pred_str = JoinStrings(pushed_preds, "\n");
    string other_pred_str = JoinStrings(other_preds, "\n");
    html << Substitute("<td>$0</td><td>$1</td></tr>\n",
                       EscapeForHtmlToString(pushed_pred_str),
                       EscapeForHtmlToString(other_pred_str));
  }
  return html.str();
}

string TabletServerPathHandlers::IteratorStatsToHtml(const vector<IteratorStats>& stats) const {
  std::stringstream html;
  html << "<table>\n";
  html << "<tr><th>Column</th><th>Blocks read from disk</th>"
      "<th>Rows read from disk</th></tr>\n";
  for (size_t idx = 0; idx < stats.size(); idx++) {
    html << Substitute("<tr><td>$0</td><td>$1</td><td>$2</td></tr>\n",
                       idx, stats[idx].data_blocks_read_from_disk,
                       stats[idx].rows_read_from_disk);
  }
  html << "</table>\n";
  return html.str();
}

} // namespace tserver
} // namespace kudu
