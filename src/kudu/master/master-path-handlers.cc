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

#include "kudu/master/master-path-handlers.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <boost/bind.hpp> // IWYU pragma: keep
#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/partition.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master_options.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/master/ts_manager.h"
#include "kudu/server/monitored_task.h"
#include "kudu/server/webui_util.h"
#include "kudu/util/easy_json.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/string_case.h"
#include "kudu/util/url-coding.h"
#include "kudu/util/web_callback_registry.h"

namespace kudu {

using consensus::ConsensusStatePB;
using consensus::RaftPeerPB;
using std::array;
using std::map;
using std::ostringstream;
using std::pair;
using std::shared_ptr;
using std::string;
using std::vector;
using strings::Substitute;

namespace master {

MasterPathHandlers::~MasterPathHandlers() {
}

void MasterPathHandlers::HandleTabletServers(const Webserver::WebRequest& req,
                                             ostringstream* output) {
  vector<std::shared_ptr<TSDescriptor> > descs;
  master_->ts_manager()->GetAllDescriptors(&descs);

  *output << "<h1>Tablet Servers</h1>\n";
  *output << Substitute("<p>There are $0 registered tablet servers.</p>", descs.size());

  map<string, array<int, 2>> version_counts;
  vector<string> live_tserver_rows;
  vector<string> dead_tserver_rows;
  for (const std::shared_ptr<TSDescriptor>& desc : descs) {
    const string time_since_hb = StringPrintf("%.1fs", desc->TimeSinceHeartbeat().ToSeconds());
    ServerRegistrationPB reg;
    desc->GetRegistration(&reg);

    string row = Substitute("<tr><th>$0</th><td>$1</td><td><pre><code>$2</code></pre></td></tr>\n",
                            RegistrationToHtml(reg, desc->permanent_uuid()),
                            time_since_hb,
                            EscapeForHtmlToString(pb_util::SecureShortDebugString(reg)));

    if (desc->PresumedDead()) {
      version_counts[reg.software_version()][1]++;
      dead_tserver_rows.push_back(row);
    } else {
      version_counts[reg.software_version()][0]++;
      live_tserver_rows.push_back(row);
    }

  }

  *output << "<h3>Version Summary</h3>";
  *output << "<table class='table table-striped'>\n";
  *output << "<thead><tr><th>Version</th><th>Count (Live)</th><th>Count (Dead)</th></tr></thead>\n";
  *output << "<tbody>\n";
  for (const auto& entry : version_counts) {
    *output << Substitute("<tr><td>$0</td><td>$1</td><td>$2</td></tr>\n",
                          entry.first, entry.second[0], entry.second[1]);
  }
  *output << "</tbody></table>\n";

  *output << "<h3>" << "Registrations" << "</h3>\n";
  auto generate_table = [](const vector<string>& rows,
                           const string& header,
                           std::ostream* output) {
    if (!rows.empty()) {
      *output << "<h4>" << header << "</h4>\n";
      *output << "<table class='table table-striped'>\n";
      *output << "<thead><tr><th>UUID</th><th>Time since heartbeat</th>"
          "<th>Registration</th></tr></thead>\n";
      *output << "<tbody>\n";
      *output << JoinStrings(rows, "\n");
      *output << "</tbody></table>\n";
    }
  };
  generate_table(live_tserver_rows, "Live Tablet Servers", output);
  generate_table(dead_tserver_rows, "Dead Tablet Servers", output);
}

void MasterPathHandlers::HandleCatalogManager(const Webserver::WebRequest& req,
                                              EasyJson* output) {
  CatalogManager::ScopedLeaderSharedLock l(master_->catalog_manager());
  if (!l.first_failed_status().ok()) {
    (*output)["error"] = Substitute("Master is not ready: $0",  l.first_failed_status().ToString());
    return;
  }

  std::vector<scoped_refptr<TableInfo>> tables;
  master_->catalog_manager()->GetAllTables(&tables);
  int num_running_tables = 0;
  EasyJson tables_json = output->Set("tables", EasyJson::kArray);
  for (const scoped_refptr<TableInfo>& table : tables) {
    TableMetadataLock l(table.get(), TableMetadataLock::READ);
    if (!l.data().is_running()) {
      continue;
    }
    num_running_tables++; // Table count excluding deleted ones
    string state = SysTablesEntryPB_State_Name(l.data().pb.state());
    Capitalize(&state);
    EasyJson table_json = tables_json.PushBack(EasyJson::kObject);
    table_json["name"] = EscapeForHtmlToString(l.data().name());
    table_json["id"] = EscapeForHtmlToString(table->id());
    table_json["state"] = state;
    table_json["message"] = EscapeForHtmlToString(l.data().pb.state_msg());
  }
  (*output).Set<int64_t>("num_tables", num_running_tables);
}

namespace {

int RoleToSortIndex(RaftPeerPB::Role r) {
  switch (r) {
    case RaftPeerPB::LEADER: return 0;
    default: return 1 + static_cast<int>(r);
  }
}

bool CompareByRole(const pair<string, RaftPeerPB::Role>& a,
                   const pair<string, RaftPeerPB::Role>& b) {
  return RoleToSortIndex(a.second) < RoleToSortIndex(b.second);
}

} // anonymous namespace


void MasterPathHandlers::HandleTablePage(const Webserver::WebRequest& req,
                                         ostringstream* output) {
  // Parse argument.
  string table_id;
  if (!FindCopy(req.parsed_args, "id", &table_id)) {
    // TODO(wdb): webserver should give a way to return a non-200 response code
    *output << "Missing 'id' argument";
    return;
  }

  CatalogManager::ScopedLeaderSharedLock l(master_->catalog_manager());
  if (!l.first_failed_status().ok()) {
    *output << "Master is not ready: " << l.first_failed_status().ToString();
    return;
  }

  scoped_refptr<TableInfo> table;
  Status s = master_->catalog_manager()->GetTableInfo(table_id, &table);
  if (!s.ok()) {
    *output << "Master is not ready: " << s.ToString();
    return;
  }

  if (!table) {
    *output << "Table not found";
    return;
  }

  Schema schema;
  PartitionSchema partition_schema;
  string table_name;
  vector<scoped_refptr<TabletInfo> > tablets;
  {
    TableMetadataLock l(table.get(), TableMetadataLock::READ);
    table_name = l.data().name();
    *output << "<h1>Table: " << EscapeForHtmlToString(table_name)
            << " (" << EscapeForHtmlToString(table_id) << ")</h1>\n";

    *output << "<table class='table table-striped'>\n";
    *output << "  <tr><td>Version:</td><td>" << l.data().pb.version() << "</td></tr>\n";

    string state = SysTablesEntryPB_State_Name(l.data().pb.state());
    Capitalize(&state);
    *output << "  <tr><td>State:</td><td>"
            << state
            << EscapeForHtmlToString(l.data().pb.state_msg())
            << "</td></tr>\n";
    *output << "</table>\n";

    SchemaFromPB(l.data().pb.schema(), &schema);
    s = PartitionSchema::FromPB(l.data().pb.partition_schema(), schema, &partition_schema);
    if (!s.ok()) {
      *output << "Unable to decode partition schema: " << s.ToString();
      return;
    }
    table->GetAllTablets(&tablets);
  }

  *output << "<h3>Schema</h3>";
  HtmlOutputSchemaTable(schema, output);

  // Visit (& lock) each tablet once to build the partition schema, tablets summary,
  // and tablets detail tables all at once.
  std::vector<string> range_partitions;
  map<string, int> summary_states;
  std::ostringstream detail_output;

  detail_output << "<h4>Detail</h4>\n";
  detail_output << "<a href='#detail' data-toggle='collapse'>(toggle)</a>";
  detail_output << "<div id='detail' class='collapse'>\n";
  detail_output << "<table class='table table-striped table-hover'>\n";
  detail_output << "  <thead><tr><th>Tablet ID</th>"
                 << partition_schema.PartitionTableHeader(schema)
                 << "<th>State</th><th>Message</th><th>Peers</th></tr></thead>\n";
  detail_output << "<tbody>\n";
  for (const scoped_refptr<TabletInfo>& tablet : tablets) {
    vector<pair<string, RaftPeerPB::Role>> sorted_replicas;
    TabletMetadataLock l(tablet.get(), TabletMetadataLock::READ);

    summary_states[SysTabletsEntryPB_State_Name(l.data().pb.state())]++;
    if (l.data().pb.has_consensus_state()) {
      const ConsensusStatePB& cstate = l.data().pb.consensus_state();
      for (const auto& peer : cstate.committed_config().peers()) {
        RaftPeerPB::Role role = GetConsensusRole(peer.permanent_uuid(), cstate);
        string html;
        string location_html;
        shared_ptr<TSDescriptor> ts_desc;
        if (master_->ts_manager()->LookupTSByUUID(peer.permanent_uuid(), &ts_desc)) {
          location_html = TSDescriptorToHtml(*ts_desc.get(), tablet->tablet_id());
        } else {
          location_html = EscapeForHtmlToString(peer.permanent_uuid());
        }
        if (role == RaftPeerPB::LEADER) {
          html = Substitute("  <li><b>LEADER: $0</b></li>\n", location_html);
        } else {
          html = Substitute("  <li>$0: $1</li>\n",
                            RaftPeerPB_Role_Name(role), location_html);
        }
        sorted_replicas.emplace_back(html, role);
      }
    }
    std::sort(sorted_replicas.begin(), sorted_replicas.end(), &CompareByRole);

    // Generate the RaftConfig table cell.
    ostringstream raft_config_html;
    raft_config_html << "<ul>\n";
    for (const auto& e : sorted_replicas) {
      raft_config_html << e.first;
    }
    raft_config_html << "</ul>\n";

    Partition partition;
    Partition::FromPB(l.data().pb.partition(), &partition);

    // For each unique range partition, add a debug string to range_partitions.
    // To ensure uniqueness, only use partitions whose hash buckets are all 0.
    if (std::all_of(partition.hash_buckets().begin(),
                    partition.hash_buckets().end(),
                    [] (const int32_t& bucket) { return bucket == 0; })) {
        range_partitions.emplace_back(
            partition_schema.RangePartitionDebugString(partition.range_key_start(),
                                                       partition.range_key_end(),
                                                       schema));
    }

    string state = SysTabletsEntryPB_State_Name(l.data().pb.state());
    Capitalize(&state);
    detail_output << Substitute(
        "<tr><th>$0</th>$1<td>$2</td><td>$3</td><td>$4</td></tr>\n",
        tablet->tablet_id(),
        partition_schema.PartitionTableEntry(schema, partition),
        state,
        EscapeForHtmlToString(l.data().pb.state_msg()),
        raft_config_html.str());
  }
  detail_output << "</tbody></table></div>\n";

  // Write out the partition schema and range bound information...
  *output << "<h3>Partition Schema</h3>";
  *output << "<pre>";
  *output << EscapeForHtmlToString(partition_schema.DisplayString(schema, range_partitions));
  *output << "</pre>";

  // ...then the summary table...
  *output << "<h3>Tablets</h3>";
  *output << "<h4>Summary</h4>\n";
  *output << "<table class='table table-striped'>\n";
  *output << "<thead><tr><th>State</th><th>Count</th><th>Percentage</th></tr></thead>";
  *output << "<tbody>\n";
  for (const auto& entry : summary_states) {
    double percentage = tablets.size() == 0 ? 0.0 : (100.0 * entry.second) / tablets.size();
    *output << Substitute("<tr><td>$0</td><td>$1</td><td>$2</td></tr>\n",
                                 entry.first, entry.second, StringPrintf("%.2f", percentage));
  }
  *output << "</tbody></table>\n";

  // ...and finally the tablet detail table.
  *output << detail_output.str();

  *output << "<h3>Impala CREATE TABLE statement</h3>\n";
  string master_addresses;
  if (master_->opts().IsDistributed()) {
    vector<string> all_addresses;
    all_addresses.reserve(master_->opts().master_addresses.size());
    for (const HostPort& hp : master_->opts().master_addresses) {
      all_addresses.push_back(hp.ToString());
    }
    master_addresses = JoinElements(all_addresses, ",");
  } else {
    Sockaddr addr = master_->first_rpc_address();
    HostPort hp;
    Status s = HostPortFromSockaddrReplaceWildcard(addr, &hp);
    if (s.ok()) {
      master_addresses = hp.ToString();
    } else {
      LOG(WARNING) << "Unable to determine proper local hostname: " << s.ToString();
      master_addresses = addr.ToString();
    }
  }
  HtmlOutputImpalaSchema(table_name, schema, master_addresses, output);

  *output << "<h3>Tasks</h3>";
  std::vector<scoped_refptr<MonitoredTask> > task_list;
  table->GetTaskList(&task_list);
  HtmlOutputTaskList(task_list, output);
}

void MasterPathHandlers::HandleMasters(const Webserver::WebRequest& req,
                                       ostringstream* output) {
  vector<ServerEntryPB> masters;
  Status s = master_->ListMasters(&masters);
  if (!s.ok()) {
    s = s.CloneAndPrepend("Unable to list Masters");
    LOG(WARNING) << s.ToString();
    *output << "<h1>" << s.ToString() << "</h1>\n";
    return;
  }
  *output << "<h1> Masters </h1>\n";
  *output <<  "<table class='table table-striped'>\n";
  *output <<  "  <thead><tr><th>UUID</th><th>Role</th><th>Registration</th></tr></thead>\n";
  *output << "<tbody>\n";
  for (const ServerEntryPB& master : masters) {
    if (master.has_error()) {
      Status error = StatusFromPB(master.error());
      *output << Substitute("  <tr><td colspan=2><font color='red'><b>$0</b></font></td></tr>\n",
                            EscapeForHtmlToString(error.ToString()));
      continue;
    }
    string uuid_text = RegistrationToHtml(
        master.registration(),
        master.instance_id().permanent_uuid());
    string reg_str = EscapeForHtmlToString(
        pb_util::SecureShortDebugString(master.registration()));
    *output << Substitute(
        "  <tr><td>$0</td><td>$1</td><td><pre><code>$2</code></pre></td></tr>\n",
        uuid_text,
        master.has_role() ? RaftPeerPB_Role_Name(master.role()) : "N/A",
        reg_str);
  }

  *output << "</tbody></table>";
}

namespace {

// Visitor for the catalog table which dumps tables and tablets in a JSON format. This
// dump is interpreted by the CM agent in order to track time series entities in the SMON
// database.
//
// This implementation relies on scanning the catalog table directly instead of using the
// catalog manager APIs. This allows it to work even on a non-leader master, and avoids
// any requirement for locking. For the purposes of metrics entity gathering, it's OK to
// serve a slightly stale snapshot.
//
// It is tempting to directly dump the metadata protobufs using JsonWriter::Protobuf(...),
// but then we would be tying ourselves to textual compatibility of the PB field names in
// our catalog table. Instead, the implementation specifically dumps the fields that we
// care about.
//
// This should be considered a "stable" protocol -- do not rename, remove, or restructure
// without consulting with the CM team.
class JsonDumper : public TableVisitor, public TabletVisitor {
 public:
  explicit JsonDumper(JsonWriter* jw) : jw_(jw) {
  }

  Status VisitTable(const std::string& table_id,
                    const SysTablesEntryPB& metadata) OVERRIDE {
    if (metadata.state() != SysTablesEntryPB::RUNNING) {
      return Status::OK();
    }

    jw_->StartObject();
    jw_->String("table_id");
    jw_->String(table_id);

    jw_->String("table_name");
    jw_->String(metadata.name());

    jw_->String("state");
    jw_->String(SysTablesEntryPB::State_Name(metadata.state()));

    jw_->EndObject();
    return Status::OK();
  }

  Status VisitTablet(const std::string& table_id,
                     const std::string& tablet_id,
                     const SysTabletsEntryPB& metadata) OVERRIDE {
    if (metadata.state() != SysTabletsEntryPB::RUNNING) {
      return Status::OK();
    }

    jw_->StartObject();
    jw_->String("table_id");
    jw_->String(table_id);

    jw_->String("tablet_id");
    jw_->String(tablet_id);

    jw_->String("state");
    jw_->String(SysTabletsEntryPB::State_Name(metadata.state()));

    // Dump replica UUIDs
    if (metadata.has_consensus_state()) {
      const consensus::ConsensusStatePB& cs = metadata.consensus_state();
      jw_->String("replicas");
      jw_->StartArray();
      for (const RaftPeerPB& peer : cs.committed_config().peers()) {
        jw_->StartObject();
        jw_->String("type");
        jw_->String(RaftPeerPB::MemberType_Name(peer.member_type()));

        jw_->String("server_uuid");
        jw_->String(peer.permanent_uuid());

        jw_->String("addr");
        jw_->String(Substitute("$0:$1", peer.last_known_addr().host(),
                               peer.last_known_addr().port()));

        jw_->EndObject();
      }
      jw_->EndArray();

      if (!cs.leader_uuid().empty()) {
        jw_->String("leader");
        jw_->String(cs.leader_uuid());
      }
    }

    jw_->EndObject();
    return Status::OK();
  }

 private:
  JsonWriter* jw_;
};

void JsonError(const Status& s, ostringstream* out) {
  out->str("");
  JsonWriter jw(out, JsonWriter::COMPACT);
  jw.StartObject();
  jw.String("error");
  jw.String(s.ToString());
  jw.EndObject();
}
} // anonymous namespace

void MasterPathHandlers::HandleDumpEntities(const Webserver::WebRequest& req,
                                            ostringstream* output) {
  Status s = master_->catalog_manager()->CheckOnline();
  if (!s.ok()) {
    JsonError(s, output);
    return;
  }
  JsonWriter jw(output, JsonWriter::COMPACT);
  JsonDumper d(&jw);

  jw.StartObject();

  jw.String("tables");
  jw.StartArray();
  s = master_->catalog_manager()->sys_catalog()->VisitTables(&d);
  if (!s.ok()) {
    JsonError(s, output);
    return;
  }
  jw.EndArray();

  jw.String("tablets");
  jw.StartArray();
  s = master_->catalog_manager()->sys_catalog()->VisitTablets(&d);
  if (!s.ok()) {
    JsonError(s, output);
    return;
  }
  jw.EndArray();

  jw.String("tablet_servers");
  jw.StartArray();
  vector<std::shared_ptr<TSDescriptor> > descs;
  master_->ts_manager()->GetAllDescriptors(&descs);
  for (const std::shared_ptr<TSDescriptor>& desc : descs) {
    jw.StartObject();

    jw.String("uuid");
    jw.String(desc->permanent_uuid());

    ServerRegistrationPB reg;
    desc->GetRegistration(&reg);

    jw.String("rpc_addrs");
    jw.StartArray();
    for (const HostPortPB& host_port : reg.rpc_addresses()) {
      jw.String(Substitute("$0:$1", host_port.host(), host_port.port()));
    }
    jw.EndArray();

    jw.String("http_addrs");
    jw.StartArray();
    for (const HostPortPB& host_port : reg.http_addresses()) {
      jw.String(Substitute("$0://$1:$2",
                           reg.https_enabled() ? "https" : "http",
                           host_port.host(), host_port.port()));
    }
    jw.EndArray();

    jw.String("live");
    jw.Bool(!desc->PresumedDead());

    jw.String("millis_since_heartbeat");
    jw.Int64(desc->TimeSinceHeartbeat().ToMilliseconds());

    jw.String("version");
    jw.String(reg.software_version());

    jw.EndObject();
  }
  jw.EndArray();

  jw.EndObject();
}

Status MasterPathHandlers::Register(Webserver* server) {
  bool is_styled = true;
  bool is_on_nav_bar = true;
  server->RegisterPrerenderedPathHandler(
      "/tablet-servers", "Tablet Servers",
      boost::bind(&MasterPathHandlers::HandleTabletServers, this, _1, _2),
      is_styled, is_on_nav_bar);
  server->RegisterPathHandler(
      "/tables", "Tables",
      boost::bind(&MasterPathHandlers::HandleCatalogManager, this, _1, _2),
      is_styled, is_on_nav_bar);
  server->RegisterPrerenderedPathHandler(
      "/table", "",
      boost::bind(&MasterPathHandlers::HandleTablePage, this, _1, _2),
      is_styled, false);
  server->RegisterPrerenderedPathHandler(
      "/masters", "Masters",
      boost::bind(&MasterPathHandlers::HandleMasters, this, _1, _2),
      is_styled, is_on_nav_bar);
  server->RegisterPrerenderedPathHandler(
      "/dump-entities", "Dump Entities",
      boost::bind(&MasterPathHandlers::HandleDumpEntities, this, _1, _2),
      false, false);
  return Status::OK();
}

string MasterPathHandlers::TSDescriptorToHtml(const TSDescriptor& desc,
                                              const std::string& tablet_id) const {
  ServerRegistrationPB reg;
  desc.GetRegistration(&reg);

  if (reg.http_addresses().size() > 0) {
    return Substitute("<a href=\"$0://$1:$2/tablet?id=$3\">$4:$5</a>",
                      reg.https_enabled() ? "https" : "http",
                      reg.http_addresses(0).host(),
                      reg.http_addresses(0).port(),
                      EscapeForHtmlToString(tablet_id),
                      EscapeForHtmlToString(reg.http_addresses(0).host()),
                      reg.http_addresses(0).port());
  } else {
    return EscapeForHtmlToString(desc.permanent_uuid());
  }
}

string MasterPathHandlers::RegistrationToHtml(
    const ServerRegistrationPB& reg,
    const std::string& link_text) const {
  string link_html = EscapeForHtmlToString(link_text);
  if (reg.http_addresses().size() > 0) {
    link_html = Substitute("<a href=\"$0://$1:$2/\">$3</a>",
                           reg.https_enabled() ? "https" : "http",
                           reg.http_addresses(0).host(),
                           reg.http_addresses(0).port(), link_html);
  }
  return link_html;
}

} // namespace master
} // namespace kudu
