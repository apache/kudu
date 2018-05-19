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

#include "kudu/master/master_path_handlers.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <iosfwd>
#include <limits>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>
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
#include "kudu/gutil/strings/numbers.h"
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
#include "kudu/util/cow_object.h"
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

void MasterPathHandlers::HandleTabletServers(const Webserver::WebRequest& /*req*/,
                                             Webserver::WebResponse* resp) {
  EasyJson* output = resp->output;
  vector<std::shared_ptr<TSDescriptor>> descs;
  master_->ts_manager()->GetAllDescriptors(&descs);

  (*output)["num_ts"] = std::to_string(descs.size());

  // In mustache, when conditionally rendering a section of the template based
  // on a key, the value becomes the context. In the subcontext, searches for
  // keys used to display a value (e.g. {{my_key}}) recurse into the parent
  // context-- but not when used to define a subsection (e.g. {{#my_list}}.
  // Thus, we use a negative test {{^has_no_live_ts}} to decide if we
  // should render the live and dead tablet server tables.
  bool has_no_live_ts = true;
  bool has_no_dead_ts = true;
  output->Set("live_tservers", EasyJson::kArray);
  output->Set("dead_tservers", EasyJson::kArray);
  map<string, array<int, 2>> version_counts;
  for (const std::shared_ptr<TSDescriptor>& desc : descs) {
    string ts_key = desc->PresumedDead() ? "dead_tservers" : "live_tservers";
    EasyJson ts_json = (*output)[ts_key].PushBack(EasyJson::kObject);

    ServerRegistrationPB reg;
    desc->GetRegistration(&reg);
    ts_json["uuid"] = desc->permanent_uuid();
    if (!reg.http_addresses().empty()) {
      ts_json["target"] = Substitute("$0://$1:$2/",
                                     reg.https_enabled() ? "https" : "http",
                                     reg.http_addresses(0).host(),
                                     reg.http_addresses(0).port());
    }
    ts_json["time_since_hb"] = StringPrintf("%.1fs", desc->TimeSinceHeartbeat().ToSeconds());
    ts_json["registration"] = pb_util::SecureShortDebugString(reg);
    version_counts[reg.software_version()][desc->PresumedDead() ? 1 : 0]++;
    has_no_live_ts &= desc->PresumedDead();
    has_no_dead_ts &= !desc->PresumedDead();
  }
  (*output)["has_no_live_ts"] = has_no_live_ts;
  (*output)["has_no_dead_ts"] = has_no_dead_ts;

  output->Set("version_counts", EasyJson::kArray);
  for (const auto& entry : version_counts) {
    EasyJson version_count_json = (*output)["version_counts"].PushBack(EasyJson::kObject);
    version_count_json["version"] = entry.first;
    version_count_json["live"] = Substitute("$0", entry.second[0]);
    version_count_json["dead"] = Substitute("$0", entry.second[1]);
  }
}

namespace {

// Extracts the value of the 'redirects' parameter from 'req'; returns 0 if the
// parameter doesn't exist or couldn't be parsed.
int ExtractRedirectsFromRequest(const Webserver::WebRequest& req) {
  string redirects_str;
  int redirects = 0;
  if (FindCopy(req.parsed_args, "redirects", &redirects_str)) {
    if (!safe_strto32(redirects_str, &redirects)) {
      return 0;
    }
  }
  return redirects;
}

} // anonymous namespace

void MasterPathHandlers::HandleCatalogManager(const Webserver::WebRequest& req,
                                              Webserver::WebResponse* resp) {
  EasyJson* output = resp->output;
  CatalogManager::ScopedLeaderSharedLock l(master_->catalog_manager());
  if (!l.catalog_status().ok()) {
    (*output)["error"] = Substitute("Master is not ready: $0",  l.catalog_status().ToString());
    return;
  }
  if (!l.leader_status().ok()) {
    // Track redirects to prevent a redirect loop.
    int redirects = ExtractRedirectsFromRequest(req);
    SetupLeaderMasterRedirect("tables?", redirects, output);
    return;
  }

  std::vector<scoped_refptr<TableInfo>> tables;
  master_->catalog_manager()->GetAllTables(&tables);
  int num_running_tables = 0;
  EasyJson tables_json = output->Set("tables", EasyJson::kArray);
  for (const scoped_refptr<TableInfo>& table : tables) {
    TableMetadataLock l(table.get(), LockMode::READ);
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


// Holds info about a peer for use in the tablet detail table.
struct TabletDetailPeerInfo {
  string text;
  string target;
  string role;
  bool is_leader;
};

int RoleToSortIndex(RaftPeerPB::Role r) {
  switch (r) {
    case RaftPeerPB::LEADER: return 0;
    default: return 1 + static_cast<int>(r);
  }
}

bool CompareByRole(const pair<TabletDetailPeerInfo, RaftPeerPB::Role>& a,
                   const pair<TabletDetailPeerInfo, RaftPeerPB::Role>& b) {
  return RoleToSortIndex(a.second) < RoleToSortIndex(b.second);
}

} // anonymous namespace


void MasterPathHandlers::HandleTablePage(const Webserver::WebRequest& req,
                                         Webserver::WebResponse* resp) {
  EasyJson* output = resp->output;
  // Parse argument.
  string table_id;
  if (!FindCopy(req.parsed_args, "id", &table_id)) {
    resp->status_code = HttpStatusCode::BadRequest;
    (*output)["error"] = "Missing 'id' argument";
    return;
  }

  CatalogManager::ScopedLeaderSharedLock l(master_->catalog_manager());
  if (!l.catalog_status().ok()) {
    (*output)["error"] = Substitute("Master is not ready: $0", l.catalog_status().ToString());
    return;
  }
  if (!l.leader_status().ok()) {
    // It's possible to respond 307 Temporary Redirect and automatically redirect with
    // a Location header, but this would likely confuse users about which master's web ui
    // they are looking at. Instead, we show a link users can click to go to the leader master.
    // We track redirects to prevent a redirect loop.
    int redirects = ExtractRedirectsFromRequest(req);
    SetupLeaderMasterRedirect(Substitute("table?id=$0", table_id), redirects, output);
    return;
  }

  scoped_refptr<TableInfo> table;
  Status s = master_->catalog_manager()->GetTableInfo(table_id, &table);
  if (!s.ok()) {
    resp->status_code = HttpStatusCode::ServiceUnavailable;
    (*output)["error"] = Substitute("Master is not ready: $0", s.ToString());
    return;
  }

  if (!table) {
    resp->status_code = HttpStatusCode::NotFound;
    (*output)["error"] = "Table not found";
    return;
  }

  Schema schema;
  PartitionSchema partition_schema;
  vector<scoped_refptr<TabletInfo>> tablets;
  {
    TableMetadataLock l(table.get(), LockMode::READ);
    (*output)["name"] = l.data().name();
    (*output)["id"] = table_id;
    (*output)["version"] = l.data().pb.version();

    string state = SysTablesEntryPB_State_Name(l.data().pb.state());
    Capitalize(&state);
    (*output)["state"] = state;
    string state_msg = l.data().pb.state_msg();
    if (!state_msg.empty()) {
      (*output)["state_msg"] = state_msg;
    }

    s = SchemaFromPB(l.data().pb.schema(), &schema);
    if (!s.ok()) {
      (*output)["error"] = Substitute("Unable to decode schema: $0", s.ToString());
      return;
    }
    s = PartitionSchema::FromPB(l.data().pb.partition_schema(), schema, &partition_schema);
    if (!s.ok()) {
      (*output)["error"] =
          Substitute("Unable to decode partition schema: $0", s.ToString());
      return;
    }
    table->GetAllTablets(&tablets);
  }

  SchemaToJson(schema, output);

  // We have to collate partition schema and tablet information in order to set
  // up the partition schema, tablet summary, and tablet detail tables.
  std::vector<string> range_partitions;
  map<string, int> summary_states;
  map<string, int> replica_counts;
  (*output)["detail_partition_schema_header"] = partition_schema.PartitionTableHeader(schema);
  EasyJson tablets_detail_json = output->Set("tablets_detail", EasyJson::kArray);
  for (const scoped_refptr<TabletInfo>& tablet : tablets) {
    vector<pair<TabletDetailPeerInfo, RaftPeerPB::Role>> sorted_replicas;
    TabletMetadataLock l(tablet.get(), LockMode::READ);

    // Count states for tablet summary.
    summary_states[SysTabletsEntryPB_State_Name(l.data().pb.state())]++;

    // Collect details about each tablet replica.
    if (l.data().pb.has_consensus_state()) {
      const ConsensusStatePB& cstate = l.data().pb.consensus_state();
      for (const auto& peer : cstate.committed_config().peers()) {
        replica_counts[peer.permanent_uuid()]++;
        TabletDetailPeerInfo peer_info;
        shared_ptr<TSDescriptor> ts_desc;
        if (master_->ts_manager()->LookupTSByUUID(peer.permanent_uuid(), &ts_desc)) {
          auto link_pair = TSDescToLinkPair(*ts_desc.get(), tablet->id());
          peer_info.text = std::move(link_pair.first);
          peer_info.target = std::move(link_pair.second);
        } else {
          peer_info.text = peer.permanent_uuid();
        }
        RaftPeerPB::Role role = GetConsensusRole(peer.permanent_uuid(), cstate);
        peer_info.role = RaftPeerPB_Role_Name(role);
        peer_info.is_leader = role == RaftPeerPB::LEADER;
        sorted_replicas.emplace_back(std::make_pair(peer_info, role));
      }
    }
    std::sort(sorted_replicas.begin(), sorted_replicas.end(), &CompareByRole);

    // Generate a readable description of the partition of each tablet, used
    // both for each tablet's details and the readable range partition schema.
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

    // Combine the tablet details and partition info for each tablet.
    EasyJson tablet_detail_json = tablets_detail_json.PushBack(EasyJson::kObject);
    tablet_detail_json["id"] = tablet->id();
    tablet_detail_json["partition_cols"] = partition_schema.PartitionTableEntry(schema, partition);
    string state = SysTabletsEntryPB_State_Name(l.data().pb.state());
    Capitalize(&state);
    tablet_detail_json["state"] = state;
    tablet_detail_json["state_msg"] = l.data().pb.state_msg();
    EasyJson peers_json = tablet_detail_json.Set("peers", EasyJson::kArray);
    for (const auto& e : sorted_replicas) {
      EasyJson peer_json = peers_json.PushBack(EasyJson::kObject);
      peer_json["text"] = e.first.text;
      if (!e.first.target.empty()) {
        peer_json["target"] = e.first.target;
      }
      peer_json["role"] = e.first.role;
      peer_json["is_leader"] = e.first.is_leader;
    }
  }

  (*output)["partition_schema"] = partition_schema.DisplayString(schema, range_partitions);

  EasyJson summary_json = output->Set("tablets_summary", EasyJson::kArray);
  for (const auto& entry : summary_states) {
    EasyJson state_json = summary_json.PushBack(EasyJson::kObject);
    state_json["state"] = entry.first;
    state_json["count"] = entry.second;
    double percentage = (100.0 * entry.second) / tablets.size();
    state_json["percentage"] = tablets.empty() ? "0.0" : StringPrintf("%.2f", percentage);
  }

  // Set up the report on replica distribution.
  EasyJson replica_dist_json = output->Set("replica_distribution", EasyJson::kObject);
  EasyJson counts_json = replica_dist_json.Set("counts", EasyJson::kArray);
  int min_count = replica_counts.empty() ? 0 : std::numeric_limits<int>::max();
  int max_count = 0;
  for (const auto& entry : replica_counts) {
    EasyJson count_json = counts_json.PushBack(EasyJson::kObject);
    count_json["ts_uuid"] = entry.first;
    count_json["count"] = entry.second;
    min_count = std::min(min_count, entry.second);
    max_count = std::max(max_count, entry.second);
  }
  replica_dist_json["max_count"] = max_count;
  replica_dist_json["min_count"] = min_count;
  replica_dist_json["skew"] = max_count - min_count;

  // Used to make the Impala CREATE TABLE statement.
  (*output)["master_addresses"] = MasterAddrsToCsv();

  std::vector<scoped_refptr<MonitoredTask>> task_list;
  table->GetTaskList(&task_list);
  TaskListToJson(task_list, output);
}

void MasterPathHandlers::HandleMasters(const Webserver::WebRequest& /*req*/,
                                       Webserver::WebResponse* resp) {
  EasyJson* output = resp->output;
  vector<ServerEntryPB> masters;
  Status s = master_->ListMasters(&masters);
  if (!s.ok()) {
    string msg = s.CloneAndPrepend("Unable to list Masters").ToString();
    LOG(WARNING) << msg;
    (*output)["error"] = msg;
    return;
  }
  output->Set("even_masters", masters.size() % 2 == 0);
  output->Set("masters", EasyJson::kArray);
  output->Set("errors", EasyJson::kArray);
  output->Set("has_no_errors", true);
  for (const ServerEntryPB& master : masters) {
    if (master.has_error()) {
      output->Set("has_no_errors", false);
      EasyJson error_json = (*output)["errors"].PushBack(EasyJson::kObject);
      Status error = StatusFromPB(master.error());
      error_json["uuid"] = master.has_instance_id() ?
                           master.instance_id().permanent_uuid() : "Unavailable";
      error_json["error"] = error.ToString();
      continue;
    }
    EasyJson master_json = (*output)["masters"].PushBack(EasyJson::kObject);
    const ServerRegistrationPB& reg = master.registration();
    master_json["uuid"] = master.instance_id().permanent_uuid();
    if (!reg.http_addresses().empty()) {
      master_json["target"] = Substitute("$0://$1:$2/",
                                         reg.https_enabled() ? "https" : "http",
                                         reg.http_addresses(0).host(),
                                         reg.http_addresses(0).port());
    }
    master_json["role"] = master.has_role() ? RaftPeerPB_Role_Name(master.role()) : "N/A";
    master_json["registration"] = pb_util::SecureShortDebugString(master.registration());
  }
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

void MasterPathHandlers::HandleDumpEntities(const Webserver::WebRequest& /*req*/,
                                            Webserver::PrerenderedWebResponse* resp) {
  ostringstream* output = resp->output;
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
  server->RegisterPathHandler(
      "/tablet-servers", "Tablet Servers",
      boost::bind(&MasterPathHandlers::HandleTabletServers, this, _1, _2),
      is_styled, is_on_nav_bar);
  server->RegisterPathHandler(
      "/tables", "Tables",
      boost::bind(&MasterPathHandlers::HandleCatalogManager, this, _1, _2),
      is_styled, is_on_nav_bar);
  server->RegisterPathHandler(
      "/table", "",
      boost::bind(&MasterPathHandlers::HandleTablePage, this, _1, _2),
      is_styled, false);
  server->RegisterPathHandler(
      "/masters", "Masters",
      boost::bind(&MasterPathHandlers::HandleMasters, this, _1, _2),
      is_styled, is_on_nav_bar);
  server->RegisterPrerenderedPathHandler(
      "/dump-entities", "Dump Entities",
      boost::bind(&MasterPathHandlers::HandleDumpEntities, this, _1, _2),
      false, false);
  return Status::OK();
}

pair<string, string> MasterPathHandlers::TSDescToLinkPair(const TSDescriptor& desc,
                                                          const string& tablet_id) const {
  ServerRegistrationPB reg;
  desc.GetRegistration(&reg);
  if (reg.http_addresses().empty()) {
    return std::make_pair(desc.permanent_uuid(), "");
  }
  string text = Substitute("$0:$1", reg.http_addresses(0).host(), reg.http_addresses(0).port());
  string target = Substitute("$0://$1:$2/tablet?id=$3",
                             reg.https_enabled() ? "https" : "http",
                             reg.http_addresses(0).host(),
                             reg.http_addresses(0).port(),
                             tablet_id);
  return std::make_pair(std::move(text), std::move(target));
}

string MasterPathHandlers::MasterAddrsToCsv() const {
  if (master_->opts().IsDistributed()) {
    vector<string> all_addresses;
    all_addresses.reserve(master_->opts().master_addresses.size());
    for (const HostPort& hp : master_->opts().master_addresses) {
      all_addresses.push_back(hp.ToString());
    }
    return JoinElements(all_addresses, ",");
  }
  Sockaddr addr = master_->first_rpc_address();
  HostPort hp;
  Status s = HostPortFromSockaddrReplaceWildcard(addr, &hp);
  if (s.ok()) {
    return hp.ToString();
  }
  LOG(WARNING) << "Unable to determine proper local hostname: " << s.ToString();
  return addr.ToString();
}

Status MasterPathHandlers::GetLeaderMasterHttpAddr(string* leader_http_addr) const {
  vector<ServerEntryPB> masters;
  RETURN_NOT_OK_PREPEND(master_->ListMasters(&masters), "unable to list masters");
  for (const auto& master : masters) {
    if (master.has_error()) {
      continue;
    }
    if (master.role() != RaftPeerPB::LEADER) {
      continue;
    }
    const ServerRegistrationPB& reg = master.registration();
    if (reg.http_addresses().empty()) {
      return Status::NotFound("leader master has no http address");
    }
    *leader_http_addr = Substitute("$0://$1:$2",
                                   reg.https_enabled() ? "https" : "http",
                                   reg.http_addresses(0).host(),
                                   reg.http_addresses(0).port());
    return Status::OK();
  }
  return Status::NotFound("no leader master known to this master");
}

void MasterPathHandlers::SetupLeaderMasterRedirect(const string& path,
                                                   int redirects,
                                                   EasyJson* output) const {
  // Allow 3 redirects.
  const int max_redirects = 3;
  (*output)["error"] = "Master is not the leader.";
  if (redirects >= max_redirects) {
    (*output)["redirect_error"] = "Too many redirects attempting to find the leader master.";
    return;
  }
  string leader_http_addr;
  Status s = GetLeaderMasterHttpAddr(&leader_http_addr);
  if (!s.ok()) {
    (*output)["redirect_error"] = Substitute("Unable to redirect to leader master: $0",
                                             s.ToString());
    return;
  }
  (*output)["leader_redirect"] = Substitute("$0/$1&redirects=$2",
                                            leader_http_addr, path, redirects + 1);
}

} // namespace master
} // namespace kudu
