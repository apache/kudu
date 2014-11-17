// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/master/master-path-handlers.h"

#include <algorithm>
#include <boost/bind.hpp>
#include <boost/foreach.hpp>
#include <string>
#include <vector>

#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/server/webui_util.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/master/ts_manager.h"
#include "kudu/util/url-coding.h"


namespace kudu {

using std::vector;
using std::string;
using strings::Substitute;
using metadata::QuorumPeerPB;

namespace master {

const int kSysTablesEntryStatePrefixLen = 11;  // kTableState
const int kSysTabletsEntryStatePrefixLen = 12; // kTabletState

MasterPathHandlers::~MasterPathHandlers() {
}

void MasterPathHandlers::HandleTabletServers(const Webserver::WebRequest& req,
                                             std::stringstream* output) {
  vector<std::tr1::shared_ptr<TSDescriptor> > descs;
  master_->ts_manager()->GetAllDescriptors(&descs);

  *output << "<h1>Tablet Servers</h1>\n";

  *output << "<table class='table table-striped'>\n";
  *output << "  <tr><th>UUID</th><th>Time since heartbeat</th><th>Registration</th></tr>\n";
  BOOST_FOREACH(const std::tr1::shared_ptr<TSDescriptor>& desc, descs) {
    const string time_since_hb = StringPrintf("%.1fs", desc->TimeSinceHeartbeat().ToSeconds());
    TSRegistrationPB reg;
    desc->GetRegistration(&reg);
    *output << Substitute("<tr><th>$0</th><td>$1</td><td><code>$2</code></td></tr>\n",
                          RegistrationToHtml(reg, desc->permanent_uuid()),
                          time_since_hb,
                          EscapeForHtmlToString(reg.ShortDebugString()));
  }
  *output << "</table>\n";
}

void MasterPathHandlers::HandleCatalogManager(const Webserver::WebRequest& req,
                                              std::stringstream* output) {
  *output << "<h1>Tables</h1>\n";

  std::vector<scoped_refptr<TableInfo> > tables;
  master_->catalog_manager()->GetAllTables(&tables);

  *output << "<table class='table table-striped'>\n";
  *output << "  <tr><th>Table Name</th><th>Table Id</th><th>State</th></tr>\n";
  BOOST_FOREACH(const scoped_refptr<TableInfo>& table, tables) {
    TableMetadataLock l(table.get(), TableMetadataLock::READ);
    *output << Substitute(
        "<tr><th>$0</th><td><a href=\"/table?id=$1\">$1</a></td><td>$2 $3</td></tr>\n",
        EscapeForHtmlToString(l.data().name()),
        EscapeForHtmlToString(table->id()),
        SysTablesEntryPB_State_Name(l.data().pb.state()).substr(kSysTablesEntryStatePrefixLen),
        EscapeForHtmlToString(l.data().pb.state_msg()));
  }
  *output << "</table>\n";
}

void MasterPathHandlers::HandleTablePage(const Webserver::WebRequest& req,
                                         std::stringstream *output) {
  // Parse argument.
  string table_id;
  if (!FindCopy(req.parsed_args, "id", &table_id)) {
    // TODO: webserver should give a way to return a non-200 response code
    *output << "Missing 'id' argument";
    return;
  }

  scoped_refptr<TableInfo> table;
  if (!master_->catalog_manager()->GetTableInfo(table_id, &table)) {
    *output << "Table not found";
    return;
  }

  Schema schema;
  string table_name;
  vector<scoped_refptr<TabletInfo> > tablets;
  {
    TableMetadataLock l(table.get(), TableMetadataLock::READ);
    table_name = l.data().name();
    *output << "<h1>Table: " << EscapeForHtmlToString(table_name)
            << " (" << EscapeForHtmlToString(table_id) << ")</h1>\n";

    *output << "<table class='table table-striped'>\n";
    *output << "  <tr><td>Version:</td><td>" << l.data().pb.version() << "</td></tr>\n";
    *output << "  <tr><td>State:</td><td>"
          << SysTablesEntryPB_State_Name(l.data().pb.state()).substr(kSysTablesEntryStatePrefixLen)
          << EscapeForHtmlToString(l.data().pb.state_msg())
          << "</td></tr>\n";
    *output << "</table>\n";

    SchemaFromPB(l.data().pb.schema(), &schema);
    table->GetAllTablets(&tablets);
  }

  HtmlOutputSchemaTable(schema, output);

  *output << "<table class='table table-striped'>\n";
  *output << "  <tr><th>Tablet ID</th><th>Start-Key</th><th>End-Key</th><th>State</th>"
      "<th>Quorum</th></tr>\n";
  BOOST_FOREACH(const scoped_refptr<TabletInfo>& tablet, tablets) {
    vector<TabletReplica> locations;
    tablet->GetLocations(&locations);
    TabletMetadataLock l(tablet.get(), TabletMetadataLock::READ);
    *output << Substitute(
        "<tr><th>$0</th><td>$1</td><td>$2</td><td>$3 $4</td><td>$5</td></tr>\n",
        tablet->tablet_id(),
        EscapeForHtmlToString(schema.DebugEncodedRowKey(l.data().pb.start_key())),
        EscapeForHtmlToString(schema.DebugEncodedRowKey(l.data().pb.end_key())),
        SysTabletsEntryPB_State_Name(l.data().pb.state()).substr(kSysTabletsEntryStatePrefixLen),
        EscapeForHtmlToString(l.data().pb.state_msg()),
        QuorumToHtml(locations));
  }
  *output << "</table>\n";

  *output << "<h2>Impala CREATE TABLE statement</h2>\n";
  const string master_addr = master_->first_rpc_address().ToString();
  HtmlOutputImpalaSchema(table_name, schema, master_addr, output);

  std::vector<scoped_refptr<MonitoredTask> > task_list;
  table->GetTaskList(&task_list);
  HtmlOutputTaskList(task_list, output);
}

void MasterPathHandlers::HandleMasters(const Webserver::WebRequest& req,
                                       std::stringstream* output) {
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
  *output <<  "  <tr><th>Registration</th><th>Role</th></tr>\n";

  BOOST_FOREACH(const ServerEntryPB& master, masters) {
    if (master.has_error()) {
      Status error = StatusFromPB(master.error());
      *output << Substitute("  <tr><td colspan=2><font color='red'><b>$0</b></font></td></tr>\n",
                            EscapeForHtmlToString(error.ToString()));
      continue;
    }
    string reg_text = RegistrationToHtml(master.registration(),
                                         master.instance_id().permanent_uuid());
    if (master.instance_id().permanent_uuid() == master_->instance_pb().permanent_uuid()) {
      reg_text = Substitute("<b>$0</b>", reg_text);
    }
    *output << Substitute("  <tr><td>$0</td><td>$1</td></tr>\n", reg_text,
                          master.has_role() ?  QuorumPeerPB_Role_Name(master.role()) : "N/A");
  }

  *output << "</table>";
}

Status MasterPathHandlers::Register(Webserver* server) {
  bool is_styled = true;
  bool is_on_nav_bar = true;
  server->RegisterPathHandler("/tablet-servers",
                              boost::bind(&MasterPathHandlers::HandleTabletServers, this, _1, _2),
                              is_styled, is_on_nav_bar);
  server->RegisterPathHandler("/tablez",
                              boost::bind(&MasterPathHandlers::HandleCatalogManager, this, _1, _2),
                              is_styled, is_on_nav_bar);
  server->RegisterPathHandler("/table",
                              boost::bind(&MasterPathHandlers::HandleTablePage, this, _1, _2),
                              is_styled, false);
  server->RegisterPathHandler("/masterz",
                              boost::bind(&MasterPathHandlers::HandleMasters, this, _1, _2),
                              is_styled, is_on_nav_bar);
  return Status::OK();
}

namespace {

bool CompareByRole(const TabletReplica& a, const TabletReplica& b) {
  return a.role < b.role;
}

} // anonymous namespace

string MasterPathHandlers::QuorumToHtml(const std::vector<TabletReplica>& locations) const {
  std::stringstream html;
  vector<TabletReplica> sorted_locations;
  sorted_locations.assign(locations.begin(), locations.end());

  std::sort(sorted_locations.begin(), sorted_locations.end(), &CompareByRole);

  html << "<ul>\n";
  BOOST_FOREACH(const TabletReplica& location, sorted_locations) {
    string location_html = TSDescriptorToHtml(*location.ts_desc);
    if (location.role == QuorumPeerPB::LEADER) {
      html << Substitute("  <li><b>LEADER: $0</b></li>\n", location_html);
    } else {
      html << Substitute("  <li>$0: $1</li>\n",
                         QuorumPeerPB_Role_Name(location.role), location_html);
    }
  }
  html << "</ul>\n";
  return html.str();
}

string MasterPathHandlers::TSDescriptorToHtml(const TSDescriptor& desc) const {
  TSRegistrationPB reg;
  desc.GetRegistration(&reg);

  string link_text = desc.permanent_uuid();
  if (reg.rpc_addresses().size() > 0) {
    link_text = reg.rpc_addresses(0).host();
  }
  return RegistrationToHtml(reg, link_text);
}

template<class RegistrationType>
string MasterPathHandlers::RegistrationToHtml(const RegistrationType& reg,
                                              const std::string& link_text) const {
  string link_html = EscapeForHtmlToString(link_text);
  if (reg.http_addresses().size() > 0) {
    link_html = Substitute("<a href=\"http://$0:$1/\">$2</a>",
                           reg.http_addresses(0).host(),
                           reg.http_addresses(0).port(), link_html);
  }
  return link_html;
}

} // namespace master
} // namespace kudu
