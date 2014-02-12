// Copyright (c) 2013, Cloudera, inc.

#include "master/master-path-handlers.h"

#include <boost/bind.hpp>
#include <boost/foreach.hpp>
#include <string>
#include <vector>

#include "common/schema.h"
#include "common/wire_protocol.h"
#include "gutil/map-util.h"
#include "gutil/stringprintf.h"
#include "gutil/strings/substitute.h"
#include "server/webui_util.h"
#include "master/catalog_manager.h"
#include "master/master.h"
#include "master/master.pb.h"
#include "master/ts_descriptor.h"
#include "master/ts_manager.h"
#include "util/url-coding.h"

using std::vector;
using std::string;
using strings::Substitute;

namespace kudu {
namespace master {

const int kSysTablesEntryStatePrefixLen = 11;  // kTableState
const int kSysTabletsEntryStatePrefixLen = 12; // kTabletState

MasterPathHandlers::~MasterPathHandlers() {
}

void MasterPathHandlers::HandleTabletServers(const Webserver::ArgumentMap& args,
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

    string uuid_cell = EscapeForHtmlToString(desc->permanent_uuid());
    if (reg.http_addresses().size() > 0) {
      uuid_cell = Substitute("<a href=\"http://$0:$1/\">$2</a>",
                           reg.http_addresses(0).host(),
                           reg.http_addresses(0).port(),
                           uuid_cell);
    }

    *output << Substitute("<tr><th>$0</th><td>$1</td><td><code>$2</code></td></tr>\n",
                          uuid_cell, time_since_hb,
                          EscapeForHtmlToString(reg.ShortDebugString()));
  }
  *output << "</table>\n";
}

void MasterPathHandlers::HandleCatalogManager(const Webserver::ArgumentMap& args,
                                              std::stringstream* output) {
  *output << "<h1>Catalog Manager</h1>\n";

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

void MasterPathHandlers::HandleTablePage(const Webserver::ArgumentMap &args,
                                         std::stringstream *output) {
  // Parse argument.
  string table_id;
  if (!FindCopy(args, "id", &table_id)) {
    // TODO: webserver should give a way to return a non-200 response code
    *output << "Missing 'table' argument";
    return;
  }

  scoped_refptr<TableInfo> table;
  if (!master_->catalog_manager()->GetTableInfo(table_id, &table)) {
    *output << "Table not found";
    return;
  }

  Schema schema;
  vector<scoped_refptr<TabletInfo> > tablets;
  {
    TableMetadataLock l(table.get(), TableMetadataLock::READ);
    *output << "<h1>Table: " << EscapeForHtmlToString(l.data().name())
            << " (" << EscapeForHtmlToString(table_id) << ")</h1>\n";

    *output << "<table class='table table-striped'>\n";
    *output << "  <tr><td>Version:</td><td>" << l.data().pb.version() << "</td></tr>\n";
    *output << "  <tr><td>State:</td><td>"
          << SysTablesEntryPB_State_Name(l.data().pb.state()).substr(kSysTabletsEntryStatePrefixLen)
          << EscapeForHtmlToString(l.data().pb.state_msg())
          << "</td></tr>\n";
    *output << "</table>\n";

    SchemaFromPB(l.data().pb.schema(), &schema);
    table->GetAllTablets(&tablets);
  }

  HtmlOutputSchemaTable(schema, output);

  *output << "<table class='table table-striped'>\n";
  *output << "  <tr><th>Tablet ID</th><th>Start-Key</th><th>End-Key</th><th>State</th></tr>\n";
  BOOST_FOREACH(const scoped_refptr<TabletInfo>& tablet, tablets) {
    TabletMetadataLock l(tablet.get(), TabletMetadataLock::READ);
    *output << Substitute(
        "<tr><th>$0</th><td>$1</td><td>$2</td><td>$3 $4</td></tr>\n",
        tablet->tablet_id(),
        EscapeForHtmlToString(l.data().pb.start_key()),
        EscapeForHtmlToString(l.data().pb.end_key()),
        SysTabletsEntryPB_State_Name(l.data().pb.state()).substr(12),
        EscapeForHtmlToString(l.data().pb.state_msg()));
  }
  *output << "</table>\n";

  std::vector<scoped_refptr<MonitoredTask> > task_list;
  table->GetTaskList(&task_list);
  HtmlOutputTaskList(task_list, output);
}

Status MasterPathHandlers::Register(Webserver* server) {
  bool is_styled = true;
  bool is_on_nav_bar = true;
  server->RegisterPathHandler("/tablet-servers",
                              boost::bind(&MasterPathHandlers::HandleTabletServers, this, _1, _2),
                              is_styled, is_on_nav_bar);
  server->RegisterPathHandler("/catalog-manager",
                              boost::bind(&MasterPathHandlers::HandleCatalogManager, this, _1, _2),
                              is_styled, is_on_nav_bar);
  server->RegisterPathHandler("/table",
                              boost::bind(&MasterPathHandlers::HandleTablePage, this, _1, _2),
                              is_styled, is_on_nav_bar);
  return Status::OK();
}

} // namespace master
} // namespace kudu
