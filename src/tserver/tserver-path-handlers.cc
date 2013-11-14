// Copyright (c) 2013, Cloudera, inc.

#include "tserver/tserver-path-handlers.h"

#include <string>
#include <tr1/memory>
#include <vector>

#include "gutil/map-util.h"
#include "gutil/strings/human_readable.h"
#include "gutil/strings/substitute.h"
#include "tablet/tablet_peer.h"
#include "tserver/tablet_server.h"
#include "tserver/ts_tablet_manager.h"
#include "util/url-coding.h"

using kudu::tablet::TabletPeer;
using std::tr1::shared_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tserver {

TabletServerPathHandlers::~TabletServerPathHandlers() {
}

Status TabletServerPathHandlers::Register(Webserver* server) {
  server->RegisterPathHandler(
    "/tablets",
    boost::bind(&TabletServerPathHandlers::HandleTabletsPage, this, _1, _2),
    true /* styled */, true /* is_on_nav_bar */);
  server->RegisterPathHandler(
    "/tablet",
    boost::bind(&TabletServerPathHandlers::HandleTabletPage, this, _1, _2),
    true /* styled */, false /* is_on_nav_bar */);
  return Status::OK();
}

void TabletServerPathHandlers::HandleTabletsPage(const Webserver::ArgumentMap &args,
                                                 std::stringstream *output) {
  vector<shared_ptr<TabletPeer> > peers;
  tserver_->tablet_manager()->GetTabletPeers(&peers);

  *output << "<h1>Tablets</h1>\n";
  *output << "<table class='table table-striped'>\n";
  *output << "  <tr><th>Tablet ID</th><th>On-disk Size</th></tr>\n";
  BOOST_FOREACH(const shared_ptr<TabletPeer>& peer, peers) {
    string id = peer->tablet()->tablet_id();
    string tablet_link = Substitute("<a href=\"/tablet?id=$0\">$1</a>",
                                    UrlEncodeToString(id),
                                    EscapeForHtmlToString(id));
    string n_bytes = HumanReadableNumBytes::ToString(peer->tablet()->EstimateOnDiskSize());
    // TODO: would be nice to include some other stuff like memory usage, table
    // name, key range, etc.
    (*output) << Substitute("<tr><th>$0</th><th>$1</th></tr>\n",
                            tablet_link, n_bytes);
  }
  *output << "</table>\n";
}

void TabletServerPathHandlers::OutputSchemaTable(const Schema& schema,
                                                 std::stringstream* output) {
  *output << "<table class='table table-striped'>\n";
  *output << "  <tr>"
          << "<th>Column</th><th>ID</th><th>Type</th>"
          << "<th>Read default</th><th>Write default</th>"
          << "</tr>\n";

  for (int i = 0; i < schema.num_columns(); i++) {
    const ColumnSchema& col = schema.column(i);
    string read_default = "-";
    if (col.has_read_default()) {
      read_default = col.Stringify(col.read_default_value());
    }
    string write_default = "-";
    if (col.has_write_default()) {
      write_default = col.Stringify(col.write_default_value());
    }
    *output << Substitute("<tr><th>$0</th><td>$1</td><td>$2</td><td>$3</td><td>$4</td></tr>\n",
                          EscapeForHtmlToString(col.name()),
                          schema.column_id(i),
                          col.TypeToString(),
                          EscapeForHtmlToString(read_default),
                          EscapeForHtmlToString(write_default));
  }
  *output << "</table>\n";
}

void TabletServerPathHandlers::OutputImpalaSchema(const std::string& table_name,
                                                  const Schema& schema,
                                                  std::stringstream* output) {
  *output << "<code><pre>\n";

  *output << "CREATE TABLE " << EscapeForHtmlToString(table_name) << " (\n";

  for (int i = 0; i < schema.num_columns(); i++) {
    const ColumnSchema& col = schema.column(i);

    *output << EscapeForHtmlToString(col.name()) << " ";
    switch (col.type_info().type()) {
      case STRING:
        *output << "STRING";
        break;
      case UINT8:
      case INT8:
        *output << "TINYINT";
        break;
      case UINT16:
      case INT16:
        *output << "SMALLINT";
        break;
      case UINT32:
      case INT32:
        *output << "INT";
        break;
      case UINT64:
      case INT64:
        *output << "BIGINT";
        break;
      default:
        *output << "[unsupported type " << col.type_info().name() << "!]";
        break;
    }
    if (i < schema.num_columns() - 1) {
      *output << ",";
    }
    *output << "\n";
  }
  *output << ");\n";;
  *output << "</pre></code>\n";
}

void TabletServerPathHandlers::HandleTabletPage(const Webserver::ArgumentMap &args,
                                                std::stringstream *output) {
  // Parse argument.
  string tablet_id;
  if (!FindCopy(args, "id", &tablet_id)) {
    // TODO: webserver should give a way to return a non-200 response code
    (*output) << "Missing 'tablet' argument";
    return;
  }

  // Look up tablet.
  shared_ptr<TabletPeer> peer;
  if (!tserver_->tablet_manager()->LookupTablet(tablet_id, &peer)) {
    (*output) << "Tablet not found";
    return;
  }

  *output << "<h1>Tablet " << EscapeForHtmlToString(tablet_id) << "</h1>\n";

  // Output schema in tabular format.
  *output << "<h2>Schema</h2>\n";
  Schema schema = peer->tablet()->schema();
  OutputSchemaTable(schema, output);

  *output << "<h2>Impala CREATE TABLE statement</h2>\n";
  OutputImpalaSchema(tablet_id, schema, output);
}

} // namespace tserver
} // namespace kudu
