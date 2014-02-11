// Copyright (c) 2013, Cloudera, inc.

#include "tserver/tserver-path-handlers.h"

#include <string>
#include <tr1/memory>
#include <vector>

#include "gutil/map-util.h"
#include "gutil/strings/human_readable.h"
#include "gutil/strings/substitute.h"
#include "server/webui_util.h"
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
  *output << "  <tr><th>Table Name</th><th>Tablet ID</th><th>On-disk Size</th></tr>\n";
  BOOST_FOREACH(const shared_ptr<TabletPeer>& peer, peers) {
    string id = peer->tablet()->tablet_id();
    string table_name = peer->tablet()->metadata()->table_name();
    string tablet_link = Substitute("<a href=\"/tablet?id=$0\">$1</a>",
                                    UrlEncodeToString(id),
                                    EscapeForHtmlToString(id));
    string n_bytes = HumanReadableNumBytes::ToString(peer->tablet()->EstimateOnDiskSize());
    // TODO: would be nice to include some other stuff like memory usage, table
    // name, key range, etc.
    (*output) << Substitute("<tr><th>$0</th><th>$1</th><th>$2</th></tr>\n",
                            EscapeForHtmlToString(table_name), tablet_link, n_bytes);
  }
  *output << "</table>\n";
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

  string table_name = peer->tablet()->metadata()->table_name();

  *output << "<h1>Tablet " << EscapeForHtmlToString(tablet_id) << "</h1>\n";

  // Output schema in tabular format.
  *output << "<h2>Schema</h2>\n";
  Schema schema = peer->tablet()->schema();
  HtmlOutputSchemaTable(schema, output);

  *output << "<h2>Impala CREATE TABLE statement</h2>\n";
  HtmlOutputImpalaSchema(table_name, schema, output);
}

} // namespace tserver
} // namespace kudu
