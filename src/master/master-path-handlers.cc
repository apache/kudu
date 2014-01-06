// Copyright (c) 2013, Cloudera, inc.

#include "master/master-path-handlers.h"

#include <boost/bind.hpp>
#include <boost/foreach.hpp>
#include <string>
#include <vector>

#include "gutil/stringprintf.h"
#include "gutil/strings/substitute.h"
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

Status MasterPathHandlers::Register(Webserver* server) {
  bool is_styled = true;
  bool is_on_nav_bar = true;
  server->RegisterPathHandler("/tablet-servers",
                              boost::bind(&MasterPathHandlers::HandleTabletServers, this, _1, _2),
                              is_styled, is_on_nav_bar);
  return Status::OK();
}

} // namespace master
} // namespace kudu
