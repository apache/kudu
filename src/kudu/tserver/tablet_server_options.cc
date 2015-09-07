// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/tserver/tablet_server_options.h"

#include <glog/logging.h>
#include <gflags/gflags.h>

#include "kudu/master/master.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/util/flag_tags.h"

namespace kudu {
namespace tserver {

DEFINE_string(tserver_master_addrs, "127.0.0.1:7051",
              "Comma separated addresses of the masters which the "
              "tablet server should connect to. The masters do not "
              "read this flag -- configure the masters separately "
              "using 'rpc_bind_addresses'.");
TAG_FLAG(tserver_master_addrs, stable);


TabletServerOptions::TabletServerOptions() {
  rpc_opts.default_port = TabletServer::kDefaultPort;

  Status s = HostPort::ParseStrings(FLAGS_tserver_master_addrs,
                                    master::Master::kDefaultPort,
                                    &master_addresses);
  if (!s.ok()) {
    LOG(FATAL) << "Couldn't parse tablet_server_master_addrs flag: " << s.ToString();
  }
}

} // namespace tserver
} // namespace kudu
