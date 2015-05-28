// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/tserver/tablet_server_options.h"

#include <glog/logging.h>
#include <gflags/gflags.h>

#include "kudu/gutil/strings/split.h"
#include "kudu/master/master.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/util/env.h"

namespace kudu {
namespace tserver {

DEFINE_string(tablet_server_wal_dir, "/tmp/demo-tablets",
              "Directory where the Tablet Server will place its "
              "write-ahead logs. May be the same as --tablet_server_data_dirs");
DEFINE_string(tablet_server_data_dirs, "/tmp/demo-tablets",
              "Comma-separated list of directories where the Tablet "
              "Server will place its data blocks");

DEFINE_string(tablet_server_rpc_bind_addresses, "0.0.0.0:7050",
             "Comma-separated list of addresses for the Tablet Server"
              " to bind to for RPC connections");
DEFINE_int32(tablet_server_web_port, TabletServer::kDefaultWebPort,
             "Port to bind to for the tablet server web server");
DEFINE_int32(tablet_server_num_acceptors_per_address, 1,
             "Number of RPC acceptor threads for each bound address");
DEFINE_int32(tablet_server_num_service_threads, 20,
             "Number of RPC worker threads to run");
DEFINE_string(tablet_server_master_addrs, "127.0.0.1:7051",
              "Comma separated addresses of the masters which the "
              "tablet server should connect to. The masters do not "
              "read this flag -- configure the masters separately "
              "using 'master_server_rpc_bind_addresses'.");


TabletServerOptions::TabletServerOptions() {
  rpc_opts.rpc_bind_addresses = FLAGS_tablet_server_rpc_bind_addresses;
  rpc_opts.num_acceptors_per_address = FLAGS_tablet_server_num_acceptors_per_address;
  rpc_opts.num_service_threads = FLAGS_tablet_server_num_service_threads;
  rpc_opts.default_port = TabletServer::kDefaultPort;

  webserver_opts.port = FLAGS_tablet_server_web_port;
  // The rest of the web server options are not overridable on a per-tablet-server
  // basis.

  wal_dir = FLAGS_tablet_server_wal_dir;
  data_dirs = strings::Split(FLAGS_tablet_server_data_dirs, ",",
                             strings::SkipEmpty());

  Status s = HostPort::ParseStrings(FLAGS_tablet_server_master_addrs,
                                    master::Master::kDefaultPort,
                                    &master_addresses);
  if (!s.ok()) {
    LOG(FATAL) << "Couldn't parse tablet_server_master_addrs flag: " << s.ToString();
  }

  env = Env::Default();
}

} // namespace tserver
} // namespace kudu
