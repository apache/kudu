// Copyright (c) 2013, Cloudera, inc.

#include "master/master_server_options.h"

#include <gflags/gflags.h>

#include "master/master_server.h"
#include "util/env.h"

namespace kudu {
namespace master {

DEFINE_string(master_base_dir, "/tmp/demo-tablets",
              "Base directory for single-tablet demo server");

DEFINE_string(master_rpc_bind_addresses, "0.0.0.0:7150",
             "Comma-separated list of addresses for the Tablet Server"
              " to bind to for RPC connections");
DEFINE_int32(master_web_port, MasterServer::kDefaultWebPort,
             "Port to bind to for the Master web server");
DEFINE_int32(master_num_acceptors_per_address, 1,
             "Number of RPC acceptor threads for each bound address");
DEFINE_int32(master_num_service_threads, 10,
             "Number of RPC worker threads to run");


MasterServerOptions::MasterServerOptions() {
  rpc_opts.rpc_bind_addresses = FLAGS_master_rpc_bind_addresses;
  rpc_opts.num_acceptors_per_address = FLAGS_master_num_acceptors_per_address;
  rpc_opts.num_service_threads = FLAGS_master_num_service_threads;
  rpc_opts.default_port = MasterServer::kDefaultPort;

  webserver_opts.port = FLAGS_master_web_port;
  // The rest of the web server options are not overridable on a per-tablet-server
  // basis.

  base_dir = FLAGS_master_base_dir;

  env = Env::Default();
}

} // namespace master
} // namespace kudu
