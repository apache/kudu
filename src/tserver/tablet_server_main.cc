// Copyright (c) 2013, Cloudera, inc.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>

#include "tserver/tablet_server.h"

DEFINE_string(tablet_server_rpc_bind_addresses, "0.0.0.0:7150",
             "Comma-separated list of addresses for the Tablet Server"
              " to bind to for RPC connections");
DEFINE_int32(tablet_server_num_rpc_reactors, 1,
             "Number of RPC reactor threads to run");
DEFINE_int32(tablet_server_num_acceptors_per_address, 1,
             "Number of RPC acceptor threads for each bound address");
DEFINE_int32(tablet_server_num_service_threads, 10,
             "Number of RPC worker threads to run");

using kudu::tserver::TabletServer;

namespace kudu {
namespace tserver {

static int TabletServerMain(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);
  if (argc != 1) {
    std::cerr << "usage: " << argv[0] << std::endl;
    return 1;
  }

  TabletServerOptions opts;
  opts.rpc_bind_addresses = FLAGS_tablet_server_rpc_bind_addresses;
  opts.num_rpc_reactors = FLAGS_tablet_server_num_rpc_reactors;
  opts.num_acceptors_per_address = FLAGS_tablet_server_num_acceptors_per_address;
  opts.num_service_threads = FLAGS_tablet_server_num_service_threads;

  TabletServer server(opts);
  LOG(INFO) << "Initializing tablet server...";
  CHECK_OK(server.Init());

  LOG(INFO) << "Starting tablet server...";
  CHECK_OK(server.Start());

  LOG(INFO) << "Tablet server successfully started.";
  while (true) {
    sleep(60);
  }

  return 0;
}

} // namespace tserver
} // namespace kudu

int main(int argc, char** argv) {
  return kudu::tserver::TabletServerMain(argc, argv);
}
