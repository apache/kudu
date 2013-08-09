// Copyright (c) 2013, Cloudera, inc.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>

#include "master/master_server.h"

DEFINE_string(master_server_rpc_bind_addresses, "0.0.0.0:7150",
             "Comma-separated list of addresses for the Master Server"
              " to bind to for RPC connections");
DEFINE_int32(master_server_num_rpc_reactors, 1,
             "Number of RPC reactor threads to run");
DEFINE_int32(master_server_num_acceptors_per_address, 1,
             "Number of RPC acceptor threads for each bound address");
DEFINE_int32(master_server_num_service_threads, 10,
             "Number of RPC worker threads to run");

using kudu::master::MasterServer;

namespace kudu {
namespace master {

static int MasterServerMain(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);
  if (argc != 1) {
    std::cerr << "usage: " << argv[0] << std::endl;
    return 1;
  }

  MasterServerOptions opts;
  opts.rpc_bind_addresses = FLAGS_master_server_rpc_bind_addresses;
  opts.num_rpc_reactors = FLAGS_master_server_num_rpc_reactors;
  opts.num_acceptors_per_address = FLAGS_master_server_num_acceptors_per_address;
  opts.num_service_threads = FLAGS_master_server_num_service_threads;

  MasterServer server(opts);
  LOG(INFO) << "Initializing master server...";
  CHECK_OK(server.Init());

  LOG(INFO) << "Starting master server...";
  CHECK_OK(server.Start());

  LOG(INFO) << "Master server successfully started.";
  while (true) {
    sleep(60);
  }

  return 0;
}

} // namespace master
} // namespace kudu

int main(int argc, char** argv) {
  return kudu::master::MasterServerMain(argc, argv);
}
