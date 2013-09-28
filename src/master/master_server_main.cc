// Copyright (c) 2013, Cloudera, inc.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>

#include "master/master_server.h"
#include "server/rpc_server.h"
#include "util/logging.h"

using kudu::master::MasterServer;

namespace kudu {
namespace master {

static int MasterServerMain(int argc, char** argv) {
  InitGoogleLoggingSafe(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);
  if (argc != 1) {
    std::cerr << "usage: " << argv[0] << std::endl;
    return 1;
  }

  MasterServerOptions opts;
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
