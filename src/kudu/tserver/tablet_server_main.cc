// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <glog/logging.h>
#include <iostream>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/util/flags.h"
#include "kudu/util/logging.h"

using kudu::tserver::TabletServer;

DECLARE_string(rpc_bind_addresses);
DECLARE_int32(rpc_num_service_threads);
DECLARE_int32(webserver_port);

namespace kudu {
namespace tserver {

static int TabletServerMain(int argc, char** argv) {
  // Reset some default values before parsing gflags.
  FLAGS_rpc_bind_addresses = strings::Substitute("0.0.0.0:$0",
                                                 TabletServer::kDefaultPort);
  FLAGS_rpc_num_service_threads = 20;
  FLAGS_webserver_port = TabletServer::kDefaultWebPort;

  ParseCommandLineFlags(&argc, &argv, true);
  if (argc != 1) {
    std::cerr << "usage: " << argv[0] << std::endl;
    return 1;
  }
  InitGoogleLoggingSafe(argv[0]);

  TabletServerOptions opts;
  TabletServer server(opts);
  LOG(INFO) << "Initializing tablet server...";
  CHECK_OK(server.Init());

  LOG(INFO) << "Starting tablet server...";
  CHECK_OK(server.Start());

  LOG(INFO) << "Tablet server successfully started.";
  while (true) {
    SleepFor(MonoDelta::FromSeconds(60));
  }

  return 0;
}

} // namespace tserver
} // namespace kudu

int main(int argc, char** argv) {
  return kudu::tserver::TabletServerMain(argc, argv);
}
