// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <glog/logging.h>
#include <iostream>

#include "kudu/common/schema.h"
#include "kudu/server/metadata.h"
#include "kudu/server/rpc_server.h"
#include "kudu/tablet/tablet.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/consensus.h"
#include "kudu/consensus/local_consensus.h"
#include "kudu/tablet/tablet_peer.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/flags.h"
#include "kudu/util/logging.h"
#include "kudu/util/thread.h"

using kudu::tablet::Tablet;
using kudu::tablet::TabletPeer;
using kudu::tserver::TabletServer;

namespace kudu {
namespace tserver {

static int TabletServerMain(int argc, char** argv) {
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
