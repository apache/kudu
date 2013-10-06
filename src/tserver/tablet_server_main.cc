// Copyright (c) 2013, Cloudera, inc.

#include <boost/thread/thread.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>

#include "common/schema.h"
#include "server/fsmanager.h"
#include "server/metadata.h"
#include "server/rpc_server.h"
#include "tablet/tablet.h"
#include "consensus/log.h"
#include "consensus/consensus.h"
#include "consensus/local_consensus.h"
#include "tablet/tablet_peer.h"
#include "tserver/tablet_server.h"
#include "tserver/ts_tablet_manager.h"
#include "twitter-demo/twitter-schema.h"
#include "util/env.h"
#include "util/logging.h"

DEFINE_int32(flush_threshold_mb, 64, "Minimum memrowset size to flush");

using kudu::metadata::TabletMetadata;
using kudu::metadata::TabletServerPB;
using kudu::tablet::Tablet;
using kudu::tablet::TabletPeer;
using kudu::tserver::TabletServer;

namespace kudu {
namespace tserver {

static const char* const kTwitterTabletId = "twitter";

// For the sake of demos, hard-code the twitter demo schema
// here in the tablet server. This will go away as soon as
// we have support for dynamically creating and dropping
// tables.
class TemporaryTabletsForDemos {
 public:
  explicit TemporaryTabletsForDemos(TabletServer* server)
    : twitter_schema_(twitter_demo::CreateTwitterSchema()) {

    shared_ptr<TabletPeer> peer;
    if (server->tablet_manager()->LookupTablet(kTwitterTabletId, &peer)) {
      CHECK(twitter_schema_.Equals(peer->tablet()->schema()))
        << "Bad schema for twitter tablet loaded on disk: " <<
        peer->tablet()->schema().ToString();
      LOG(INFO) << "Using previously-created twitter tablet";
    } else {
      CHECK_OK(server->tablet_manager()->CreateNewTablet(
                 kTwitterTabletId, "", "", twitter_schema_, &peer));
    }
    twitter_tablet_ = peer->shared_tablet();
  }

  const shared_ptr<Tablet>& twitter_tablet() {
    return twitter_tablet_;
  }

 private:
  Schema twitter_schema_;

  shared_ptr<Tablet> twitter_tablet_;

  DISALLOW_COPY_AND_ASSIGN(TemporaryTabletsForDemos);
};

static void FlushThread(Tablet* tablet) {
  while (true) {
    if (tablet->MemRowSetSize() > FLAGS_flush_threshold_mb * 1024 * 1024) {
      CHECK_OK(tablet->Flush());
    } else {
      VLOG(1) << "Not flushing, memrowset not very full";
    }
    usleep(250 * 1000);
  }
}

static void CompactThread(Tablet* tablet) {
  while (true) {
    CHECK_OK(tablet->Compact(Tablet::COMPACT_NO_FLAGS));

    usleep(3000 * 1000);
  }
}

static int TabletServerMain(int argc, char** argv) {
  InitGoogleLoggingSafe(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);
  if (argc != 1) {
    std::cerr << "usage: " << argv[0] << std::endl;
    return 1;
  }

  TabletServerOptions opts;
  TabletServer server(opts);
  LOG(INFO) << "Initializing tablet server...";
  CHECK_OK(server.Init());

  LOG(INFO) << "Setting up demo tablets...";
  TemporaryTabletsForDemos demo_setup(&server);

  // Temporary hack for demos: start threads which compact/flush the tablet.
  // Eventually this will be part of TabletServer itself, and take care of deciding
  // which tablet to perform operations on. But as a stop-gap, just start these
  // simple threads here from main.
  LOG(INFO) << "Starting flush/compact threads";
  boost::thread compact_thread(CompactThread, demo_setup.twitter_tablet().get());
  boost::thread flush_thread(FlushThread, demo_setup.twitter_tablet().get());

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
