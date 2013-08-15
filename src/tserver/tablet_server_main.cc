// Copyright (c) 2013, Cloudera, inc.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>

#include "common/schema.h"
#include "server/fsmanager.h"
#include "server/metadata.h"
#include "server/rpc_server.h"
#include "tablet/tablet.h"
#include "tserver/tablet_server.h"
#include "twitter-demo/twitter-schema.h"
#include "util/env.h"

DEFINE_string(tablet_server_base_dir, "/tmp/demo-tablets",
              "Base directory for single-tablet demo server");

DEFINE_string(tablet_server_rpc_bind_addresses, "0.0.0.0:7150",
             "Comma-separated list of addresses for the Tablet Server"
              " to bind to for RPC connections");
DEFINE_int32(tablet_server_num_rpc_reactors, 1,
             "Number of RPC reactor threads to run");
DEFINE_int32(tablet_server_num_acceptors_per_address, 1,
             "Number of RPC acceptor threads for each bound address");
DEFINE_int32(tablet_server_num_service_threads, 10,
             "Number of RPC worker threads to run");

DEFINE_int32(flush_threshold_mb, 64, "Minimum memrowset size to flush");

using kudu::metadata::TabletMetadata;
using kudu::tablet::Tablet;
using kudu::tserver::TabletServer;

namespace kudu {
namespace tserver {

// For the sake of demos, hard-code the twitter demo schema
// here in the tablet server. This will go away as soon as
// we have support for dynamically creating and dropping
// tables.
class TemporaryTabletsForDemos {
 public:
  TemporaryTabletsForDemos()
    : env_(Env::Default()),
      fs_manager_(env_, FLAGS_tablet_server_base_dir),
      twitter_schema_(twitter_demo::CreateTwitterSchema()) {
    CHECK_OK(fs_manager_.CreateInitialFileSystemLayout());

    metadata::TabletMasterBlockPB master_block;
    master_block.set_tablet_id("twitter");
    master_block.set_block_a("00000000000000000000000000000000");
    master_block.set_block_b("11111111111111111111111111111111");
    gscoped_ptr<TabletMetadata> meta(
      new TabletMetadata(&fs_manager_, master_block));
    twitter_tablet_.reset(new Tablet(meta.Pass(), twitter_schema_));

    Status s = twitter_tablet_->Open();
    if (!s.ok() && s.IsNotFound()) {
      LOG(INFO) << "Creating new Twitter tablet";
      s = twitter_tablet_->CreateNew();
    }
    CHECK_OK(s);
  }

  const shared_ptr<Tablet>& twitter_tablet() {
    return twitter_tablet_;
  }

 private:
  Env* env_;
  FsManager fs_manager_;
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
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);
  if (argc != 1) {
    std::cerr << "usage: " << argv[0] << std::endl;
    return 1;
  }

  RpcServerOptions opts;
  opts.rpc_bind_addresses = FLAGS_tablet_server_rpc_bind_addresses;
  opts.num_rpc_reactors = FLAGS_tablet_server_num_rpc_reactors;
  opts.num_acceptors_per_address = FLAGS_tablet_server_num_acceptors_per_address;
  opts.num_service_threads = FLAGS_tablet_server_num_service_threads;

  TabletServer server(opts);
  LOG(INFO) << "Initializing tablet server...";
  CHECK_OK(server.Init());

  LOG(INFO) << "Setting up demo tablets...";
  TemporaryTabletsForDemos demo_setup;
  server.RegisterTablet(demo_setup.twitter_tablet());

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
