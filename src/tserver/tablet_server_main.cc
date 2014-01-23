// Copyright (c) 2013, Cloudera, inc.

#include <boost/thread/thread.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>

#include "common/schema.h"
#include "gutil/strings/split.h"
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
#include "benchmarks/ycsb-schema.h"
#include "util/env.h"
#include "util/logging.h"


using kudu::metadata::QuorumPB;
using kudu::metadata::QuorumPeerPB;
using kudu::metadata::TabletMetadata;
using kudu::tablet::Tablet;
using kudu::tablet::TabletPeer;
using kudu::tserver::TabletServer;

static const char* const kYCSBTabletId = "ycsb";
static const char* const kQuorumFlagFormat =
  "Malformed \"tablet_quorum_for_demo\" flag. "
  "Format: name:host0:port0,name:host1:port1,name:host2:port2\n"
  "Note the first host is assumed to be the LEADER."
  "Number of hosts may be 2 or 3.";

DEFINE_int32(flush_threshold_mb, 64, "Minimum memrowset size to flush");
DEFINE_string(tablet_server_tablet_id, kYCSBTabletId,
              "Which tablet to use: ycsb (default)");

DEFINE_string(tablet_quorum_for_demo, "",
              "The locations of other tablet servers in the quorum, for demo purposes.\n"
              "Format: name:host0:port0,name:host1:port1,name:host2:port2\n"
              "Note the first host is assumed to be the LEADER and 'name' should"
              " correspond to the overriden uuid in each server (set with '')");

namespace kudu {
namespace tserver {

// For demos, keep only a single tablet that can be ycsb
class TemporaryTabletsForDemos {
 public:
  explicit TemporaryTabletsForDemos(TabletServer* server, Schema schema,
                                    const string& tablet_id)
    : schema_(schema) {

    shared_ptr<TabletPeer> peer;
    if (server->tablet_manager()->LookupTablet(tablet_id, &peer)) {
      CHECK(schema_.Equals(peer->tablet()->schema()))
        << "Bad schema loaded on disk: " <<
        peer->tablet()->schema().ToString();
      LOG(INFO) << "Using previously-created tablet";
    } else {

      BuildQuorumFromFlagIfProvided();

      CHECK_OK(server->tablet_manager()->CreateNewTablet(
                 tablet_id, "", "", SchemaBuilder(schema_).Build(), quorum_, &peer));
    }
    tablet_ = peer->shared_tablet();
  }

  const shared_ptr<Tablet>& tablet() {
    return tablet_;
  }

 private:

  void BuildQuorumFromFlagIfProvided() {
    // If the 'tablet_quorum_for_demo' flag was specified, build the quorum
    if (FLAGS_tablet_quorum_for_demo != "") {

      vector<string> hostport_pairs;
      SplitStringUsing(FLAGS_tablet_quorum_for_demo, ",", &hostport_pairs);
      CHECK(!hostport_pairs.empty() && hostport_pairs.size() >= 2 && hostport_pairs.size() <= 3)
      << kQuorumFlagFormat << "\nwas: " << FLAGS_tablet_quorum_for_demo;

      int count = 0;
      BOOST_FOREACH(const string& hostport_triple, hostport_pairs) {
        vector<string> name_host_and_port;
        SplitStringUsing(hostport_triple, ":", &name_host_and_port);
        CHECK(name_host_and_port.size() == 3) << kQuorumFlagFormat;
        QuorumPeerPB* peer = quorum_.add_peers();
        peer->set_permanent_uuid(name_host_and_port[0]);
        HostPortPB* hostport = peer->mutable_last_known_addr();
        hostport->set_host(name_host_and_port[1]);
        hostport->set_port(atoi(name_host_and_port[2].c_str()));
        peer->set_role(count++ == 0 ? QuorumPeerPB::LEADER : QuorumPeerPB::FOLLOWER);
      }
      quorum_.set_local(false);
    }
  }

  Schema schema_;
  QuorumPB quorum_;

  shared_ptr<Tablet> tablet_;

  DISALLOW_COPY_AND_ASSIGN(TemporaryTabletsForDemos);
};

// Pick the next tablet to perform some maintenance on. This simply round-robins
// through all the tablets in the server. It may return NULL if there are no
// tablets.
static shared_ptr<Tablet> PickATablet(const TSTabletManager* tablet_mgr, int iteration) {
  vector<shared_ptr<TabletPeer> > peers;
  tablet_mgr->GetTabletPeers(&peers);

  if (peers.empty()) {
    return shared_ptr<Tablet>();
  } else {
    return peers[iteration % peers.size()]->shared_tablet();
  }
}

static void FlushThread(const TSTabletManager* tablet_mgr) {
  int iter = 0;
  while (true) {
    shared_ptr<Tablet> tablet = PickATablet(tablet_mgr, iter++);
    if (!tablet) {
      VLOG(1) << "Not flushing: no tablets to flush";
    } else if (tablet->MemRowSetSize() > FLAGS_flush_threshold_mb * 1024 * 1024) {
      CHECK_OK(tablet->Flush());
    } else {
      VLOG(1) << "Not flushing " << tablet->tablet_id() << ": memrowset not very full";
    }
    usleep(250 * 1000);
  }
}

static void FlushDeltaMemStoresThread(const TSTabletManager* tablet_mgr) {
  int iter = 0;
  while (true) {
    shared_ptr<Tablet> tablet = PickATablet(tablet_mgr, iter++);
    if (!tablet) {
      VLOG(1) << "Not flushing deltas: no tablets";
    } else if (tablet->DeltaMemStoresSize() > FLAGS_flush_threshold_mb * 1024 * 1024) {
      CHECK_OK(tablet->FlushBiggestDMS());
    } else {
      VLOG(1) << "Not flushing deltas for " << tablet->tablet_id()
              << ": DeltaMemStores not very full";
    }
    usleep(250 * 1000);
  }
}

static void CompactThread(const TSTabletManager* tablet_mgr) {
  int iter = 0;
  while (true) {
    shared_ptr<Tablet> tablet = PickATablet(tablet_mgr, iter++);
    if (tablet) {
      CHECK_OK(tablet->Compact(Tablet::COMPACT_NO_FLAGS));
    }

    usleep(3000 * 1000);
  }
}

static void CompactDeltasThread(const TSTabletManager* tablet_mgr) {
  int iter = 0;
  while (true) {
    shared_ptr<Tablet> tablet = PickATablet(tablet_mgr, iter++);
    if (tablet) {
      CHECK_OK(tablet->MinorCompactWorstDeltas());
    }

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

  // TODO: Twitter does not use the client and the java api for YCSB is in progress
  //       so they both relies on a fixed tablet-id to be created.
  //       Remove this stuff once everything is switched to the client create/open api.
  LOG(INFO) << "Setting up demo tablets...";
  const string& id = FLAGS_tablet_server_tablet_id;
  Schema schema;
  if (id == kYCSBTabletId) {
    schema = kudu::CreateYCSBSchema();
  } else {
    LOG(WARNING) << "Unknown tablet_server_tablet_id: " << id;
  }
  if (schema.initialized()) {
    TemporaryTabletsForDemos demo_setup(&server, schema, id);
  }

  // Temporary hack for demos: start threads which compact/flush the tablet.
  // Eventually this will be part of TabletServer itself, and take care of deciding
  // which tablet to perform operations on. But as a stop-gap, just start these
  // simple threads here from main.
  LOG(INFO) << "Starting flush/compact threads";
  const TSTabletManager* ts_tablet_manager = server.tablet_manager();
  boost::thread compact_thread(CompactThread, ts_tablet_manager);
  boost::thread compact_deltas_thread(CompactDeltasThread, ts_tablet_manager);
  boost::thread flush_thread(FlushThread, ts_tablet_manager);
  boost::thread flushdm_thread(FlushDeltaMemStoresThread, ts_tablet_manager);

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
