// Copyright (c) 2013, Cloudera, inc.

#include "integration-tests/mini_cluster.h"

#include <boost/foreach.hpp>

#include "gutil/strings/substitute.h"
#include "master/mini_master.h"
#include "master/catalog_manager.h"
#include "master/master.h"
#include "master/ts_descriptor.h"
#include "master/ts_manager.h"
#include "tserver/mini_tablet_server.h"
#include "tserver/tablet_server.h"
#include "util/path_util.h"
#include "util/status.h"
#include "util/stopwatch.h"

using strings::Substitute;

namespace kudu {

using master::MiniMaster;
using master::TSDescriptor;
using master::TabletLocationsPB;
using std::tr1::shared_ptr;
using tserver::MiniTabletServer;
using tserver::TabletServer;

MiniCluster::MiniCluster(Env* env,
                         const string& fs_root,
                         int num_tablet_servers)
  : running_(false),
    env_(env),
    fs_root_(fs_root),
    num_ts_initial_(num_tablet_servers) {
}

MiniCluster::~MiniCluster() {
  CHECK(!running_);
}

Status MiniCluster::Start() {
  CHECK(!fs_root_.empty()) << "No Fs root was provided";
  CHECK(!running_);

  // start the master (we need the port to set on the servers)
  gscoped_ptr<MiniMaster> mini_master(new MiniMaster(env_, GetMasterFsRoot()));
  RETURN_NOT_OK_PREPEND(mini_master->Start(), "Couldn't start master");
  mini_master_.reset(mini_master.release());

  for (int i = 0; i < num_ts_initial_; i++) {
    RETURN_NOT_OK_PREPEND(AddTabletServer(),
                          Substitute("Error adding TS $0", i));
  }

  RETURN_NOT_OK_PREPEND(WaitForTabletServerCount(num_ts_initial_),
                        "Waiting for tablet servers to start");

  running_ = true;
  return Status::OK();
}

Status MiniCluster::StartSync() {
  RETURN_NOT_OK(Start());
  int count = 0;
  BOOST_FOREACH(const shared_ptr<MiniTabletServer>& tablet_server, mini_tablet_servers_) {
    RETURN_NOT_OK_PREPEND(tablet_server->WaitStarted(),
                          Substitute("TabletServer $0 based on dir: $1 failed to start.",
                                     count, tablet_server->options()->base_dir));
    count++;
  }
  return Status::OK();
}

Status MiniCluster::AddTabletServer() {
  if (!mini_master_) {
    return Status::IllegalState("Master not yet initialized");
  }
  int new_idx = mini_tablet_servers_.size();

  gscoped_ptr<MiniTabletServer> tablet_server(
    new MiniTabletServer(env_, GetTabletServerFsRoot(new_idx)));
  // set the master port
  tablet_server->options()->master_hostport = HostPort(mini_master_.get()->bound_rpc_addr());
  RETURN_NOT_OK(tablet_server->Start())
  mini_tablet_servers_.push_back(shared_ptr<MiniTabletServer>(tablet_server.release()));
  return Status::OK();
}

void MiniCluster::Shutdown() {
  BOOST_FOREACH(const shared_ptr<MiniTabletServer>& tablet_server, mini_tablet_servers_) {
    tablet_server->Shutdown();
  }
  mini_master_->Shutdown();
  running_ = false;
}

MiniTabletServer* MiniCluster::mini_tablet_server(int idx) {
  CHECK_GE(idx, 0) << "TabletServer idx must be >= 0";
  CHECK_LT(idx, mini_tablet_servers_.size()) << "TabletServer idx must be < 'num_ts_started_'";
  return mini_tablet_servers_[idx].get();
}

string MiniCluster::GetMasterFsRoot() {
  return JoinPathSegments(fs_root_, "master-root");
}

string MiniCluster::GetTabletServerFsRoot(int idx) {
  return JoinPathSegments(fs_root_, Substitute("ts-$0-root", idx));
}

Status MiniCluster::WaitForReplicaCount(const string& tablet_id,
                                        int expected_count) {
  TabletLocationsPB locations;
  return WaitForReplicaCount(tablet_id, expected_count, &locations);
}

Status MiniCluster::WaitForReplicaCount(const string& tablet_id,
                                        int expected_count,
                                        TabletLocationsPB* locations) {
  Stopwatch sw;
  sw.start();
  while (sw.elapsed().wall_seconds() < kTabletReportWaitTimeSeconds) {
    mini_master_->master()->catalog_manager()->GetTabletLocations(tablet_id, locations);
    if ((locations->stale() && expected_count == 0) ||
        (!locations->stale() && locations->replicas_size() == expected_count)) {
      return Status::OK();
    }

    usleep(1 * 1000); // 1ms
  }
  return Status::TimedOut(Substitute("Tablet $0 never reached expected replica count $1",
                                     tablet_id, expected_count));
}

Status MiniCluster::WaitForTabletServerCount(int count) {
  vector<shared_ptr<master::TSDescriptor> > descs;
  return WaitForTabletServerCount(count, &descs);
}

Status MiniCluster::WaitForTabletServerCount(int count,
                                             vector<shared_ptr<TSDescriptor> >* descs) {
  Stopwatch sw;
  sw.start();
  while (sw.elapsed().wall_seconds() < kRegistrationWaitTimeSeconds) {
    mini_master_->master()->ts_manager()->GetAllDescriptors(descs);
    if (descs->size() == count) {
      // GetAllDescriptors() may return servers that are no longer online.
      // Do a second step of verification to verify that the descs that we got
      // are aligned (same uuid/seqno) with the TSs that we have in the cluster.
      int match_count = 0;
      BOOST_FOREACH(const shared_ptr<TSDescriptor>& desc, *descs) {
        for (int i = 0; i < mini_tablet_servers_.size(); ++i) {
          TabletServer *ts = mini_tablet_servers_[i]->server();
          if (ts->instance_pb().permanent_uuid() == desc->permanent_uuid() &&
              ts->instance_pb().instance_seqno() == desc->latest_seqno()) {
            match_count++;
            break;
          }
        }
      }

      if (match_count == count) {
        LOG(INFO) << count << " TS(s) registered with Master after "
                  << sw.elapsed().wall_seconds() << "s";
        return Status::OK();
      }
    }
    usleep(1 * 1000); // 1ms
  }
  return Status::TimedOut(Substitute("$0 TS(s) never registered with master", count));
}

} // namespace kudu

