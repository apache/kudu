// Copyright (c) 2013, Cloudera, inc.

#include "kudu/integration-tests/mini_cluster.h"

#include <boost/foreach.hpp>

#include "kudu/client/client.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/master/ts_manager.h"
#include "kudu/rpc/messenger.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util.h"

using strings::Substitute;

namespace kudu {

using master::MiniMaster;
using master::TSDescriptor;
using master::TabletLocationsPB;
using std::tr1::shared_ptr;
using tserver::MiniTabletServer;
using tserver::TabletServer;

MiniClusterOptions::MiniClusterOptions()
  : num_tablet_servers(1),
    data_root(JoinPathSegments(GetTestDataDirectory(), "minicluster-data")),
    master_rpc_port(0) {
}

MiniCluster::MiniCluster(Env* env, const MiniClusterOptions& options)
  : running_(false),
    env_(env),
    fs_root_(options.data_root),
    num_ts_initial_(options.num_tablet_servers),
    master_rpc_port_(options.master_rpc_port),
    tserver_rpc_ports_(options.tserver_rpc_ports) {
}

MiniCluster::~MiniCluster() {
  CHECK(!running_);
}

Status MiniCluster::Start() {
  CHECK(!fs_root_.empty()) << "No Fs root was provided";
  CHECK(!running_);
  if (!env_->FileExists(fs_root_)) {
    RETURN_NOT_OK(env_->CreateDir(fs_root_));
  }

  // start the master (we need the port to set on the servers)
  gscoped_ptr<MiniMaster> mini_master(new MiniMaster(env_, GetMasterFsRoot(), master_rpc_port_));
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

  uint16_t ts_rpc_port = 0;
  if (tserver_rpc_ports_.size() > new_idx) {
    ts_rpc_port = tserver_rpc_ports_[new_idx];
  }
  gscoped_ptr<MiniTabletServer> tablet_server(
    new MiniTabletServer(env_, GetTabletServerFsRoot(new_idx), ts_rpc_port));
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

