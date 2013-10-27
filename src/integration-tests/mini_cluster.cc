// Copyright (c) 2013, Cloudera, inc.

#include "integration-tests/mini_cluster.h"

#include <boost/foreach.hpp>

#include "gutil/strings/substitute.h"
#include "master/mini_master.h"
#include "tserver/mini_tablet_server.h"
#include "tserver/tablet_server.h"
#include "util/status.h"

using strings::Substitute;

namespace kudu {

using master::MiniMaster;
using std::tr1::shared_ptr;
using tserver::MiniTabletServer;

MiniCluster::MiniCluster(Env* env,
                         const string& fs_root,
                         int num_tablet_servers)
  : started_(false),
    env_(env),
    fs_root_(fs_root),
    num_tablet_servers_(num_tablet_servers) {
}

MiniCluster::~MiniCluster() {
}

Status MiniCluster::Start() {
  CHECK(!fs_root_.empty()) << "No Fs root was provided";
  CHECK(!started_);

  // start the master (we need the port to set on the servers)
  gscoped_ptr<MiniMaster> mini_master(new MiniMaster(env_, GetMasterFsRoot()));
  RETURN_NOT_OK(mini_master->Start());
  mini_master_.reset(mini_master.release());

  for (int i = 0; i < num_tablet_servers_; i++) {
    gscoped_ptr<MiniTabletServer> tablet_server(new MiniTabletServer(env_, GetTabletServerFsRoot(i)));
    // set the master port
    tablet_server->options()->master_hostport = HostPort(mini_master_.get()->bound_rpc_addr());
    RETURN_NOT_OK(tablet_server->Start());
    mini_tablet_servers_.push_back(shared_ptr<MiniTabletServer>(tablet_server.release()));
  }
  started_ = true;
  return Status::OK();
}

Status MiniCluster::Shutdown() {
  BOOST_FOREACH(const shared_ptr<MiniTabletServer>& tablet_server, mini_tablet_servers_) {
    WARN_NOT_OK(tablet_server->Shutdown(),
                Substitute("Could not shutdown TabletServer: $0",
                           tablet_server->server()->ToString()));
  }
  WARN_NOT_OK(mini_master_->Shutdown(), "Could not shutdown master.");
  return Status::OK();
}

MiniTabletServer* MiniCluster::mini_tablet_server(int idx) {
  CHECK_GE(idx, 0) << "TabletServer idx must be >= 0";
  CHECK_LT(idx, num_tablet_servers_) << "TabletServer idx must be < 'num_tablet_servers'";
  return mini_tablet_servers_[idx].get();
}

string MiniCluster::GetMasterFsRoot() {
  return env_->JoinPathSegments(fs_root_, "master-root");
}

string MiniCluster::GetTabletServerFsRoot(int idx) {
  return env_->JoinPathSegments(fs_root_, Substitute("ts-$0-root", idx));
}

} // namespace kudu

