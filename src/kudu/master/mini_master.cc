// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/master/mini_master.h"

#include <glog/logging.h>
#include <string>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/server/rpc_server.h"
#include "kudu/server/webserver.h"
#include "kudu/master/master.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"

using strings::Substitute;

namespace kudu {
namespace master {

MiniMaster::MiniMaster(Env* env, const string& fs_root, uint16_t rpc_port)
  : running_(false),
    env_(env),
    fs_root_(fs_root),
    rpc_port_(rpc_port) {
}

MiniMaster::~MiniMaster() {
  CHECK(!running_);
}

Status MiniMaster::Start() {
  CHECK(!running_);
  RETURN_NOT_OK(StartOnPorts(rpc_port_, 0));
  return master_->WaitForCatalogManagerInit();
}

Status MiniMaster::StartLeader(const vector<uint16_t>& follower_ports) {
  CHECK(!running_);
  return StartLeaderOnPorts(rpc_port_, 0, follower_ports);
}

Status MiniMaster::StartFollower(uint16_t leader_port, const vector<uint16_t>& peer_ports) {
  CHECK(!running_);
  return StartFollowerOnPorts(rpc_port_, 0, leader_port, peer_ports);
}

void MiniMaster::Shutdown() {
  if (running_) {
    master_->Shutdown();
  }
  running_ = false;
  master_.reset();
}

Status MiniMaster::StartOnPorts(uint16_t rpc_port, uint16_t web_port) {
  CHECK(!running_);
  CHECK(!master_);

  MasterOptions opts;
  return StartOnPorts(rpc_port, web_port, &opts);
}

Status MiniMaster::StartOnPorts(uint16_t rpc_port, uint16_t web_port,
                                MasterOptions* opts) {
  opts->rpc_opts.rpc_bind_addresses = Substitute("127.0.0.1:$0", rpc_port);
  opts->webserver_opts.port = web_port;
  opts->base_dir = fs_root_;

  gscoped_ptr<Master> server(new Master(*opts));
  RETURN_NOT_OK(server->Init());
  RETURN_NOT_OK(server->StartAsync());

  master_.swap(server);
  running_ = true;

  return Status::OK();
}

Status MiniMaster::StartLeaderOnPorts(uint16_t rpc_port, uint16_t web_port,
                                      const vector<uint16_t>& follower_ports) {
  CHECK(!running_);
  CHECK(!master_);

  MasterOptions opts;
  opts.leader = true;

  vector<HostPort> follower_addresses;
  BOOST_FOREACH(uint16_t follower_port, follower_ports) {
    HostPort follower_address("127.0.0.1", follower_port);
    follower_addresses.push_back(follower_address);
  }
  opts.follower_addresses = follower_addresses;

  return StartOnPorts(rpc_port, web_port, &opts);
}


Status MiniMaster::StartFollowerOnPorts(uint16_t rpc_port, uint16_t web_port,
                                        uint16_t leader_port,
                                        const vector<uint16_t>& peer_ports) {
  CHECK(!running_);
  CHECK(!master_);

  MasterOptions opts;
  opts.leader = false;
  opts.leader_address = HostPort("127.0.0.1", leader_port);

  vector<HostPort> peer_addresses;
  BOOST_FOREACH(uint16_t peer_port, peer_ports) {
    HostPort peer_address("127.0.0.1", peer_port);
    peer_addresses.push_back(peer_address);
  }
  opts.follower_addresses = peer_addresses;

  return StartOnPorts(rpc_port, web_port, &opts);
}

Status MiniMaster::Restart() {
  CHECK(running_);

  Sockaddr prev_rpc = bound_rpc_addr();
  Sockaddr prev_http = bound_http_addr();
  Shutdown();

  RETURN_NOT_OK(StartOnPorts(prev_rpc.port(), prev_http.port()));
  CHECK(running_);
  return WaitForCatalogManagerInit();
}

Status MiniMaster::WaitForCatalogManagerInit() {
  return master_->WaitForCatalogManagerInit();
}

const Sockaddr MiniMaster::bound_rpc_addr() const {
  CHECK(running_);
  return master_->first_rpc_address();
}

const Sockaddr MiniMaster::bound_http_addr() const {
  CHECK(running_);
  return master_->first_http_address();
}

} // namespace master
} // namespace kudu
