// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/master/mini_master.h"

#include <boost/assign/list_of.hpp>
#include <string>

#include <glog/logging.h>

#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/server/rpc_server.h"
#include "kudu/server/webserver.h"
#include "kudu/master/master.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"

using strings::Substitute;

DECLARE_bool(rpc_server_allow_ephemeral_ports);

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
  FLAGS_rpc_server_allow_ephemeral_ports = true;
  RETURN_NOT_OK(StartOnPorts(rpc_port_, 0));
  return master_->WaitForCatalogManagerInit();
}


Status MiniMaster::StartDistributedMaster(const vector<uint16_t>& peer_ports) {
  CHECK(!running_);
  return StartDistributedMasterOnPorts(rpc_port_, 0, peer_ports);
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
  opts->fs_opts.wal_path = fs_root_;
  opts->fs_opts.data_paths = boost::assign::list_of(fs_root_);

  gscoped_ptr<Master> server(new Master(*opts));
  RETURN_NOT_OK(server->Init());
  RETURN_NOT_OK(server->StartAsync());

  master_.swap(server);
  running_ = true;

  return Status::OK();
}

Status MiniMaster::StartDistributedMasterOnPorts(uint16_t rpc_port, uint16_t web_port,
                                                 const vector<uint16_t>& peer_ports) {
  CHECK(!running_);
  CHECK(!master_);

  MasterOptions opts;

  vector<HostPort> peer_addresses;
  BOOST_FOREACH(uint16_t peer_port, peer_ports) {
    HostPort peer_address("127.0.0.1", peer_port);
    peer_addresses.push_back(peer_address);
  }
  opts.master_addresses = peer_addresses;

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

std::string MiniMaster::permanent_uuid() const {
  CHECK(master_);
  return DCHECK_NOTNULL(master_->fs_manager())->uuid();
}

std::string MiniMaster::bound_rpc_addr_str() const {
  return bound_rpc_addr().ToString();
}

} // namespace master
} // namespace kudu
