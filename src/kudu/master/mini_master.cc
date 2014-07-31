// Copyright (c) 2013, Cloudera, inc.

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
  return StartOnPorts(rpc_port_, 0);
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

  // Start RPC server on loopback.
  opts.rpc_opts.rpc_bind_addresses = Substitute("127.0.0.1:$0", rpc_port);
  opts.webserver_opts.port = web_port;
  opts.base_dir = fs_root_;

  gscoped_ptr<Master> server(new Master(opts));
  RETURN_NOT_OK(server->Init());
  RETURN_NOT_OK(server->Start());

  master_.swap(server);
  running_ = true;
  return Status::OK();
}

Status MiniMaster::Restart() {
  CHECK(running_);

  Sockaddr prev_rpc = bound_rpc_addr();
  Sockaddr prev_http = bound_http_addr();
  Shutdown();

  RETURN_NOT_OK(StartOnPorts(prev_rpc.port(), prev_http.port()));
  CHECK(running_);
  return Status::OK();
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
