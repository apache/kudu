// Copyright (c) 2013, Cloudera, inc.

#include "master/mini_master.h"

#include <glog/logging.h>
#include <string>

#include "server/rpc_server.h"
#include "server/webserver.h"
#include "master/master.h"
#include "util/net/sockaddr.h"
#include "util/status.h"

namespace kudu {
namespace master {

MiniMaster::MiniMaster(Env* env, const string& fs_root)
  : started_(false),
    env_(env),
    fs_root_(fs_root) {
}

MiniMaster::~MiniMaster() {
}

Status MiniMaster::Start() {
  CHECK(!started_);

  MasterOptions opts;

  // Start RPC server on loopback.
  opts.rpc_opts.rpc_bind_addresses = "127.0.0.1:0";
  opts.webserver_opts.port = 0;

  gscoped_ptr<Master> server(new Master(opts));
  RETURN_NOT_OK(server->Init());
  RETURN_NOT_OK(server->Start());

  server_.swap(server);
  started_ = true;
  return Status::OK();
}

const Sockaddr MiniMaster::bound_rpc_addr() const {
  CHECK(started_);
  return server_->first_rpc_address();
}

const Sockaddr MiniMaster::bound_http_addr() const {
  CHECK(started_);
  return server_->first_http_address();
}

} // namespace master
} // namespace kudu
