// Copyright (c) 2013, Cloudera, inc.

#include "tserver/mini_tablet_server.h"

#include <glog/logging.h>
#include <vector>

#include "common/schema.h"
#include "gutil/macros.h"
#include "server/metadata.h"
#include "server/metadata_util.h"
#include "server/rpc_server.h"
#include "tablet/tablet.h"
#include "tserver/tablet_server.h"
#include "util/net/sockaddr.h"
#include "util/status.h"

using kudu::metadata::TabletMetadata;
using kudu::tablet::Tablet;

namespace kudu {
namespace tserver {

MiniTabletServer::MiniTabletServer(Env* env,
                                   const string& fs_root)
  : started_(false),
    env_(env),
    fs_root_(fs_root) {
}

MiniTabletServer::~MiniTabletServer() {
}

Status MiniTabletServer::Start() {
  CHECK(!started_);

  // Init the filesystem manager.
  fs_manager_.reset(new FsManager(env_, fs_root_));
  RETURN_NOT_OK(fs_manager_->CreateInitialFileSystemLayout());

  // Start server on loopback.
  RpcServerOptions opts;
  opts.rpc_bind_addresses = "127.0.0.1:0";

  // TODO: set web server port to 0 also. When this is done,
  // remove the RESOURCE_LOCK from tests which use tablet servers
  // so they can run in parallel.

  gscoped_ptr<TabletServer> server(new TabletServer(opts));
  RETURN_NOT_OK(server->Init());
  RETURN_NOT_OK(server->Start());

  // Find the ephemeral address of the server.
  vector<Sockaddr> addrs;
  server->rpc_server()->GetBoundAddresses(&addrs);
  CHECK(!addrs.empty());

  bound_addr_ = addrs[0];
  server_.swap(server);
  started_ = true;
  return Status::OK();
}

Status MiniTabletServer::AddTestTablet(const std::string& tablet_id,
                                       const Schema& schema) {
  CHECK(started_) << "Must Start()";

  metadata::TabletMasterBlockPB master_block;
  master_block.set_tablet_id(tablet_id);
  // TODO: generate master block IDs based on tablet ID for tests?
  // This won't allow multiple tablets
  master_block.set_block_a("00000000000000000000000000000000");
  master_block.set_block_b("11111111111111111111111111111111");
  gscoped_ptr<TabletMetadata> meta(
    new TabletMetadata(fs_manager_.get(), master_block));

  shared_ptr<Tablet> t(new Tablet(meta.Pass(), schema));
  RETURN_NOT_OK(t->CreateNew());
  server_->RegisterTablet(t);
  return Status::OK();
}

const Sockaddr& MiniTabletServer::bound_addr() const {
  CHECK(started_);
  return bound_addr_;
}

} // namespace tserver
} // namespace kudu
