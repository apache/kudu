// Copyright (c) 2013, Cloudera, inc.

#include "tserver/mini_tablet_server.h"

#include <glog/logging.h>

#include "common/schema.h"
#include "gutil/macros.h"
#include "server/metadata.h"
#include "server/metadata_util.h"
#include "server/rpc_server.h"
#include "server/webserver.h"
#include "tablet/tablet.h"
#include "tablet/tablet_peer.h"
#include "tserver/tablet_server.h"
#include "consensus/log.h"
#include "consensus/log.pb.h"
#include "consensus/consensus.h"
#include "consensus/consensus.pb.h"
#include "consensus/local_consensus.h"
#include "util/net/sockaddr.h"
#include "util/status.h"

using kudu::consensus::Consensus;
using kudu::consensus::ConsensusOptions;
using kudu::consensus::QuorumPeerPB;
using kudu::consensus::QuorumPB;
using kudu::consensus::OpId;
using kudu::consensus::LEADER;
using kudu::consensus::LocalConsensus;
using kudu::log::Log;
using kudu::log::LogOptions;
using kudu::metadata::TabletMetadata;
using kudu::metadata::TabletServerPB;
using kudu::tablet::Tablet;
using kudu::tablet::TabletPeer;

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

  TabletServerOptions opts;

  // Start RPC server on loopback.
  opts.rpc_opts.rpc_bind_addresses = "127.0.0.1:0";
  opts.webserver_opts.port = 0;

  gscoped_ptr<TabletServer> server(new TabletServer(opts));
  RETURN_NOT_OK(server->Init());
  RETURN_NOT_OK(server->Start());

  server_.swap(server);
  started_ = true;
  return Status::OK();
}

Status MiniTabletServer::Shutdown() {
  RETURN_NOT_OK(server_->Shutdown());
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

  shared_ptr<metadata::TabletSuperBlockPB> super_block;
  meta->ToSuperBlock(&super_block);

  shared_ptr<Tablet> t(new Tablet(meta.Pass(), schema));
  RETURN_NOT_OK(t->CreateNew());

  shared_ptr<TabletPeer> tablet_peer(new TabletPeer(t));
  RETURN_NOT_OK(tablet_peer->Init());
  RETURN_NOT_OK(tablet_peer->Start());

  server_->RegisterTablet(tablet_peer);
  return Status::OK();
}

const Sockaddr MiniTabletServer::bound_rpc_addr() const {
  CHECK(started_);
  return server_->first_rpc_address();
}

const Sockaddr MiniTabletServer::bound_http_addr() const {
  CHECK(started_);
  return server_->first_http_address();
}

FsManager* MiniTabletServer::fs_manager() {
  CHECK(started_);
  return fs_manager_.get();
}

} // namespace tserver
} // namespace kudu
