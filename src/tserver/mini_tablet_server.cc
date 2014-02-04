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
#include "tserver/ts_tablet_manager.h"
#include "consensus/log.h"
#include "consensus/log.pb.h"
#include "consensus/consensus.h"
#include "consensus/consensus.pb.h"
#include "consensus/local_consensus.h"
#include "util/net/sockaddr.h"
#include "util/status.h"

using kudu::consensus::Consensus;
using kudu::consensus::ConsensusOptions;
using kudu::consensus::OpId;
using kudu::log::Log;
using kudu::log::LogOptions;
using kudu::metadata::QuorumPeerPB;
using kudu::metadata::QuorumPB;
using kudu::metadata::TabletMetadata;

namespace kudu {
namespace tserver {

MiniTabletServer::MiniTabletServer(Env* env,
                                   const string& fs_root)
  : started_(false),
    env_(env),
    fs_root_(fs_root) {

  // Start RPC server on loopback.
  opts_.rpc_opts.rpc_bind_addresses = "127.0.0.1:0";
  opts_.webserver_opts.port = 0;
  opts_.base_dir = fs_root;
}

MiniTabletServer::~MiniTabletServer() {
}

Status MiniTabletServer::Start() {
  CHECK(!started_);

  // Init the filesystem manager.
  // TODO: this is kind of redundant -- the TS class itself makes its own
  // FsManager as well - do we need this? Confuses things.
  fs_manager_.reset(new FsManager(env_, fs_root_));
  RETURN_NOT_OK(fs_manager_->CreateInitialFileSystemLayout());

  gscoped_ptr<TabletServer> server(new TabletServer(opts_));
  RETURN_NOT_OK(server->Init());
  RETURN_NOT_OK(server->Start());

  server_.swap(server);
  started_ = true;
  return Status::OK();
}

Status MiniTabletServer::WaitStarted() {
  return server_->WaitInited();
}

void MiniTabletServer::Shutdown() {
  server_->Shutdown();
  server_.reset();
  started_ = false;
}

Status MiniTabletServer::AddTestTablet(const std::string& table_id,
                                       const std::string& tablet_id,
                                       const Schema& schema) {
  QuorumPB quorum;
  quorum.set_seqno(0);
  return AddTestTablet(table_id, tablet_id, schema, quorum);
}

Status MiniTabletServer::AddTestTablet(const std::string& table_id,
                                       const std::string& tablet_id,
                                       const Schema& schema,
                                       const QuorumPB& quorum) {
  CHECK(started_) << "Must Start()";
  return server_->tablet_manager()->CreateNewTablet(
    table_id, tablet_id, "", "", table_id, SchemaBuilder(schema).Build(), quorum, NULL);
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
  return CHECK_NOTNULL(fs_manager_.get());
}

} // namespace tserver
} // namespace kudu
