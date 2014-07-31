// Copyright (c) 2013, Cloudera, inc.

#include "kudu/tserver/mini_tablet_server.h"

#include <glog/logging.h>

#include "kudu/common/schema.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/server/metadata.h"
#include "kudu/server/rpc_server.h"
#include "kudu/server/webserver.h"
#include "kudu/tablet/maintenance_manager.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_peer.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log.pb.h"
#include "kudu/consensus/consensus.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/local_consensus.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"

using kudu::consensus::Consensus;
using kudu::consensus::ConsensusOptions;
using kudu::consensus::OpId;
using kudu::log::Log;
using kudu::log::LogOptions;
using kudu::metadata::QuorumPeerPB;
using kudu::metadata::QuorumPB;
using kudu::metadata::TabletMetadata;
using strings::Substitute;

namespace kudu {
namespace tserver {

MiniTabletServer::MiniTabletServer(Env* env,
                                   const string& fs_root,
                                   uint16_t rpc_port)
  : started_(false),
    env_(env),
    fs_root_(fs_root) {

  // Start RPC server on loopback.
  opts_.rpc_opts.rpc_bind_addresses = Substitute("127.0.0.1:$0", rpc_port);
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
  if (started_) {
    server_->Shutdown();
    server_.reset();
  }
  started_ = false;
}

QuorumPB MiniTabletServer::CreateLocalQuorum() const {
  QuorumPB quorum;
  quorum.set_seqno(0);
  quorum.set_local(true);
  QuorumPeerPB* peer = quorum.add_peers();
  peer->set_permanent_uuid(server_->instance_pb().permanent_uuid());
  peer->set_role(QuorumPeerPB::LEADER);
  return quorum;
}

Status MiniTabletServer::AddTestTablet(const std::string& table_id,
                                       const std::string& tablet_id,
                                       const Schema& schema) {
  return AddTestTablet(table_id, tablet_id, schema, CreateLocalQuorum());
}

Status MiniTabletServer::AddTestTablet(const std::string& table_id,
                                       const std::string& tablet_id,
                                       const Schema& schema,
                                       const QuorumPB& quorum) {
  CHECK(started_) << "Must Start()";
  return server_->tablet_manager()->CreateNewTablet(
    table_id, tablet_id, "", "", table_id, SchemaBuilder(schema).Build(), quorum, NULL);
}

void MiniTabletServer::FailHeartbeats() {
  server_->set_fail_heartbeats_for_tests(true);
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
