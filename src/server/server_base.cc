// Copyright (c) 2013, Cloudera, inc.

#include "server/server_base.h"

#include <gflags/gflags.h>
#include <vector>

#include "common/wire_protocol.pb.h"
#include "rpc/messenger.h"
#include "server/default-path-handlers.h"
#include "server/rpc_server.h"
#include "server/webserver.h"
#include "util/env.h"
#include "util/net/sockaddr.h"

DEFINE_int32(num_reactor_threads, 4, "Number of libev reactor threads to start."
             " (Advanced option).");

using std::vector;

namespace kudu {
namespace server {

ServerBase::ServerBase(const RpcServerOptions& rpc_opts,
                       const WebserverOptions& web_opts)
  : rpc_server_(new RpcServer(rpc_opts)),
    web_server_(new Webserver(web_opts)) {
}

ServerBase::~ServerBase() {
  Shutdown();
}

Sockaddr ServerBase::first_rpc_address() const {
  vector<Sockaddr> addrs;
  rpc_server_->GetBoundAddresses(&addrs);
  CHECK(!addrs.empty()) << "Not bound";
  return addrs[0];
}

Sockaddr ServerBase::first_http_address() const {
  vector<Sockaddr> addrs;
  web_server_->GetBoundAddresses(&addrs);
  CHECK(!addrs.empty()) << "Not bound";
  return addrs[0];
}

const NodeInstancePB& ServerBase::instance_pb() const {
  return *DCHECK_NOTNULL(instance_pb_.get());
}

Status ServerBase::GenerateInstanceID() {
  // TODO: this should be something stored on local disks,
  // with a sequence number instead of the system time.
  instance_pb_.reset(new NodeInstancePB);
  instance_pb_->set_permanent_uuid("TODO_perm_uuid");
  instance_pb_->set_instance_seqno(Env::Default()->NowMicros());
  return Status::OK();
}

Status ServerBase::Init() {
  RETURN_NOT_OK(GenerateInstanceID());

  // Create the Messenger.
  rpc::MessengerBuilder builder("TODO: add a ToString for ServerBase");

  builder.set_num_reactors(FLAGS_num_reactor_threads);
  RETURN_NOT_OK(builder.Build(&messenger_));

  RETURN_NOT_OK(rpc_server_->Init());
  return Status::OK();
}

Status ServerBase::Start(gscoped_ptr<rpc::ServiceIf> rpc_impl) {
  RETURN_NOT_OK(rpc_server_->Start(messenger_, rpc_impl.Pass()));

  AddDefaultPathHandlers(web_server_.get());
  RETURN_NOT_OK(web_server_->Start());
  return Status::OK();
}

Status ServerBase::Shutdown() {
  if (messenger_) {
    messenger_->Shutdown();
    messenger_.reset();
  }
  web_server_->Stop();
  rpc_server_->Shutdown();
  return Status::OK();
}

} // namespace server
} // namespace kudu
