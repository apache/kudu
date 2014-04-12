// Copyright (c) 2013, Cloudera, inc.

#include "rpc/messenger.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <boost/foreach.hpp>
#include <boost/thread/locks.hpp>
#include <glog/logging.h>
#include <list>
#include <set>
#include <string>

#include "gutil/gscoped_ptr.h"
#include "gutil/stl_util.h"
#include "rpc/acceptor_pool.h"
#include "rpc/connection.h"
#include "rpc/constants.h"
#include "rpc/reactor.h"
#include "rpc/rpc_header.pb.h"
#include "rpc/rpc_service.h"
#include "rpc/sasl_common.h"
#include "rpc/transfer.h"
#include "util/errno.h"
#include "util/metrics.h"
#include "util/monotime.h"
#include "util/net/socket.h"
#include "util/status.h"
#include "util/task_executor.h"
#include "util/trace.h"

using std::string;
using std::tr1::shared_ptr;

DEFINE_int32(accept_backlog, 128, "backlog parameter to use for accept");

namespace kudu {
namespace rpc {

class Messenger;
class ServerBuilder;

MessengerBuilder::MessengerBuilder(const std::string &name)
  : name_(name),
    connection_keepalive_time_(MonoDelta::FromSeconds(10)),
    num_reactors_(4),
    num_negotiation_threads_(4),
    coarse_timer_granularity_(MonoDelta::FromMilliseconds(100)) {
}

MessengerBuilder& MessengerBuilder::set_connection_keepalive_time(const MonoDelta &keepalive) {
  connection_keepalive_time_ = keepalive;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_num_reactors(int num_reactors) {
  num_reactors_ = num_reactors;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_negotiation_threads(int num_negotiation_threads) {
  num_negotiation_threads_ = num_negotiation_threads;
  return *this;
}

MessengerBuilder& MessengerBuilder::set_coarse_timer_granularity(const MonoDelta &granularity) {
  coarse_timer_granularity_ = granularity;
  return *this;
}

MessengerBuilder &MessengerBuilder::set_metric_context(const MetricContext& metric_ctx) {
  metric_ctx_.reset(new MetricContext(metric_ctx, "rpc"));
  return *this;
}

Status MessengerBuilder::Build(Messenger **msgr) {
  RETURN_NOT_OK(SaslInit(kSaslAppName)); // Initialize SASL library before we start making requests
  gscoped_ptr<Messenger> new_msgr(new Messenger(*this));
  RETURN_NOT_OK(new_msgr.get()->Init());
  *msgr = new_msgr.release();
  return Status::OK();
}

Status MessengerBuilder::Build(shared_ptr<Messenger> *msgr) {
  Messenger *ptr;
  RETURN_NOT_OK(Build(&ptr));

  // See docs on Messenger::retain_self_ for info about this odd hack.
  *msgr = shared_ptr<Messenger>(
    ptr, std::mem_fun(&Messenger::AllExternalReferencesDropped));
  return Status::OK();
}

// See comment on Messenger::retain_self_ member.
void Messenger::AllExternalReferencesDropped() {
  Shutdown();
  CHECK(retain_self_.get());
  // If we have no more external references, then we no longer
  // need to retain ourself. We'll destruct as soon as all our
  // internal-facing references are dropped (ie those from reactor
  // threads).
  retain_self_.reset();
}

void Messenger::Shutdown() {
  boost::lock_guard<boost::mutex> guard(lock_);
  if (closing_) {
    return;
  }
  VLOG(1) << "shutting down messenger " << name_;
  closing_ = true;

  DCHECK(!rpc_service_) << "Unregister RPC services before shutting down Messenger";
  rpc_service_ = NULL;

  BOOST_FOREACH(const shared_ptr<AcceptorPool>& acceptor_pool, acceptor_pools_) {
    acceptor_pool->Shutdown();
  }
  acceptor_pools_.clear();

  // Need to shut down negotiation executor before the reactors, since the
  // reactors close the Connection sockets, and may race against the negotiation
  // thread's blocking reads & writes.
  negotiation_executor_->Shutdown();

  BOOST_FOREACH(Reactor* reactor, reactors_) {
    reactor->Shutdown();
  }
}

Status Messenger::AddAcceptorPool(const Sockaddr &accept_addr,
                                  int num_threads,
                                  shared_ptr<AcceptorPool>* pool) {
  Socket sock;
  RETURN_NOT_OK(sock.Init(0));
  RETURN_NOT_OK(sock.BindAndListen(accept_addr, FLAGS_accept_backlog));
  Sockaddr remote;
  RETURN_NOT_OK(sock.GetSocketAddress(&remote));
  shared_ptr<AcceptorPool> acceptor_pool(new AcceptorPool(this, &sock, remote));
  RETURN_NOT_OK(acceptor_pool->Init(num_threads));
  boost::lock_guard<boost::mutex> guard(lock_);
  acceptor_pools_.push_back(acceptor_pool);
  *pool = acceptor_pool;
  return Status::OK();
}

// Register a new RpcService to handle inbound requests.
Status Messenger::RegisterService(const string& service_name,
                                  const scoped_refptr<RpcService>& service) {
  DCHECK(service);
  boost::lock_guard<boost::mutex> guard(lock_);
  // TODO: Key services on service name.
  rpc_service_ = service;
  return Status::OK();
}

// Unregister an RpcService.
Status Messenger::UnregisterService(const string& service_name) {
  boost::lock_guard<boost::mutex> guard(lock_);
  if (!rpc_service_) {
    return Status::ServiceUnavailable("Service is not registered");
  } else {
    rpc_service_ = NULL;
  }
  return Status::OK();
}

void Messenger::QueueOutboundCall(const shared_ptr<OutboundCall> &call) {
  Reactor *reactor = RemoteToReactor(call->conn_id().remote());
  reactor->QueueOutboundCall(call);
}

void Messenger::QueueInboundCall(gscoped_ptr<InboundCall> call) {
  TRACE_TO(call->trace(), "Inserting onto call queue");

  boost::lock_guard<boost::mutex> guard(lock_);
  if (PREDICT_FALSE(!rpc_service_)) {
    Status s = Status::ServiceUnavailable("RPC Service is not registered",
                                          call->ToString());
    LOG(INFO) << s.ToString();
    call.release()->RespondFailure(ErrorStatusPB::ERROR_NO_SUCH_SERVICE, s);
    return;
  }

  // The RpcService will respond to the client on success or failure.
  WARN_NOT_OK(rpc_service_->QueueInboundCall(call.Pass()), "Unable to handle RPC call");
}

void Messenger::RegisterInboundSocket(Socket *new_socket, const Sockaddr &remote) {
  Reactor *reactor = RemoteToReactor(remote);
  reactor->RegisterInboundSocket(new_socket, remote);
}

Messenger::Messenger(const MessengerBuilder &bld)
  : name_(bld.name_),
    closing_(false),
    retain_self_(this) {
  if (bld.metric_ctx_) {
    metric_ctx_.reset(new MetricContext(*bld.metric_ctx_));
  }
  for (int i = 0; i < bld.num_reactors_; i++) {
    reactors_.push_back(new Reactor(retain_self_, i, bld));
  }
  negotiation_executor_.reset(TaskExecutor::CreateNew("rpc negotiation",
                                                      bld.num_negotiation_threads_));
}

Messenger::~Messenger() {
  boost::lock_guard<boost::mutex> guard(lock_);
  CHECK(closing_) << "Should have already shut down";
  STLDeleteElements(&reactors_);
}

Reactor* Messenger::RemoteToReactor(const Sockaddr &remote) {
  uint32_t hashCode = remote.HashCode();
  int reactor_idx = hashCode % reactors_.size();
  // This is just a static partitioning; we could get a lot
  // fancier with assigning Sockaddrs to Reactors.
  return reactors_[reactor_idx];
}


Status Messenger::Init() {
  Status status;
  BOOST_FOREACH(Reactor* r, reactors_) {
    RETURN_NOT_OK(r->Init());
  }

  return Status::OK();
}

} // namespace rpc
} // namespace kudu
