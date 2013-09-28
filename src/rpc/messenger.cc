// Copyright (c) 2013, Cloudera, inc.

#include "rpc/messenger.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <boost/foreach.hpp>
#include <boost/thread.hpp>
#include <glog/logging.h>
#include <list>
#include <set>
#include <string>

#include "gutil/gscoped_ptr.h"
#include "rpc/acceptor_pool.h"
#include "rpc/connection.h"
#include "rpc/constants.h"
#include "rpc/reactor.h"
#include "rpc/rpc_header.pb.h"
#include "rpc/sasl_common.h"
#include "rpc/transfer.h"
#include "util/errno.h"
#include "util/monotime.h"
#include "util/net/socket.h"
#include "util/status.h"
#include "util/task_executor.h"

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
    coarse_timer_granularity_(MonoDelta::FromMilliseconds(100)),
    service_queue_length_(50) {
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

MessengerBuilder &MessengerBuilder::set_service_queue_length(
                                int service_queue_length) {
  service_queue_length_ = service_queue_length;
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
  msgr->reset(ptr);
  return Status::OK();
}

void Messenger::Shutdown() {
  {
    boost::lock_guard<boost::mutex> guard(lock_);
    if (closing_) {
      return;
    }
    VLOG(1) << "shutting down messenger " << name_;
    closing_ = true;
  }
  service_queue_.Shutdown();

  // Drain any remaining calls that haven't been responded to.
  InboundCall *call;
  while (service_queue_.BlockingGet(&call)) {
    call->RespondFailure(Status::RuntimeError("Server shutting down"));
  }


  BOOST_FOREACH(shared_ptr<AcceptorPool> &pool, acceptor_pools_) {
    pool->Shutdown();
  }
  BOOST_FOREACH(Reactor* reactor, reactors_) {
    reactor->Shutdown();
  }
  negotiation_executor_->Shutdown();
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

void Messenger::QueueOutboundCall(const shared_ptr<OutboundCall> &call) {
  Reactor *reactor = RemoteToReactor(call->conn_id().remote());
  reactor->QueueOutboundCall(call);
}

void Messenger::QueueInboundCall(gscoped_ptr<InboundCall> call) {
  InboundCall *c = call.release();

  // Queue message on service queue
  if (PREDICT_TRUE(service_queue_.Put(c))) {
    return;
  }
  // TODO: write a test for backpressure!

  c->RespondFailure(
    Status::NetworkError(StringPrintf(
                           "The service queue is full; it "
                           "has %zd items. Transfer dropped because of backpressure.",
                           service_queue_.max_elements())));
}

void Messenger::RegisterInboundSocket(Socket *new_socket, const Sockaddr &remote) {
  Reactor *reactor = RemoteToReactor(remote);
  reactor->RegisterInboundSocket(new_socket, remote);
}

Messenger::Messenger(const MessengerBuilder &bld)
  : closing_(false),
    service_queue_(bld.service_queue_length_),
    name_(bld.name_) {
  for (int i = 0; i < bld.num_reactors_; i++) {
    reactors_.push_back(new Reactor(this, i, bld));
  }
  negotiation_executor_.reset(TaskExecutor::CreateNew(bld.num_negotiation_threads_));
}

Messenger::~Messenger() {
  Shutdown();
  BOOST_FOREACH(Reactor* r, reactors_) {
    delete r;
  }
  reactors_.clear();
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
