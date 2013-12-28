// Copyright (c) 2013, Cloudera, inc.

#include "rpc/acceptor_pool.h"

#include <boost/foreach.hpp>
#include <boost/thread/mutex.hpp>
#include <glog/logging.h>
#include <inttypes.h>
#include <stdint.h>
#include <tr1/memory>

#include <iostream>
#include <string>
#include <vector>

#include "gutil/strings/substitute.h"
#include "rpc/messenger.h"
#include "util/net/sockaddr.h"
#include "util/net/socket.h"
#include "util/status.h"
#include "util/thread_util.h"

using google::protobuf::Message;
using std::tr1::shared_ptr;
using std::string;

namespace kudu {
namespace rpc {

AcceptorPool::AcceptorPool(Messenger *messenger,
                           Socket *socket, const Sockaddr &bind_address)
 : messenger_(messenger),
   socket_(socket->Release()),
   bind_address_(bind_address),
   closing_(false) {
}

AcceptorPool::~AcceptorPool() {
  Shutdown();
}

Status AcceptorPool::Init(int num_threads) {
  try {
    for (int i = 0; i < num_threads; i++) {
      threads_.push_back(shared_ptr<boost::thread>(
          new boost::thread(boost::bind(&AcceptorPool::RunThread, this))));
    }
  } catch(const boost::thread_resource_error &exception) {
    Shutdown();
    return Status::RuntimeError(string("boost thread creation error: ") +
                                exception.what());
  }
  return Status::OK();
}

void AcceptorPool::Shutdown() {
  if (Acquire_CompareAndSwap(&closing_, false, true) != false) {
    VLOG(2) << "Acceptor Pool on " << bind_address_.ToString()
            << " already shut down";
    return;
  }

  // Closing the socket will break us out of accept() if we're in it, and
  // prevent future accepts.
  WARN_NOT_OK(socket_.Shutdown(true, true),
              strings::Substitute("Could not shut down acceptor socket on $0",
                                  bind_address_.ToString()));

  BOOST_FOREACH(const shared_ptr<boost::thread>& thread, threads_) {
    CHECK_OK(ThreadJoiner(thread.get(), "acceptor thread").Join());
  }
  threads_.clear();
}

Sockaddr AcceptorPool::bind_address() const {
  return bind_address_;
}

void AcceptorPool::RunThread() {
  while (true) {
    Socket new_sock;
    Sockaddr remote;
    VLOG(2) << "calling accept() on socket " << socket_.GetFd()
            << " listening on " << bind_address_.ToString();
    Status s = socket_.Accept(&new_sock, &remote, Socket::FLAG_NONBLOCKING);
    if (!s.ok()) {
      if (Release_Load(&closing_)) {
        break;
      }
      LOG(WARNING) << "AcceptorPool: accept failed: " << s.ToString();
      continue;
    }
    s = new_sock.SetNoDelay(true);
    if (!s.ok()) {
      LOG(WARNING) << "Acceptor with remote = " << remote.ToString()
          << " failed to set TCP_NODELAY on a newly accepted socket: "
          << s.ToString();
      continue;
    }
    messenger_->RegisterInboundSocket(&new_sock, remote);
  }
  VLOG(1) << "AcceptorPool shutting down.";
}

} // namespace rpc
} // namespace kudu
