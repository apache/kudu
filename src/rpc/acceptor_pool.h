// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_RPC_ACCEPTOR_POOL_H
#define KUDU_RPC_ACCEPTOR_POOL_H

#include <tr1/memory>
#include <vector>

#include "gutil/atomicops.h"
#include "util/thread.h"
#include "util/net/sockaddr.h"
#include "util/net/socket.h"
#include "util/status.h"

namespace kudu {

class Counter;
class Socket;

namespace rpc {

class Messenger;

// A pool of threads calling accept() to create new connections.
// Acceptor pool threads terminate when they notice that the messenger has been
// shut down, if Shutdown() is called, or if the pool object is destructed.
class AcceptorPool {
 public:
  // Create a new acceptor pool.  Calls socket::Release to take ownership of the
  // socket.
  AcceptorPool(Messenger *messenger,
               Socket *socket, const Sockaddr &bind_address);
  ~AcceptorPool();
  Status Init(int num_threads);
  void Shutdown();

  // Return the address that the pool is bound to.
  Sockaddr bind_address() const;

 private:
  void RunThread();

  Messenger *messenger_;
  Socket socket_;
  Sockaddr bind_address_;
  std::vector<scoped_refptr<kudu::Thread> > threads_;

  Counter* rpc_connections_accepted_;

  Atomic32 closing_;

  DISALLOW_COPY_AND_ASSIGN(AcceptorPool);
};

} // namespace rpc
} // namespace kudu
#endif
