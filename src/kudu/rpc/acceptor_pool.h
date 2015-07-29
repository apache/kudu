// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#ifndef KUDU_RPC_ACCEPTOR_POOL_H
#define KUDU_RPC_ACCEPTOR_POOL_H

#include <tr1/memory>
#include <vector>

#include "kudu/gutil/atomicops.h"
#include "kudu/util/thread.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/status.h"

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
  // 'socket' must be already bound, but should not yet be listening.
  AcceptorPool(Messenger *messenger,
               Socket *socket, const Sockaddr &bind_address);
  ~AcceptorPool();

  // Start listening and accepting connections.
  Status Start(int num_threads);
  void Shutdown();

  // Return the address that the pool is bound to. If the port is specified as
  // 0, then this will always return port 0.
  Sockaddr bind_address() const;

  // Return the address that the pool is bound to. This only works while the
  // socket is open, and if the specified port is 0 then this will return the
  // actual port that was bound.
  Status GetBoundAddress(Sockaddr* addr) const;

 private:
  void RunThread();

  Messenger *messenger_;
  Socket socket_;
  Sockaddr bind_address_;
  std::vector<scoped_refptr<kudu::Thread> > threads_;

  scoped_refptr<Counter> rpc_connections_accepted_;

  Atomic32 closing_;

  DISALLOW_COPY_AND_ASSIGN(AcceptorPool);
};

} // namespace rpc
} // namespace kudu
#endif
