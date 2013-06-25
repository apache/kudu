// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_RPC_MESSENGER_H
#define KUDU_RPC_MESSENGER_H

#include <gtest/gtest.h>
#include <gutil/gscoped_ptr.h>
#include <stdint.h>
#include <string>
#include <tr1/memory>
#include <vector>

#include "rpc/client_call.h"
#include "rpc/response_callback.h"
#include "rpc/server_call.h"
#include "rpc/socket.h"
#include "rpc/transfer.h"
#include "util/blocking_queue.h"
#include "util/monotime.h"
#include "util/status.h"

namespace kudu {
namespace rpc {

class AcceptorPool;
class Connection;
class Messenger;
class Reactor;
class ReactorThread;

struct AcceptorPoolInfo {
public:
  explicit AcceptorPoolInfo(const Sockaddr &bind_address)
    : bind_address_(bind_address)
  {
  }

  Sockaddr bind_address() const {
    return bind_address_;
  }

private:
  Sockaddr bind_address_;
};

//
// Used to construct a Messenger. 
//
class MessengerBuilder {
public:
  friend class Messenger;
  friend class ReactorThread;

  explicit MessengerBuilder(const std::string &name);

  // Set the length of time we will keep a TCP connection will alive with no traffic.
  MessengerBuilder &set_connection_keepalive_time(const MonoDelta &keepalive);

  // Set the number of reactor threads that will be used for sending and
  // receiving.
  MessengerBuilder &set_num_reactors(int num_reactors);

  // Set the granularity with which connections are checked for keepalive.
  MessengerBuilder &set_coarse_timer_granularity(const MonoDelta &granularity);

  // Set the maximum number of inbound messages that can be queued before we
  // start rejecting them.
  MessengerBuilder &set_service_queue_length(int service_queue_length);

  Status Build(Messenger **msgr);
  Status Build(std::tr1::shared_ptr<Messenger> *msgr);
private:
  const std::string name_;
  MonoDelta connection_keepalive_time_;
  int num_reactors_;
  MonoDelta coarse_timer_granularity_;
  size_t service_queue_length_;
};

//
// The Messenger is the core class which manages sending and receiving
// traffic.
//
class Messenger {
public:
  friend class MessengerBuilder;
  friend class Proxy;
  friend class Reactor;
  typedef std::vector<std::tr1::shared_ptr<AcceptorPool> > acceptor_vec_t;

  static const uint64_t UNKNOWN_CALL_ID = 0;

  ~Messenger();

  void Shutdown(); // stop all communication and prevent further use.

  // Add a new acceptor pool listening to the given accept address.
  // You can create any number of acceptor pools you want, including none.
  Status AddAcceptorPool(const Sockaddr &accept_addr, int num_threads);

  // get information about current acceptor pools
  void GetAcceptorInfo(std::list<AcceptorPoolInfo> *info) const;

  // Queue a call for transmission. This will pick the appropriate reactor,
  // and enqueue a task on that reactor to assign and send the call.
  void QueueOutboundCall(const std::tr1::shared_ptr<OutboundCall> &call);

  // Enqueue a call for processing on the server.
  void QueueInboundCall(gscoped_ptr<InboundCall> call);

  // Take ownership of the socket via Socket::Release
  void RegisterInboundSocket(Socket *new_socket, const Sockaddr &remote);

  BlockingQueue<InboundCall*>& service_queue() {
    return service_queue_;
  }

  std::string name() const {
    return name_;
  }

  bool closing() const {
    boost::lock_guard<boost::mutex> guard(lock_);
    return closing_;
  }

private:
  FRIEND_TEST(TestRpc, TestConnectionKeepalive);

  explicit Messenger(const MessengerBuilder &bld);

  Reactor* RemoteToReactor(const Sockaddr &remote);
  Status Init();
  void RunTimeoutThread();
  void UpdateCurTime();

  // protects closing_, acceptor_pools_, next_call_id_, cur_time_
  mutable boost::mutex lock_;

  bool closing_;

  kudu::BlockingQueue<InboundCall*> service_queue_;

  const std::string name_;

  acceptor_vec_t acceptor_pools_;

  std::vector<Reactor*> reactors_;

  DISALLOW_COPY_AND_ASSIGN(Messenger);
};

} // namespace rpc
} // namespace kudu

#endif
