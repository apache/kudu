// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_RPC_MESSENGER_H
#define KUDU_RPC_MESSENGER_H

#include <boost/thread/thread.hpp>
#include <gtest/gtest.h>
#include <gutil/gscoped_ptr.h>
#include <stdint.h>
#include <tr1/memory>

#include <list>
#include <string>
#include <vector>

#include "gutil/ref_counted.h"
#include "rpc/response_callback.h"
#include "util/metrics.h"
#include "util/monotime.h"
#include "util/net/sockaddr.h"
#include "util/status.h"

namespace kudu {

class FutureTask;
class Socket;
class TaskExecutor;

namespace rpc {

class AcceptorPool;
class InboundCall;
class Messenger;
class OutboundCall;
class Reactor;
class ReactorThread;
class RpcService;

struct AcceptorPoolInfo {
 public:
  explicit AcceptorPoolInfo(const Sockaddr &bind_address)
    : bind_address_(bind_address) {
  }

  Sockaddr bind_address() const {
    return bind_address_;
  }

 private:
  Sockaddr bind_address_;
};

// Used to construct a Messenger.
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

  // Set the number of connection-negotiation threads that will be used to handle the
  // blocking connection-negotiation step.
  MessengerBuilder &set_negotiation_threads(int num_negotiation_threads);

  // Set the granularity with which connections are checked for keepalive.
  MessengerBuilder &set_coarse_timer_granularity(const MonoDelta &granularity);

  // Set metric context for use by RPC systems.
  MessengerBuilder &set_metric_context(const MetricContext& metric_ctx);

  Status Build(std::tr1::shared_ptr<Messenger> *msgr);

 private:
  Status Build(Messenger **msgr);
  const std::string name_;
  MonoDelta connection_keepalive_time_;
  int num_reactors_;
  int num_negotiation_threads_;
  MonoDelta coarse_timer_granularity_;
  gscoped_ptr<MetricContext> metric_ctx_;
};

// A Messenger is a container for the reactor threads which run event loops
// for the RPC service. If the process is a server, a Messenger can also have
// one or more attached AcceptorPools which accept RPC connections. In this case,
// calls received over the connection are enqueued into the messenger's service_queue
// for processing by a ServicePool.
//
// Users do not typically interact with the Messenger directly except to create
// one as a singleton, and then make calls using Proxy objects.
//
// See rpc-test.cc and rpc-bench.cc for example usages.
class Messenger {
 public:
  friend class MessengerBuilder;
  friend class Proxy;
  friend class Reactor;
  typedef std::vector<std::tr1::shared_ptr<AcceptorPool> > acceptor_vec_t;

  static const uint64_t UNKNOWN_CALL_ID = 0;

  ~Messenger();

  // Stop all communication and prevent further use.
  // It's not required to call this -- dropping the shared_ptr provided
  // from MessengerBuilder::Build will automatically call this method.
  void Shutdown();

  // Add a new acceptor pool listening to the given accept address.
  // You can create any number of acceptor pools you want, including none.
  //
  // The created pool is returned in *pool. The Messenger also retains
  // a reference to the pool, so the caller may safely drop this reference
  // and the pool will remain running.
  Status AddAcceptorPool(const Sockaddr &accept_addr, int num_threads,
                         std::tr1::shared_ptr<AcceptorPool>* pool);

  // Register a new RpcService to handle inbound requests.
  Status RegisterService(const std::string& service_name,
                         const scoped_refptr<RpcService>& service);

  // Unregister currently-registered RpcService.
  Status UnregisterService(const std::string& service_name);

  // Queue a call for transmission. This will pick the appropriate reactor,
  // and enqueue a task on that reactor to assign and send the call.
  void QueueOutboundCall(const std::tr1::shared_ptr<OutboundCall> &call);

  // Enqueue a call for processing on the server.
  void QueueInboundCall(gscoped_ptr<InboundCall> call);

  // Take ownership of the socket via Socket::Release
  void RegisterInboundSocket(Socket *new_socket, const Sockaddr &remote);

  TaskExecutor* negotiation_executor() const { return negotiation_executor_.get(); }

  std::string name() const {
    return name_;
  }

  bool closing() const {
    boost::lock_guard<boost::mutex> guard(lock_);
    return closing_;
  }

  MetricContext* metric_context() const { return metric_ctx_.get(); }

 private:
  FRIEND_TEST(TestRpc, TestConnectionKeepalive);

  explicit Messenger(const MessengerBuilder &bld);

  Reactor* RemoteToReactor(const Sockaddr &remote);
  Status Init();
  void RunTimeoutThread();
  void UpdateCurTime();

  // Called by external-facing shared_ptr when the user no longer holds
  // any references. See 'retain_self_' for more info.
  void AllExternalReferencesDropped();

  const std::string name_;

  // protects closing_, acceptor_pools_, rpc_service_.
  mutable boost::mutex lock_;

  bool closing_;

  // Pools which are listening on behalf of this messenger.
  // Note that the user may have called Shutdown() on one of these
  // pools, so even though we retain the reference, it may no longer
  // be listening.
  acceptor_vec_t acceptor_pools_;

  // RPC service that handles inbound requests.
  // TODO: Allow multiple services to be registered.
  scoped_refptr<RpcService> rpc_service_;

  std::vector<Reactor*> reactors_;

  gscoped_ptr<TaskExecutor> negotiation_executor_;

  gscoped_ptr<MetricContext> metric_ctx_;

  // The ownership of the Messenger object is somewhat subtle. The pointer graph
  // looks like this:
  //
  //    [User Code ]             |      [ Internal code ]
  //                             |
  //     shared_ptr[1]           |
  //         |                   |
  //         v
  //      Messenger    <------------ shared_ptr[2] --- Reactor
  //       ^    |       ----------- bare pointer --> Reactor
  //        \__/
  //     shared_ptr[2]
  //     (retain_self_)
  //
  // shared_ptr[1] instances use Messenger::AllExternalReferencesDropped()
  //   as a deleter.
  // shared_ptr[2] are "traditional" shared_ptrs which call 'delete' on the
  //   object.
  //
  // The teardown sequence is as follows:
  // Option 1): User calls "Shutdown()" explicitly:
  //  - Messenger::Shutdown tells Reactors to shut down
  //  - When each reactor thread finishes, it drops its shared_ptr[2]
  //  - the Messenger::retain_self instance remains, keeping the Messenger
  //    alive.
  //  - The user eventually drops its shared_ptr[1], which calls
  //    Messenger::AllExternalReferencesDropped. This drops retain_self_
  //    and results in object destruction.
  // Option 2): User drops all of its shared_ptr[1] references
  //  - Though the Reactors still reference the Messenger, AllExternalReferencesDropped
  //    will get called, which triggers Messenger::Shutdown.
  //  - AllExternalReferencesDropped drops retain_self_, so the only remaining
  //    references are from Reactor threads. But the reactor threads are shutting down.
  //  - When the last Reactor thread dies, there will be no more shared_ptr[1] references
  //    and the Messenger will be destroyed.
  //
  // The main goal of all of this confusion is that the reactor threads need to be able
  // to shut down asynchronously, and we need to keep the Messenger alive until they
  // do so. So, handing out a normal shared_ptr to users would force the Messenger
  // destructor to Join() the reactor threads, which causes a problem if the user
  // tries to destruct the Messenger from within a Reactor thread itself.
  std::tr1::shared_ptr<Messenger> retain_self_;

  DISALLOW_COPY_AND_ASSIGN(Messenger);
};

} // namespace rpc
} // namespace kudu

#endif
