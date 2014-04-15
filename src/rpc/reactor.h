// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_RPC_REACTOR_H
#define KUDU_RPC_REACTOR_H

#include <boost/thread/thread.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/utility.hpp>
#include <ev++.h>
#include <gutil/gscoped_ptr.h>
#include <stdint.h>
#include <tr1/memory>

#include <map>
#include <string>

#include "rpc/connection.h"
#include "rpc/transfer.h"
#include "util/locks.h"
#include "util/monotime.h"
#include "util/net/socket.h"
#include "util/status.h"

namespace kudu {
namespace rpc {

typedef std::list<scoped_refptr<Connection> > conn_list_t;

class Messenger;
class MessengerBuilder;
class Reactor;

// Simple metrics information from within a reactor.
struct ReactorMetrics {
  // Number of client RPC connections currently connected.
  int32_t num_client_connections_;
  // Number of server RPC connections currently connected.
  int32_t num_server_connections_;
};

// A task which can be enqueued to run on the reactor thread.
class ReactorTask : public boost::intrusive::list_base_hook<> {
 public:
  ReactorTask();

  // Run the task. 'reactor' is guaranteed to be the current thread.
  virtual void Run(ReactorThread *reactor) = 0;

  // Abort the task, in the case that the reactor shut down before the
  // task could be processed. This may or may not run on the reactor thread
  // itself.
  //
  // The Reactor guarantees that the Reactor lock is free when this
  // method is called.
  virtual void Abort(const Status &abort_status) {}

  virtual ~ReactorTask();

 private:
  DISALLOW_COPY_AND_ASSIGN(ReactorTask);
};

// A ReactorThread is a libev event handler thread which manages I/O
// on a list of sockets.
//
// All methods in this class are _only_ called from the reactor thread itself
// except where otherwise specified. New methods should DCHECK(IsCurrentThread())
// to ensure this.
class ReactorThread {
 public:
  friend class Connection;

  // Client-side connection map.
  typedef std::tr1::unordered_map<ConnectionId, scoped_refptr<Connection>,
                                  ConnectionIdHash, ConnectionIdEqual> conn_map_t;

  ReactorThread(Reactor *reactor, const MessengerBuilder &bld);

  // This may be called from another thread.
  Status Init();

  // Collect metrics on the reactor.
  // May be called from another thread.
  Status GetMetrics(ReactorMetrics *metrics);

  // Block until the Reactor thread is shut down
  //
  // This must be called from another thread.
  void Shutdown();

  // This method is thread-safe.
  void WakeThread();

  // libev callback for handling async notifications in our epoll thread.
  void AsyncHandler(ev::async &watcher, int revents);

  // libev callback for handling timer events in our epoll thread.
  void TimerHandler(ev::timer &watcher, int revents);

  // Register an epoll timer watcher with our event loop.
  // Does not set a timeout or start it.
  void RegisterTimeout(ev::timer *watcher);

  // This may be called from another thread.
  const std::string &name() const;

  MonoTime cur_time() const;

  // This may be called from another thread.
  Reactor *reactor();

  // Return true if this reactor thread is the thread currently
  // running. Should be used in DCHECK assertions.
  bool IsCurrentThread() const;

  // Begin the process of connection negotiation.
  // Must be called from the reactor thread.
  // Deadline specifies latest time negotiation may complete before timeout.
  Status StartConnectionNegotiation(const scoped_refptr<Connection>& conn,
                                    const MonoTime& deadline);

  // Transition back from negotiating to processing requests.
  // Must be called from the reactor thread.
  void CompleteConnectionNegotiation(const scoped_refptr<Connection>& conn,
      const Status& status);

 private:
  friend class AssignOutboundCallTask;
  friend class GetMetricsTask;
  friend class RegisterConnectionTask;

  // Run the main event loop of the reactor.
  void RunThread();

  // Find or create a new connection to the given remote.
  // If such a connection already exists, returns that, otherwise creates a new one.
  // May return a bad Status if the connect() call fails.
  // The resulting connection object is managed internally by the reactor thread.
  // Deadline specifies latest time allowed for initializing the connection.
  Status FindOrStartConnection(const ConnectionId& conn_id,
                               scoped_refptr<Connection>* conn,
                               const MonoTime& deadline);

  // Shut down the given connection, removing it from the connection tracking
  // structures of this reactor.
  //
  // The connection is not explicitly deleted -- shared_ptr reference counting
  // may hold on to the object after this, but callers should assume that it
  // _may_ be deleted by this call.
  void DestroyConnection(Connection *conn, const Status &conn_status);

  // Scan any open connections for idle ones that have been idle longer than
  // connection_keepalive_time_
  void ScanIdleConnections();

  // Create a new client socket (non-blocking, NODELAY)
  static Status CreateClientSocket(Socket *sock);

  // Initiate a new connection on the given socket, setting *in_progress
  // to true if the connection is still pending upon return.
  static Status StartConnect(Socket *sock, const Sockaddr &remote, bool *in_progress);

  // Assign a new outbound call to the appropriate connection object.
  // If this fails, the call is marked failed and completed.
  void AssignOutboundCall(const std::tr1::shared_ptr<OutboundCall> &call);

  // Register a new connection.
  void RegisterConnection(const scoped_refptr<Connection>& conn);

  // Actually perform shutdown of the thread, tearing down any connections,
  // etc. This is called from within the thread.
  void ShutdownInternal();

  // Collect metrics -- called within the loop.
  void GetMetricsInternal(ReactorMetrics *metrics);

  gscoped_ptr<boost::thread> thread_;

  // our epoll object (or kqueue, etc).
  ev::dynamic_loop loop_;

  // Used by other threads to notify the reactor thread
  ev::async async_;

  // Handles the periodic timer.
  ev::timer timer_;

  // The current monotonic time.  Updated every coarse_timer_granularity_secs_.
  MonoTime cur_time_;

  // last time we did TCP timeouts.
  MonoTime last_unused_tcp_scan_;

  // Map of sockaddrs to Connection objects for outbound (client) connections.
  conn_map_t client_conns_;

  // List of current connections coming into the server.
  conn_list_t server_conns_;

  Reactor *reactor_;

  // If a connection has been idle for this much time, it is torn down.
  const MonoDelta connection_keepalive_time_;

  // Scan for idle connections on this granularity.
  const MonoDelta coarse_timer_granularity_;
};

// A Reactor manages a ReactorThread
class Reactor {
 public:
  Reactor(const std::tr1::shared_ptr<Messenger>& messenger,
          int index,
          const MessengerBuilder &bld);
  Status Init();

  // Block until the Reactor is shut down
  void Shutdown();

  ~Reactor();

  const std::string &name() const;

  // Collect metrics about the reactor.
  Status GetMetrics(ReactorMetrics *metrics);

  // Queue a new incoming connection. Takes ownership of the underlying fd from
  // 'socket', but not the Socket object itself.
  // If the reactor is already shut down, takes care of closing the socket.
  void RegisterInboundSocket(Socket *socket, const Sockaddr &remote);

  // Queue a new call to be sent. If the reactor is already shut down, marks
  // the call as failed.
  void QueueOutboundCall(const std::tr1::shared_ptr<OutboundCall> &call);

  // Schedule the given task's Run() method to be called on the
  // reactor thread.
  // If the reactor shuts down before it is run, the Abort method will be
  // called.
  // Does _not_ take ownership of 'task' -- the task should take care of
  // deleting itself after running if it is allocated on the heap.
  void ScheduleReactorTask(ReactorTask *task);

  // If the Reactor is closing, returns false.
  // Otherwise, drains the pending_tasks_ queue into the provided list.
  bool DrainTaskQueue(boost::intrusive::list<ReactorTask> *tasks);

  Messenger *messenger() const {
    return messenger_.get();
  }

  // Indicates whether the reactor is shutting down.
  //
  // This method is thread-safe.
  bool closing() const;

 private:
  friend class ReactorThread;
  typedef simple_spinlock LockType;
  mutable LockType lock_;

  // parent messenger
  std::tr1::shared_ptr<Messenger> messenger_;

  const std::string name_;

  // Whether the reactor is shutting down.
  // Guarded by lock_.
  bool closing_;

  // Tasks to be run within the reactor thread.
  // Guarded by lock_.
  boost::intrusive::list<ReactorTask> pending_tasks_; // NOLINT(build/include_what_you_use)

  ReactorThread thread_;

  DISALLOW_COPY_AND_ASSIGN(Reactor);
};

} // namespace rpc
} // namespace kudu

#endif
