// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef KUDU_RPC_CONNECTION_H
#define KUDU_RPC_CONNECTION_H

#include <cstdint>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>

#include <boost/intrusive/list.hpp>
#include <boost/optional/optional.hpp>
#include <ev++.h>
#include <glog/logging.h>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/connection_id.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/remote_user.h"
#include "kudu/rpc/transfer.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/object_pool.h"
#include "kudu/util/status.h"

namespace kudu {

namespace rpc {

class DumpRunningRpcsRequestPB;
class InboundCall;
class OutboundCall;
class RpcConnectionPB;
class ReactorThread;
class RpczStore;
enum class CredentialsPolicy;

//
// A connection between an endpoint and us.
//
// Inbound connections are created by AcceptorPools, which eventually schedule
// RegisterConnection() to be called from the reactor thread.
//
// Outbound connections are created by the Reactor thread in order to service
// outbound calls.
//
// Once a Connection is created, it can be used both for sending messages and
// receiving them, but any given connection is explicitly a client or server.
// If a pair of servers are making bidirectional RPCs, they will use two separate
// TCP connections (and Connection objects).
//
// This class is not fully thread-safe.  It is accessed only from the context of a
// single ReactorThread except where otherwise specified.
//
class Connection : public RefCountedThreadSafe<Connection> {
 public:
  enum Direction {
    // This host is sending calls via this connection.
    CLIENT,
    // This host is receiving calls via this connection.
    SERVER
  };

  // Create a new Connection.
  // reactor_thread: the reactor that owns us.
  // remote: the address of the remote end
  // socket: the socket to take ownership of.
  // direction: whether we are the client or server side
  Connection(ReactorThread *reactor_thread,
             Sockaddr remote,
             std::unique_ptr<Socket> socket,
             Direction direction,
             CredentialsPolicy policy = CredentialsPolicy::ANY_CREDENTIALS);

  // Set underlying socket to non-blocking (or blocking) mode.
  Status SetNonBlocking(bool enabled);

  // Register our socket with an epoll loop.  We will only ever be registered in
  // one epoll loop at a time.
  void EpollRegister(ev::loop_ref& loop);

  ~Connection();

  MonoTime last_activity_time() const {
    return last_activity_time_;
  }

  // Returns true if we are not in the process of receiving or sending a
  // message, and we have no outstanding calls.
  bool Idle() const;

  // Fail any calls which are currently queued or awaiting response.
  // Prohibits any future calls (they will be failed immediately with this
  // same Status).
  void Shutdown(const Status& status,
                std::unique_ptr<ErrorStatusPB> rpc_error = {});

  // Queue a new call to be made. If the queueing fails, the call will be
  // marked failed. The caller is expected to check if 'call' has been cancelled
  // before making the call.
  // Takes ownership of the 'call' object regardless of whether it succeeds or fails.
  void QueueOutboundCall(std::shared_ptr<OutboundCall> call);

  // Queue a call response back to the client on the server side.
  //
  // This may be called from a non-reactor thread.
  void QueueResponseForCall(gscoped_ptr<InboundCall> call);

  // Cancel an outbound call by removing any reference to it by CallAwaitingResponse
  // in 'awaiting_responses_'.
  void CancelOutboundCall(const std::shared_ptr<OutboundCall> &call);

  // The address of the remote end of the connection.
  const Sockaddr &remote() const { return remote_; }

  // Set the user credentials for an outbound connection.
  void set_outbound_connection_id(ConnectionId conn_id) {
    DCHECK_EQ(direction_, CLIENT);
    DCHECK(!outbound_connection_id_);
    outbound_connection_id_ = std::move(conn_id);
  }

  // Get the user credentials which will be used to log in.
  const ConnectionId& outbound_connection_id() const {
    DCHECK_EQ(direction_, CLIENT);
    DCHECK(outbound_connection_id_);
    return *outbound_connection_id_;
  }

  bool is_confidential() const {
    return is_confidential_;
  }

  // Set/unset the 'confidentiality' property for this connection.
  void set_confidential(bool is_confidential);

  // Credentials policy to start connection negotiation.
  CredentialsPolicy credentials_policy() const { return credentials_policy_; }

  // Whether the connection satisfies the specified credentials policy.
  //
  // NOTE: The policy is set prior to connection negotiation, and the actual
  //       authentication credentials used for connection negotiation might
  //       effectively make the connection to satisfy a stronger policy.
  //       An example: the credentials policy for the connection was set to
  //       ANY_CREDENTIALS, but since the authn token was not available
  //       at the time of negotiation, the primary credentials were used, making
  //       the connection de facto satisfying the PRIMARY_CREDENTIALS policy.
  bool SatisfiesCredentialsPolicy(CredentialsPolicy policy) const;

  RpczStore* rpcz_store();

  // libev callback when data is available to read.
  void ReadHandler(ev::io &watcher, int revents);

  // libev callback when we may write to the socket.
  void WriteHandler(ev::io &watcher, int revents);

  // Safe to be called from other threads.
  std::string ToString() const;

  Direction direction() const { return direction_; }

  Socket* socket() { return socket_.get(); }

  // Go through the process of transferring control of the underlying socket back to the Reactor.
  void CompleteNegotiation(Status negotiation_status,
                           std::unique_ptr<ErrorStatusPB> rpc_error);

  // Indicate that negotiation is complete and that the Reactor is now in control of the socket.
  void MarkNegotiationComplete();

  Status DumpPB(const DumpRunningRpcsRequestPB& req,
                RpcConnectionPB* resp);

  ReactorThread* reactor_thread() const { return reactor_thread_; }

  std::unique_ptr<Socket> release_socket() {
    return std::move(socket_);
  }

  void adopt_socket(std::unique_ptr<Socket> socket) {
    socket_ = std::move(socket);
  }

  void set_remote_features(std::set<RpcFeatureFlag> remote_features) {
    remote_features_ = std::move(remote_features);
  }

  void set_remote_user(RemoteUser user) {
    DCHECK_EQ(direction_, SERVER);
    remote_user_ = std::move(user);
  }

  const RemoteUser& remote_user() const {
    DCHECK_EQ(direction_, SERVER);
    return remote_user_;
  }

  // Whether the connection is scheduled for shutdown.
  bool scheduled_for_shutdown() const {
    DCHECK_EQ(direction_, CLIENT);
    return scheduled_for_shutdown_;
  }

  // Mark the connection as scheduled to be shut down. Reactor does not dispatch
  // new calls on such a connection.
  void set_scheduled_for_shutdown() {
    DCHECK_EQ(direction_, CLIENT);
    scheduled_for_shutdown_ = true;
  }

 private:
  friend struct CallAwaitingResponse;
  friend class QueueTransferTask;
  friend struct CallTransferCallbacks;
  friend struct ResponseTransferCallbacks;

  // A call which has been fully sent to the server, which we're waiting for
  // the server to process. This is used on the client side only.
  struct CallAwaitingResponse {
    ~CallAwaitingResponse();

    // Notification from libev that the call has timed out.
    void HandleTimeout(ev::timer &watcher, int revents);

    Connection *conn;
    std::shared_ptr<OutboundCall> call;
    ev::timer timeout_timer;

    // We time out RPC calls in two stages. This is set to the amount of timeout
    // remaining after the next timeout fires. See Connection::QueueOutboundCall().
    double remaining_timeout;
  };

  typedef std::unordered_map<uint64_t, CallAwaitingResponse*> car_map_t;
  typedef std::unordered_map<uint64_t, InboundCall*> inbound_call_map_t;

  // Returns the next valid (positive) sequential call ID by incrementing a counter
  // and ensuring we roll over from INT32_MAX to 0.
  // Negative numbers are reserved for special purposes.
  int32_t GetNextCallId() {
    int32_t call_id = next_call_id_;
    if (PREDICT_FALSE(next_call_id_ == std::numeric_limits<int32_t>::max())) {
      next_call_id_ = 0;
    } else {
      next_call_id_++;
    }
    return call_id;
  }

  // An incoming packet has completed transferring on the server side.
  // This parses the call and delivers it into the call queue.
  void HandleIncomingCall(gscoped_ptr<InboundTransfer> transfer);

  // An incoming packet has completed on the client side. This parses the
  // call response, looks up the CallAwaitingResponse, and calls the
  // client callback.
  void HandleCallResponse(gscoped_ptr<InboundTransfer> transfer);

  // The given CallAwaitingResponse has elapsed its user-defined timeout.
  // Set it to Failed.
  void HandleOutboundCallTimeout(CallAwaitingResponse *car);

  // Queue a transfer for sending on this connection.
  // We will take ownership of the transfer.
  // This must be called from the reactor thread.
  void QueueOutbound(gscoped_ptr<OutboundTransfer> transfer);

  // Internal test function for injecting cancellation request when 'call'
  // reaches state specified in 'FLAGS_rpc_inject_cancellation_state'.
  void MaybeInjectCancellation(const std::shared_ptr<OutboundCall> &call);

  // The reactor thread that created this connection.
  ReactorThread* const reactor_thread_;

  // The remote address we're talking to.
  const Sockaddr remote_;

  // The socket we're communicating on.
  std::unique_ptr<Socket> socket_;

  // The ConnectionId that serves as a key into the client connection map
  // within this reactor. Only set in the case of outbound connections.
  boost::optional<ConnectionId> outbound_connection_id_;

  // The authenticated remote user (if this is an inbound connection on the server).
  RemoteUser remote_user_;

  // whether we are client or server
  Direction direction_;

  // The last time we read or wrote from the socket.
  MonoTime last_activity_time_;

  // the inbound transfer, if any
  gscoped_ptr<InboundTransfer> inbound_;

  // notifies us when our socket is writable.
  ev::io write_io_;

  // notifies us when our socket is readable.
  ev::io read_io_;

  // Set to true when the connection is registered on a loop.
  // This is used for a sanity check in the destructor that we are properly
  // un-registered before shutting down.
  bool is_epoll_registered_;

  // waiting to be sent
  boost::intrusive::list<OutboundTransfer> outbound_transfers_; // NOLINT(*)

  // Calls which have been sent and are now waiting for a response.
  car_map_t awaiting_response_;

  // Calls which have been received on the server and are currently
  // being handled.
  inbound_call_map_t calls_being_handled_;

  // the next call ID to use
  int32_t next_call_id_;

  // Starts as Status::OK, gets set to a shutdown status upon Shutdown().
  Status shutdown_status_;

  // RPC features supported by the remote end of the connection.
  std::set<RpcFeatureFlag> remote_features_;

  // Pool from which CallAwaitingResponse objects are allocated.
  // Also a funny name.
  ObjectPool<CallAwaitingResponse> car_pool_;
  typedef ObjectPool<CallAwaitingResponse>::scoped_ptr scoped_car;

  // The credentials policy to use for connection negotiation. It defines which
  // type of user credentials used to negotiate a connection. The actual type of
  // credentials used for authentication during the negotiation process depends
  // on the credentials availability, but the result credentials guaranteed to
  // always satisfy the specified credentials policy. In other words, the actual
  // type of credentials used for connection negotiation might effectively make
  // the connection to satisfy a stronger/narrower policy.
  //
  // An example:
  //   The credentials policy for the connection was set to ANY_CREDENTIALS,
  //   but since no secondary credentials (such authn token) were available
  //   at the time of negotiation, the primary credentials were used,making the
  //   connection satisfying the PRIMARY_CREDENTIALS policy de facto.
  const CredentialsPolicy credentials_policy_;

  // Whether we completed connection negotiation.
  bool negotiation_complete_;

  // Whether it's OK to pass confidential information over the connection.
  // For example, an encrypted (but not necessarily authenticated) connection
  // is considered confidential.
  bool is_confidential_;

  // Whether the connection is scheduled for shutdown.
  bool scheduled_for_shutdown_;
};

} // namespace rpc
} // namespace kudu

#endif
