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
#pragma once

#include <memory>
#include <string>
#include <utility>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace kudu {

namespace rpc {

class Messenger;
class Rpc;

// Exponential backoff with jitter anchored between 10ms and 20ms, and an upper
// bound between 2.5s and 5s.
MonoDelta ComputeExponentialBackoff(int num_attempts);

// Result status of a retriable Rpc.
//
// TODO(todd): Consider merging this with ScanRpcStatus.
struct RetriableRpcStatus {
  enum Result {
    // There was no error, i.e. the Rpc was successful.
    OK,

    // The Rpc got an error and it's not retriable.
    NON_RETRIABLE_ERROR,

    // The server couldn't be reached, i.e. there was a network error while
    // reaching the replica or a DNS resolution problem.
    SERVER_NOT_ACCESSIBLE,

    // The server received the request but it was not ready to serve it right
    // away. It might happen that the server was too busy and did not have
    // necessary resources or information to serve the request but it
    // anticipates it should be ready to serve the request really soon, so it's
    // worth retrying the request at a later time.
    SERVICE_UNAVAILABLE,

    // For rpc's that are meant only for the leader of a shared resource, when the server
    // we're interacting with is not the leader.
    REPLICA_NOT_LEADER,

    // The server doesn't know the resource we're interacting with. For instance a TabletServer
    // is not part of the config for the tablet we're trying to write to.
    RESOURCE_NOT_FOUND,

    // The authentication token supplied with the operation was found invalid
    // by the server. The token has likely expired. If so, get a new token
    // using client credentials and retry the operation with it.
    INVALID_AUTHENTICATION_TOKEN,

    // Similar to INVALID_AUTHENTICATION_TOKEN, but for authorization tokens.
    INVALID_AUTHORIZATION_TOKEN,
  };

  Result result;
  Status status;
};

// This class picks a server among a possible set of servers serving a given resource.
//
// TODO Currently this only picks the leader, though it wouldn't be unfeasible to have this
// have an enum so that it can pick any server.
template <class Server>
class ServerPicker : public RefCountedThreadSafe<ServerPicker<Server>> {
 public:
  virtual ~ServerPicker() {}

  typedef std::function<void(const Status&, Server*)> ServerPickedCallback;

  // Picks the leader among the replicas serving a resource.
  // If the leader was found, it calls the callback with Status::OK() and
  // with 'server' set to the current leader, otherwise calls the callback
  // with 'status' set to the failure reason, and 'server' set to nullptr.
  // If picking a leader takes longer than 'deadline' the callback is called with
  // Status::TimedOut().
  virtual void PickLeader(const ServerPickedCallback& callback, const MonoTime& deadline) = 0;

  // Marks a server as failed/unacessible.
  virtual void MarkServerFailed(Server *server, const Status &status) = 0;

  // Marks a server as not the leader of config serving the resource we're trying to interact with.
  virtual void MarkReplicaNotLeader(Server* replica) = 0;

  // Marks a server as not serving the resource we want.
  virtual void MarkResourceNotFound(Server *replica) = 0;
};

// Backoff strategy to use when retrying RPCs.
enum class BackoffType {
  // Backoff a small amount of jitter before retrying, roughly linear with the
  // number of RPC attempts.
  LINEAR,

  // Backoff by a bounded amount of time that is otherwise exponential with the
  // number of RPC attempts.
  EXPONENTIAL,
};

// Provides utilities for retrying failed RPCs. The default implementation adds
// a small amount of jitter before retrying, roughly linear with the number of
// RPC attempts.
class RpcRetrier {
 public:
  RpcRetrier(MonoTime deadline, std::shared_ptr<rpc::Messenger> messenger,
             BackoffType backoff)
      : attempt_num_(1),
        deadline_(deadline),
        messenger_(std::move(messenger)),
        backoff_(backoff) {
    if (deadline_.Initialized()) {
      controller_.set_deadline(deadline_);
    }
    controller_.Reset();
  }

  // Computes an appropriate backoff time for the given attempt.
  MonoDelta ComputeBackoff(int num_attempts) const;

  // Retries an RPC at some point in the near future. If 'why_status' is not OK,
  // records it as the most recent error causing the RPC to retry. This is
  // reported to the caller eventually if the RPC never succeeds.
  //
  // If the RPC's deadline expires, the callback will fire with a timeout
  // error when the RPC comes up for retrying. This is true even if the
  // deadline has already expired at the time that Retry() was called.
  //
  // Callers should ensure that 'rpc' remains alive.
  void DelayedRetry(Rpc* rpc, const Status& why_status);

  RpcController* mutable_controller() { return &controller_; }
  const RpcController& controller() const { return controller_; }

  const MonoTime& deadline() const { return deadline_; }

  const std::shared_ptr<Messenger>& messenger() const {
    return messenger_;
  }

  int attempt_num() const { return attempt_num_; }

  // Called when an RPC comes up for retrying. Actually sends the RPC.
  void DelayedRetryCb(Rpc* rpc, const Status& status);

 private:
  // The next sent rpc will be the nth attempt (indexed from 1).
  int attempt_num_;

  // If the remote end is busy, the RPC will be retried (with a small
  // delay) until this deadline is reached.
  //
  // May be uninitialized.
  MonoTime deadline_;

  // Messenger to use when sending the RPC.
  std::shared_ptr<Messenger> messenger_;

  // In case any retries have already happened, remembers the last error.
  // Errors from the server take precedence over timeout errors.
  Status last_error_;

  // RPC controller to use when sending the RPC.
  RpcController controller_;

  // The type of backoff this retrier should employ.
  const BackoffType backoff_;

  DISALLOW_COPY_AND_ASSIGN(RpcRetrier);
};

// Encapsulates an in-flight remote procedure call to some server, employing
// the provided backoff type to retry when necessary.
class Rpc {
 public:
  Rpc(const MonoTime& deadline,
      std::shared_ptr<rpc::Messenger> messenger,
      BackoffType backoff)
      : retrier_(deadline, std::move(messenger), backoff) {}
  virtual ~Rpc() {}

  // Asynchronously sends the RPC to the remote end.
  //
  // Subclasses should use SendRpcCb() below as the callback function.
  virtual void SendRpc() = 0;

  // Returns a string representation of the RPC.
  virtual std::string ToString() const = 0;

  // Returns the number of times this RPC has been sent. Should always be at
  // least one.
  int num_attempts() const { return retrier().attempt_num(); }

 protected:
  // Used to retry some failed RPCs.
  const RpcRetrier& retrier() const { return retrier_; }
  RpcRetrier* mutable_retrier() { return &retrier_; }

 private:
  friend class RpcRetrier;

  // Callback for SendRpc(). If 'status' is not OK, something failed before the
  // RPC was sent.
  virtual void SendRpcCb(const Status& status) = 0;

  // Used to retry some failed RPCs.
  RpcRetrier retrier_;

  DISALLOW_COPY_AND_ASSIGN(Rpc);
};

} // namespace rpc
} // namespace kudu

