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

#include "kudu/rpc/connection.h"

#include <algorithm>
#include <boost/intrusive/list.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>
#include <set>
#include <stdint.h>
#include <string>
#include <vector>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/auth_store.h"
#include "kudu/rpc/rpc_introspection.pb.h"
#include "kudu/rpc/constants.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/reactor.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/sasl_client.h"
#include "kudu/rpc/sasl_server.h"
#include "kudu/rpc/transfer.h"
#include "kudu/util/debug-util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/trace.h"

using std::function;
using std::includes;
using std::set;
using std::shared_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace rpc {

///
/// Connection
///
Connection::Connection(ReactorThread *reactor_thread, Sockaddr remote,
                       int socket, Direction direction)
    : reactor_thread_(reactor_thread),
      socket_(socket),
      remote_(std::move(remote)),
      direction_(direction),
      last_activity_time_(MonoTime::Now()),
      is_epoll_registered_(false),
      next_call_id_(1),
      sasl_client_(kSaslAppName, socket),
      sasl_server_(kSaslAppName, socket),
      negotiation_complete_(false) {}

Status Connection::SetNonBlocking(bool enabled) {
  return socket_.SetNonBlocking(enabled);
}

void Connection::EpollRegister(ev::loop_ref& loop) {
  DCHECK(reactor_thread_->IsCurrentThread());
  DVLOG(4) << "Registering connection for epoll: " << ToString();
  write_io_.set(loop);
  write_io_.set(socket_.GetFd(), ev::WRITE);
  write_io_.set<Connection, &Connection::WriteHandler>(this);
  if (direction_ == CLIENT && negotiation_complete_) {
    write_io_.start();
  }
  read_io_.set(loop);
  read_io_.set(socket_.GetFd(), ev::READ);
  read_io_.set<Connection, &Connection::ReadHandler>(this);
  read_io_.start();
  is_epoll_registered_ = true;
}

Connection::~Connection() {
  // Must clear the outbound_transfers_ list before deleting.
  CHECK(outbound_transfers_.begin() == outbound_transfers_.end());

  // It's crucial that the connection is Shutdown first -- otherwise
  // our destructor will end up calling read_io_.stop() and write_io_.stop()
  // from a possibly non-reactor thread context. This can then make all
  // hell break loose with libev.
  CHECK(!is_epoll_registered_);
}

bool Connection::Idle() const {
  DCHECK(reactor_thread_->IsCurrentThread());
  // check if we're in the middle of receiving something
  InboundTransfer *transfer = inbound_.get();
  if (transfer && (transfer->TransferStarted())) {
    return false;
  }
  // check if we still need to send something
  if (!outbound_transfers_.empty()) {
    return false;
  }
  // can't kill a connection if calls are waiting response
  if (!awaiting_response_.empty()) {
    return false;
  }

  if (!calls_being_handled_.empty()) {
    return false;
  }

  // We are not idle if we are in the middle of connection negotiation.
  if (!negotiation_complete_) {
    return false;
  }

  return true;
}

void Connection::Shutdown(const Status &status) {
  DCHECK(reactor_thread_->IsCurrentThread());
  shutdown_status_ = status.CloneAndPrepend("RPC connection failed");

  if (inbound_ && inbound_->TransferStarted()) {
    double secs_since_active =
        (reactor_thread_->cur_time() - last_activity_time_).ToSeconds();
    LOG(WARNING) << "Shutting down connection " << ToString() << " with pending inbound data ("
                 << inbound_->StatusAsString() << ", last active "
                 << HumanReadableElapsedTime::ToShortString(secs_since_active)
                 << " ago, status=" << status.ToString() << ")";
  }

  // Clear any calls which have been sent and were awaiting a response.
  for (const car_map_t::value_type &v : awaiting_response_) {
    CallAwaitingResponse *c = v.second;
    if (c->call) {
      c->call->SetFailed(status);
    }
    // And we must return the CallAwaitingResponse to the pool
    car_pool_.Destroy(c);
  }
  awaiting_response_.clear();

  // Clear any outbound transfers.
  while (!outbound_transfers_.empty()) {
    OutboundTransfer *t = &outbound_transfers_.front();
    outbound_transfers_.pop_front();
    delete t;
  }

  read_io_.stop();
  write_io_.stop();
  is_epoll_registered_ = false;
  WARN_NOT_OK(socket_.Close(), "Error closing socket");
}

void Connection::QueueOutbound(gscoped_ptr<OutboundTransfer> transfer) {
  DCHECK(reactor_thread_->IsCurrentThread());

  if (!shutdown_status_.ok()) {
    // If we've already shut down, then we just need to abort the
    // transfer rather than bothering to queue it.
    transfer->Abort(shutdown_status_);
    return;
  }

  DVLOG(3) << "Queueing transfer: " << transfer->HexDump();

  outbound_transfers_.push_back(*transfer.release());

  if (negotiation_complete_ && !write_io_.is_active()) {
    // If we weren't currently in the middle of sending anything,
    // then our write_io_ interest is stopped. Need to re-start it.
    // Only do this after connection negotiation is done doing its work.
    write_io_.start();
  }
}

Connection::CallAwaitingResponse::~CallAwaitingResponse() {
  DCHECK(conn->reactor_thread_->IsCurrentThread());
}

void Connection::CallAwaitingResponse::HandleTimeout(ev::timer &watcher, int revents) {
  if (remaining_timeout > 0) {
    if (watcher.remaining() < -1.0) {
      LOG(WARNING) << "RPC call timeout handler was delayed by "
                   << -watcher.remaining() << "s! This may be due to a process-wide "
                   << "pause such as swapping, logging-related delays, or allocator lock "
                   << "contention. Will allow an additional "
                   << remaining_timeout << "s for a response.";
    }

    watcher.set(remaining_timeout, 0);
    watcher.start();
    remaining_timeout = 0;
    return;
  }

  conn->HandleOutboundCallTimeout(this);
}

void Connection::HandleOutboundCallTimeout(CallAwaitingResponse *car) {
  DCHECK(reactor_thread_->IsCurrentThread());
  DCHECK(car->call);
  // The timeout timer is stopped by the car destructor exiting Connection::HandleCallResponse()
  DCHECK(!car->call->IsFinished());

  // Mark the call object as failed.
  car->call->SetTimedOut();

  // Drop the reference to the call. If the original caller has moved on after
  // seeing the timeout, we no longer need to hold onto the allocated memory
  // from the request.
  car->call.reset();

  // We still leave the CallAwaitingResponse in the map -- this is because we may still
  // receive a response from the server, and we don't want a spurious log message
  // when we do finally receive the response. The fact that CallAwaitingResponse::call
  // is a NULL pointer indicates to the response processing code that the call
  // already timed out.
}

// Callbacks after sending a call on the wire.
// This notifies the OutboundCall object to change its state to SENT once it
// has been fully transmitted.
struct CallTransferCallbacks : public TransferCallbacks {
 public:
  explicit CallTransferCallbacks(shared_ptr<OutboundCall> call)
      : call_(std::move(call)) {}

  virtual void NotifyTransferFinished() OVERRIDE {
    // TODO: would be better to cancel the transfer while it is still on the queue if we
    // timed out before the transfer started, but there is still a race in the case of
    // a partial send that we have to handle here
    if (call_->IsFinished()) {
      DCHECK(call_->IsTimedOut());
    } else {
      call_->SetSent();
    }
    delete this;
  }

  virtual void NotifyTransferAborted(const Status &status) OVERRIDE {
    VLOG(1) << "Transfer of RPC call " << call_->ToString() << " aborted: "
            << status.ToString();
    delete this;
  }

 private:
  shared_ptr<OutboundCall> call_;
};

void Connection::QueueOutboundCall(const shared_ptr<OutboundCall> &call) {
  DCHECK(call);
  DCHECK_EQ(direction_, CLIENT);
  DCHECK(reactor_thread_->IsCurrentThread());

  if (PREDICT_FALSE(!shutdown_status_.ok())) {
    // Already shutdown
    call->SetFailed(shutdown_status_);
    return;
  }

  // At this point the call has a serialized request, but no call header, since we haven't
  // yet assigned a call ID.
  DCHECK(!call->call_id_assigned());

  // Assign the call ID.
  int32_t call_id = GetNextCallId();
  call->set_call_id(call_id);

  // Serialize the actual bytes to be put on the wire.
  slices_tmp_.clear();
  Status s = call->SerializeTo(&slices_tmp_);
  if (PREDICT_FALSE(!s.ok())) {
    call->SetFailed(s);
    return;
  }

  call->SetQueued();

  scoped_car car(car_pool_.make_scoped_ptr(car_pool_.Construct()));
  car->conn = this;
  car->call = call;

  // Set up the timeout timer.
  const MonoDelta &timeout = call->controller()->timeout();
  if (timeout.Initialized()) {
    reactor_thread_->RegisterTimeout(&car->timeout_timer);
    car->timeout_timer.set<CallAwaitingResponse, // NOLINT(*)
                           &CallAwaitingResponse::HandleTimeout>(car.get());

    // For calls with a timeout of at least 500ms, we actually run the timeout
    // handler in two stages. The first timeout fires with a timeout 10% less
    // than the user-specified one. It then schedules a second timeout for the
    // remaining amount of time.
    //
    // The purpose of this two-stage timeout is to be more robust when the client
    // has some process-wide pause, such as lock contention in tcmalloc, or a
    // reactor callback that blocks in glog. Consider the following case:
    //
    // T = 0s        user issues an RPC with 5 second timeout
    // T = 0.5s - 6s   process is blocked
    // T = 6s        process unblocks, and the timeout fires (1s late)
    //
    // Without the two-stage timeout, we would determine that the call had timed out,
    // even though it's likely that the response is waiting on our TCP socket.
    // With the two-stage timeout, we'll end up with:
    //
    // T = 0s           user issues an RPC with 5 second timeout
    // T = 0.5s - 6s    process is blocked
    // T = 6s           process unblocks, and the first-stage timeout fires (1.5s late)
    // T = 6s - 6.200s  time for the client to read the response which is waiting
    // T = 6.200s       if the response was not actually available, we'll time out here
    //
    // We don't bother with this logic for calls with very short timeouts - assumedly
    // a user setting such a short RPC timeout is well equipped to handle one.
    double time = timeout.ToSeconds();
    if (time >= 0.5) {
      car->remaining_timeout = time * 0.1;
      time -= car->remaining_timeout;
    } else {
      car->remaining_timeout = 0;
    }

    car->timeout_timer.set(time, 0);
    car->timeout_timer.start();
  }

  TransferCallbacks *cb = new CallTransferCallbacks(call);
  awaiting_response_[call_id] = car.release();
  QueueOutbound(gscoped_ptr<OutboundTransfer>(
      OutboundTransfer::CreateForCallRequest(call_id, slices_tmp_, cb)));
}

// Callbacks for sending an RPC call response from the server.
// This takes ownership of the InboundCall object so that, once it has
// been responded to, we can free up all of the associated memory.
struct ResponseTransferCallbacks : public TransferCallbacks {
 public:
  ResponseTransferCallbacks(gscoped_ptr<InboundCall> call,
                            Connection *conn) :
    call_(std::move(call)),
    conn_(conn)
  {}

  ~ResponseTransferCallbacks() {
    // Remove the call from the map.
    InboundCall *call_from_map = EraseKeyReturnValuePtr(
      &conn_->calls_being_handled_, call_->call_id());
    DCHECK_EQ(call_from_map, call_.get());
  }

  virtual void NotifyTransferFinished() OVERRIDE {
    delete this;
  }

  virtual void NotifyTransferAborted(const Status &status) OVERRIDE {
    LOG(WARNING) << "Connection torn down before " <<
      call_->ToString() << " could send its response";
    delete this;
  }

 private:
  gscoped_ptr<InboundCall> call_;
  Connection *conn_;
};

// Reactor task which puts a transfer on the outbound transfer queue.
class QueueTransferTask : public ReactorTask {
 public:
  QueueTransferTask(gscoped_ptr<OutboundTransfer> transfer,
                    Connection *conn)
    : transfer_(std::move(transfer)),
      conn_(conn)
  {}

  virtual void Run(ReactorThread *thr) OVERRIDE {
    conn_->QueueOutbound(std::move(transfer_));
    delete this;
  }

  virtual void Abort(const Status &status) OVERRIDE {
    transfer_->Abort(status);
    delete this;
  }

 private:
  gscoped_ptr<OutboundTransfer> transfer_;
  Connection *conn_;
};

void Connection::QueueResponseForCall(gscoped_ptr<InboundCall> call) {
  // This is usually called by the IPC worker thread when the response
  // is set, but in some circumstances may also be called by the
  // reactor thread (e.g. if the service has shut down)

  DCHECK_EQ(direction_, SERVER);

  // If the connection is torn down, then the QueueOutbound() call that
  // eventually runs in the reactor thread will take care of calling
  // ResponseTransferCallbacks::NotifyTransferAborted.

  std::vector<Slice> slices;
  call->SerializeResponseTo(&slices);

  TransferCallbacks *cb = new ResponseTransferCallbacks(std::move(call), this);
  // After the response is sent, can delete the InboundCall object.
  // We set a dummy call ID and required feature set, since these are not needed
  // when sending responses.
  gscoped_ptr<OutboundTransfer> t(OutboundTransfer::CreateForCallResponse(slices, cb));

  QueueTransferTask *task = new QueueTransferTask(std::move(t), this);
  reactor_thread_->reactor()->ScheduleReactorTask(task);
}

void Connection::set_user_credentials(const UserCredentials &user_credentials) {
  user_credentials_.CopyFrom(user_credentials);
}

RpczStore* Connection::rpcz_store() {
  return reactor_thread_->reactor()->messenger()->rpcz_store();
}

void Connection::ReadHandler(ev::io &watcher, int revents) {
  DCHECK(reactor_thread_->IsCurrentThread());

  DVLOG(3) << ToString() << " ReadHandler(revents=" << revents << ")";
  if (revents & EV_ERROR) {
    reactor_thread_->DestroyConnection(this, Status::NetworkError(ToString() +
                                     ": ReadHandler encountered an error"));
    return;
  }
  last_activity_time_ = reactor_thread_->cur_time();

  while (true) {
    if (!inbound_) {
      inbound_.reset(new InboundTransfer());
    }
    Status status = inbound_->ReceiveBuffer(socket_);
    if (PREDICT_FALSE(!status.ok())) {
      if (status.posix_code() == ESHUTDOWN) {
        VLOG(1) << ToString() << " shut down by remote end.";
      } else {
        LOG(WARNING) << ToString() << " recv error: " << status.ToString();
      }
      reactor_thread_->DestroyConnection(this, status);
      return;
    }
    if (!inbound_->TransferFinished()) {
      DVLOG(3) << ToString() << ": read is not yet finished yet.";
      return;
    }
    DVLOG(3) << ToString() << ": finished reading " << inbound_->data().size() << " bytes";

    if (direction_ == CLIENT) {
      HandleCallResponse(std::move(inbound_));
    } else if (direction_ == SERVER) {
      HandleIncomingCall(std::move(inbound_));
    } else {
      LOG(FATAL) << "Invalid direction: " << direction_;
    }

    // TODO: it would seem that it would be good to loop around and see if
    // there is more data on the socket by trying another recv(), but it turns
    // out that it really hurts throughput to do so. A better approach
    // might be for each InboundTransfer to actually try to read an extra byte,
    // and if it succeeds, then we'd copy that byte into a new InboundTransfer
    // and loop around, since it's likely the next call also arrived at the
    // same time.
    break;
  }
}

void Connection::HandleIncomingCall(gscoped_ptr<InboundTransfer> transfer) {
  DCHECK(reactor_thread_->IsCurrentThread());

  gscoped_ptr<InboundCall> call(new InboundCall(this));
  Status s = call->ParseFrom(std::move(transfer));
  if (!s.ok()) {
    LOG(WARNING) << ToString() << ": received bad data: " << s.ToString();
    // TODO: shutdown? probably, since any future stuff on this socket will be
    // "unsynchronized"
    return;
  }

  if (!InsertIfNotPresent(&calls_being_handled_, call->call_id(), call.get())) {
    LOG(WARNING) << ToString() << ": received call ID " << call->call_id() <<
      " but was already processing this ID! Ignoring";
    reactor_thread_->DestroyConnection(
      this, Status::RuntimeError("Received duplicate call id",
                                 Substitute("$0", call->call_id())));
    return;
  }

  reactor_thread_->reactor()->messenger()->QueueInboundCall(std::move(call));
}

void Connection::HandleCallResponse(gscoped_ptr<InboundTransfer> transfer) {
  DCHECK(reactor_thread_->IsCurrentThread());
  gscoped_ptr<CallResponse> resp(new CallResponse);
  CHECK_OK(resp->ParseFrom(std::move(transfer)));

  CallAwaitingResponse *car_ptr =
    EraseKeyReturnValuePtr(&awaiting_response_, resp->call_id());
  if (PREDICT_FALSE(car_ptr == nullptr)) {
    LOG(WARNING) << ToString() << ": Got a response for call id " << resp->call_id() << " which "
                 << "was not pending! Ignoring.";
    return;
  }

  // The car->timeout_timer ev::timer will be stopped automatically by its destructor.
  scoped_car car(car_pool_.make_scoped_ptr(car_ptr));

  if (PREDICT_FALSE(car->call.get() == nullptr)) {
    // The call already failed due to a timeout.
    VLOG(1) << "Got response to call id " << resp->call_id() << " after client already timed out";
    return;
  }

  car->call->SetResponse(std::move(resp));
}

void Connection::WriteHandler(ev::io &watcher, int revents) {
  DCHECK(reactor_thread_->IsCurrentThread());

  if (revents & EV_ERROR) {
    reactor_thread_->DestroyConnection(this, Status::NetworkError(ToString() +
          ": writeHandler encountered an error"));
    return;
  }
  DVLOG(3) << ToString() << ": writeHandler: revents = " << revents;

  OutboundTransfer *transfer;
  if (outbound_transfers_.empty()) {
    LOG(WARNING) << ToString() << " got a ready-to-write callback, but there is "
      "nothing to write.";
    write_io_.stop();
    return;
  }

  while (!outbound_transfers_.empty()) {
    transfer = &(outbound_transfers_.front());

    if (!transfer->TransferStarted()) {

      if (transfer->is_for_outbound_call()) {
        CallAwaitingResponse* car = FindOrDie(awaiting_response_, transfer->call_id());
        if (!car->call) {
          // If the call has already timed out, then the 'call' field will have been nulled.
          // In that case, we don't need to bother sending it.
          outbound_transfers_.pop_front();
          transfer->Abort(Status::Aborted("already timed out"));
          delete transfer;
          continue;
        }

        // If this is the start of the transfer, then check if the server has the
        // required RPC flags. We have to wait until just before the transfer in
        // order to ensure that the negotiation has taken place, so that the flags
        // are available.
        const set<RpcFeatureFlag>& required_features = car->call->required_rpc_features();
        const set<RpcFeatureFlag>& server_features = sasl_client_.server_features();
        if (!includes(server_features.begin(), server_features.end(),
                      required_features.begin(), required_features.end())) {
          outbound_transfers_.pop_front();
          Status s = Status::NotSupported("server does not support the required RPC features");
          transfer->Abort(s);
          car->call->SetFailed(s);
          car->call.reset();
          delete transfer;
          continue;
        }
      }
    }

    last_activity_time_ = reactor_thread_->cur_time();
    Status status = transfer->SendBuffer(socket_);
    if (PREDICT_FALSE(!status.ok())) {
      LOG(WARNING) << ToString() << " send error: " << status.ToString();
      reactor_thread_->DestroyConnection(this, status);
      return;
    }

    if (!transfer->TransferFinished()) {
      DVLOG(3) << ToString() << ": writeHandler: xfer not finished.";
      return;
    }

    outbound_transfers_.pop_front();
    delete transfer;
  }

  // If we were able to write all of our outbound transfers,
  // we don't have any more to write.
  write_io_.stop();
}

std::string Connection::ToString() const {
  // This may be called from other threads, so we cannot
  // include anything in the output about the current state,
  // which might concurrently change from another thread.
  return strings::Substitute(
    "$0 $1",
    direction_ == SERVER ? "server connection from" : "client connection to",
    remote_.ToString());
}

Status Connection::InitSaslClient() {
  RETURN_NOT_OK(sasl_client().EnableAnonymous());
  RETURN_NOT_OK(sasl_client().EnablePlain(user_credentials().real_user(),
                                          user_credentials().password()));
  RETURN_NOT_OK(sasl_client().Init(kSaslProtoName));
  return Status::OK();
}

Status Connection::InitSaslServer() {
  // TODO: Do necessary configuration plumbing to enable user authentication.
  // Right now we just enable PLAIN with a "dummy" auth store, which allows everyone in.
  RETURN_NOT_OK(sasl_server().Init(kSaslProtoName));
  gscoped_ptr<AuthStore> auth_store(new DummyAuthStore());
  RETURN_NOT_OK(sasl_server().EnablePlain(std::move(auth_store)));
  return Status::OK();
}

// Reactor task that transitions this Connection from connection negotiation to
// regular RPC handling. Destroys Connection on negotiation error.
class NegotiationCompletedTask : public ReactorTask {
 public:
  NegotiationCompletedTask(Connection* conn,
      const Status& negotiation_status)
    : conn_(conn),
      negotiation_status_(negotiation_status) {
  }

  virtual void Run(ReactorThread *rthread) OVERRIDE {
    rthread->CompleteConnectionNegotiation(conn_, negotiation_status_);
    delete this;
  }

  virtual void Abort(const Status &status) OVERRIDE {
    DCHECK(conn_->reactor_thread()->reactor()->closing());
    VLOG(1) << "Failed connection negotiation due to shut down reactor thread: " <<
        status.ToString();
    delete this;
  }

 private:
  scoped_refptr<Connection> conn_;
  Status negotiation_status_;
};

void Connection::CompleteNegotiation(const Status& negotiation_status) {
  auto task = new NegotiationCompletedTask(this, negotiation_status);
  reactor_thread_->reactor()->ScheduleReactorTask(task);
}

void Connection::MarkNegotiationComplete() {
  DCHECK(reactor_thread_->IsCurrentThread());
  negotiation_complete_ = true;
}

Status Connection::DumpPB(const DumpRunningRpcsRequestPB& req,
                          RpcConnectionPB* resp) {
  DCHECK(reactor_thread_->IsCurrentThread());
  resp->set_remote_ip(remote_.ToString());
  if (negotiation_complete_) {
    resp->set_state(RpcConnectionPB::OPEN);
    resp->set_remote_user_credentials(user_credentials_.ToString());
  } else {
    // It's racy to dump credentials while negotiating, since the Connection
    // object is owned by the negotiation thread at that point.
    resp->set_state(RpcConnectionPB::NEGOTIATING);
  }

  if (direction_ == CLIENT) {
    for (const car_map_t::value_type& entry : awaiting_response_) {
      CallAwaitingResponse *c = entry.second;
      if (c->call) {
        c->call->DumpPB(req, resp->add_calls_in_flight());
      }
    }
  } else if (direction_ == SERVER) {
    for (const inbound_call_map_t::value_type& entry : calls_being_handled_) {
      InboundCall* c = entry.second;
      c->DumpPB(req, resp->add_calls_in_flight());
    }
  } else {
    LOG(FATAL);
  }
  return Status::OK();
}

} // namespace rpc
} // namespace kudu
