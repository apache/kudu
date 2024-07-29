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
#include <cerrno>
#include <iostream>
#include <memory>
#include <set>
#include <string>

#include <boost/intrusive/detail/list_iterator.hpp>
#include <boost/intrusive/list.hpp>
#include <ev.h>
#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/inbound_call.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/outbound_call.h"
#include "kudu/rpc/reactor.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/rpc_introspection.pb.h"
#include "kudu/rpc/transfer.h"
#include "kudu/util/faststring.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

using std::includes;
using std::set;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {
namespace rpc {

typedef OutboundCall::Phase Phase;

///
/// Connection
///
Connection::Connection(ReactorThread* reactor_thread,
                       const Sockaddr& remote,
                       unique_ptr<Socket> socket,
                       Direction direction,
                       CredentialsPolicy policy)
    : reactor_thread_(reactor_thread),
      remote_(remote),
      socket_(std::move(socket)),
      direction_(direction),
      last_activity_time_(MonoTime::Now()),
      is_epoll_registered_(false),
      call_id_(std::numeric_limits<int32_t>::max()),
      credentials_policy_(policy),
      negotiation_complete_(false),
      is_confidential_(false),
      scheduled_for_shutdown_(false) {
}

Status Connection::SetNonBlocking(bool enabled) {
  return socket_->SetNonBlocking(enabled);
}

Status Connection::SetTcpKeepAlive(int idle_time_s, int retry_time_s, int num_retries) {
  DCHECK_GT(idle_time_s, 0);
  DCHECK_GE(retry_time_s, 0);
  DCHECK_GE(num_retries, 0);
  return socket_->SetTcpKeepAlive(std::max(1, idle_time_s), std::max(0, retry_time_s),
      std::max(0, num_retries));
}

void Connection::EpollRegister(ev::loop_ref& loop) {
  DCHECK(reactor_thread_->IsCurrentThread());
  DVLOG(4) << Substitute("registering connection for epoll: $0", ToString());
  write_io_.set(loop);
  write_io_.set(socket_->GetFd(), ev::WRITE);
  write_io_.set<Connection, &Connection::WriteHandler>(this);
  if (direction_ == CLIENT && negotiation_complete_) {
    write_io_.start();
  }
  read_io_.set(loop);
  read_io_.set(socket_->GetFd(), ev::READ);
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
  InboundTransfer* transfer = inbound_.get();
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

void Connection::Shutdown(const Status& status,
                          unique_ptr<ErrorStatusPB> rpc_error) {
  DCHECK(reactor_thread_->IsCurrentThread());
  shutdown_status_ = status.CloneAndPrepend("RPC connection failed");

  if (inbound_ && inbound_->TransferStarted()) {
    double secs_since_active =
        (reactor_thread_->cur_time() - last_activity_time_).ToSeconds();
    LOG(WARNING) << Substitute(
        "shutting down $0 with pending inbound data: "
        "$1; last active $2 ago: status $3",
        ToString(),
        inbound_->StatusAsString(),
        HumanReadableElapsedTime::ToShortString(secs_since_active),
        status.ToString());
  }

  // Clear any calls which have been sent and were awaiting a response.
  for (const auto& [_, c] : awaiting_response_) {
    if (c->call) {
      // Make sure every awaiting call receives the error info, if any.
      unique_ptr<ErrorStatusPB> error;
      if (rpc_error) {
        error.reset(new ErrorStatusPB(*rpc_error));
      }
      c->call->SetFailed(status,
                         negotiation_complete_ ? Phase::REMOTE_CALL
                                               : Phase::CONNECTION_NEGOTIATION,
                         std::move(error));
    }
    // And we must return the CallAwaitingResponse to the pool
    car_pool_.Destroy(c);
  }
  awaiting_response_.clear();

  // Clear any outbound transfers.
  while (!outbound_transfers_.empty()) {
    auto* t = &outbound_transfers_.front();
    outbound_transfers_.pop_front();
    delete t;
  }

  read_io_.stop();
  write_io_.stop();
  is_epoll_registered_ = false;
  if (socket_) {
    WARN_NOT_OK(socket_->Close(), "Error closing socket");
  }
}

void Connection::QueueOutbound(unique_ptr<OutboundTransfer> transfer) {
  DCHECK(reactor_thread_->IsCurrentThread());

  if (PREDICT_FALSE(!shutdown_status_.ok())) {
    // If we've already shut down, then we just need to abort the
    // transfer rather than bothering to queue it.
    transfer->Abort(shutdown_status_);
    return;
  }

  DVLOG(3) << Substitute("queueing transfer: $0", transfer->HexDump());

  outbound_transfers_.push_back(*transfer.release());

  if (negotiation_complete_ && !write_io_.is_active()) {
    // Optimistically assume that the socket is writable if we didn't already
    // have something queued.
    if (ProcessOutboundTransfers() == kMoreToSend) {
      write_io_.start();
    }
  }
}

Connection::CallAwaitingResponse::~CallAwaitingResponse() {
  DCHECK(conn->reactor_thread_->IsCurrentThread());
}

void Connection::CallAwaitingResponse::HandleTimeout(ev::timer& watcher,
                                                     int /*revents*/) {
  if (remaining_timeout > 0) {
    const auto rem = watcher.remaining();
    if (PREDICT_FALSE(rem < -1.0)) {
      LOG(WARNING) << Substitute(
          "RPC call timeout handler was delayed by $0s: this may be due "
          "to a process-wide pause such as swapping, logging-related delays, "
          "or allocator lock contention. Will allow extra $1s for a response",
          rem, remaining_timeout);
    }

    watcher.set(remaining_timeout, 0);
    watcher.start();
    remaining_timeout = 0;
    return;
  }

  conn->HandleOutboundCallTimeout(this);
}

void Connection::HandleOutboundCallTimeout(CallAwaitingResponse* car) {
  DCHECK(reactor_thread_->IsCurrentThread());
  if (!car->call) {
    // The RPC may have been cancelled before the timeout was hit.
    return;
  }
  // The timeout timer is stopped by the car destructor exiting Connection::HandleCallResponse()
  DCHECK(!car->call->IsFinished());

  // Mark the call object as failed.
  car->call->SetTimedOut(negotiation_complete_ ? Phase::REMOTE_CALL
                                               : Phase::CONNECTION_NEGOTIATION);

  // Test cancellation when 'car->call' is in 'TIMED_OUT' state
  MaybeInjectCancellation(car->call);

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

void Connection::CancelOutboundCall(const shared_ptr<OutboundCall>& call) {
  CallAwaitingResponse* car = FindPtrOrNull(awaiting_response_, call->call_id());
  if (car != nullptr) {
    // car->call may be NULL if the call has timed out already.
    DCHECK(!car->call || car->call.get() == call.get());
    car->call.reset();
  }
}

Status Connection::GetLocalAddress(Sockaddr* addr) const {
  DCHECK(socket_);
  DCHECK(addr);
  return socket_->GetSocketAddress(addr);
}

// Inject a cancellation when 'call' is in state 'FLAGS_rpc_inject_cancellation_state'.
void inline Connection::MaybeInjectCancellation(const shared_ptr<OutboundCall>& call) {
  if (PREDICT_FALSE(call->ShouldInjectCancellation())) {
    reactor_thread_->reactor()->messenger()->QueueCancellation(call);
  }
}

// Callbacks after sending a call on the wire.
// This notifies the OutboundCall object to change its state to SENT once it
// has been fully transmitted.
struct CallTransferCallbacks : public TransferCallbacks {
 public:
  explicit CallTransferCallbacks(shared_ptr<OutboundCall> call,
                                 Connection* conn)
      : call_(std::move(call)), conn_(conn) {}

  void NotifyTransferFinished() override {
    // TODO(mpercy): would be better to cancel the transfer while it is still on the queue if we
    // timed out before the transfer started, but there is still a race in the case of
    // a partial send that we have to handle here
    if (call_->IsFinished()) {
      DCHECK(call_->IsTimedOut() || call_->IsCancelled());
    } else {
      call_->SetSent();
      // Test cancellation when 'call_' is in 'SENT' state.
      conn_->MaybeInjectCancellation(call_);
    }
    delete this;
  }

  void NotifyTransferAborted(const Status& status) override {
    VLOG(1) << Substitute(
        "transfer of $0 aborted: $1", call_->ToString(), status.ToString());
    delete this;
  }

 private:
  shared_ptr<OutboundCall> call_;
  Connection* conn_;
};

void Connection::QueueOutboundCall(shared_ptr<OutboundCall> call) {
  DCHECK(call);
  DCHECK_EQ(direction_, CLIENT);
  DCHECK(reactor_thread_->IsCurrentThread());

  if (PREDICT_FALSE(!shutdown_status_.ok())) {
    // Already shutdown
    call->SetFailed(shutdown_status_,
                    negotiation_complete_ ? Phase::REMOTE_CALL
                                          : Phase::CONNECTION_NEGOTIATION);
    return;
  }

  // At this point the call has a serialized request, but no call header, since we haven't
  // yet assigned a call ID.
  DCHECK(!call->call_id_assigned());

  // We shouldn't reach this point if 'call' was requested to be cancelled.
  DCHECK(!call->cancellation_requested());

  // Assign the call ID.
  const int32_t call_id = GetNextCallId();
  call->set_call_id(call_id);

  // Serialize the actual bytes to be put on the wire.
  TransferPayload tmp_slices;
  call->SerializeTo(&tmp_slices);

  call->SetQueued();

  // Test cancellation when 'call_' is in 'ON_OUTBOUND_QUEUE' state.
  MaybeInjectCancellation(call);

  scoped_car car(car_pool_.make_scoped_ptr(car_pool_.Construct()));
  car->conn = this;
  car->call = call;

  // Set up the timeout timer.
  const auto& timeout = call->controller()->timeout();
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

  TransferCallbacks* cb = new CallTransferCallbacks(std::move(call), this);
  awaiting_response_[call_id] = car.release();
  QueueOutbound(unique_ptr<OutboundTransfer>(
      OutboundTransfer::CreateForCallRequest(call_id, tmp_slices, cb)));
}

// Callbacks for sending an RPC call response from the server.
// This takes ownership of the InboundCall object so that, once it has
// been responded to, we can free up all of the associated memory.
struct ResponseTransferCallbacks final : public TransferCallbacks {
 public:
  ResponseTransferCallbacks(unique_ptr<InboundCall> call, Connection* conn)
      : call_(std::move(call)),
        conn_(conn) {
  }

  ~ResponseTransferCallbacks() override {
    // Remove the call from the map.
    auto* call_from_map = EraseKeyReturnValuePtr(
        &conn_->calls_being_handled_, call_->call_id());
    DCHECK_EQ(call_from_map, call_.get());
  }

  void NotifyTransferFinished() override {
    delete this;
  }

  void NotifyTransferAborted(const Status& /*status*/) override {
    LOG(WARNING) << Substitute(
        "$0 torn down before $1 could send its response",
        conn_->ToString(), call_->ToString());
    delete this;
  }

 private:
  unique_ptr<InboundCall> call_;
  Connection* conn_;
};

// Reactor task which puts a transfer on the outbound transfer queue.
class QueueTransferTask : public ReactorTask {
 public:
  QueueTransferTask(unique_ptr<OutboundTransfer> transfer, Connection* conn)
      : transfer_(std::move(transfer)),
        conn_(conn) {
  }

  void Run(ReactorThread* /*thr*/) override {
    conn_->QueueOutbound(std::move(transfer_));
    delete this;
  }

  void Abort(const Status& status) override {
    transfer_->Abort(status);
    delete this;
  }

 private:
  unique_ptr<OutboundTransfer> transfer_;
  Connection* conn_;
};

void Connection::QueueResponseForCall(unique_ptr<InboundCall> call) {
  // This is usually called by the IPC worker thread when the response
  // is set, but in some circumstances may also be called by the
  // reactor thread (e.g. if the service has shut down)

  DCHECK_EQ(direction_, SERVER);

  // If the connection is torn down, then the QueueOutbound() call that
  // eventually runs in the reactor thread will take care of calling
  // ResponseTransferCallbacks::NotifyTransferAborted.

  TransferPayload tmp_slices;
  call->SerializeResponseTo(&tmp_slices);

  TransferCallbacks* cb = new ResponseTransferCallbacks(std::move(call), this);
  // After the response is sent, can delete the InboundCall object.
  // We set a dummy call ID and required feature set, since these are not needed
  // when sending responses.
  unique_ptr<OutboundTransfer> t(
      OutboundTransfer::CreateForCallResponse(tmp_slices, cb));

  reactor_thread_->reactor()->ScheduleReactorTask(
      new QueueTransferTask(std::move(t), this));
}

void Connection::set_confidential(bool is_confidential) {
  is_confidential_ = is_confidential;
}

bool Connection::SatisfiesCredentialsPolicy(CredentialsPolicy policy) const {
  DCHECK_EQ(direction_, CLIENT);
  return (policy == CredentialsPolicy::ANY_CREDENTIALS) ||
      (policy == credentials_policy_);
}

RpczStore* Connection::rpcz_store() {
  return reactor_thread_->reactor()->messenger()->rpcz_store();
}

void Connection::ReadHandler(ev::io& /*watcher*/, int revents) {
  DCHECK(reactor_thread_->IsCurrentThread());

  DVLOG(3) << Substitute("$0 ReadHandler(revents=$1)", ToString(), revents);
  if (revents & EV_ERROR) {
    reactor_thread_->DestroyConnection(this, Status::NetworkError(ToString() +
                                     ": ReadHandler encountered an error"));
    return;
  }
  last_activity_time_ = reactor_thread_->cur_time();

  const int64_t rpc_max_size = reactor_thread_->reactor()->messenger()->rpc_max_message_size();
  faststring extra_buf;
  while (true) {
    if (!inbound_) {
      // Initialize the maximum RPC message size set by caller.
      inbound_.reset(new InboundTransfer());
    }
    Status status = inbound_->ReceiveBuffer(socket_.get(), &extra_buf, rpc_max_size);
    if (PREDICT_FALSE(!status.ok())) {
      if (status.posix_code() == ESHUTDOWN) {
        VLOG(1) << Substitute("$0 shut down by remote end", ToString());
      } else {
        LOG(WARNING) << Substitute("$0 recv error: $1",
                                   ToString(), status.ToString());
      }
      reactor_thread_->DestroyConnection(this, status);
      return;
    }
    if (!inbound_->TransferFinished()) {
      DVLOG(3) << Substitute("$0: read is not yet finished yet", ToString());
      return;
    }
    DVLOG(3) << Substitute("$0: finished reading $1 bytes",
                           ToString(), inbound_->data().size());

    switch (direction_) {
      case CLIENT:
        HandleCallResponse(std::move(inbound_));
        break;

      case SERVER:
        HandleIncomingCall(std::move(inbound_));
        break;

      default:
        LOG(DFATAL) << Substitute("$0: invalid direction",
                                  static_cast<uint16_t>(direction_));
        break;
    }

    if (extra_buf.size() > 0) {
      inbound_.reset(new InboundTransfer(std::move(extra_buf)));
    } else {
      break;
    }
  }
}

void Connection::HandleIncomingCall(unique_ptr<InboundTransfer> transfer) {
  DCHECK(reactor_thread_->IsCurrentThread());

  unique_ptr<InboundCall> call(new InboundCall(this));
  Status s = call->ParseFrom(std::move(transfer));
  if (PREDICT_FALSE(!s.ok())) {
    LOG(WARNING) << Substitute("$0: received bad data: '$1'",
                             ToString(), s.ToString());
    // Shutting down down the connection since there is a high risk of receiving
    // "unsynchronized" data on this socket after this error.
    Shutdown(s);
    return;
  }

  if (PREDICT_FALSE(!InsertIfNotPresent(&calls_being_handled_,
                                        call->call_id(),
                                        call.get()))) {
    LOG(WARNING) << Substitute(
        "$0: received call ID $1 but was already processing this ID, ignoring",
        ToString(), call->call_id());
    reactor_thread_->DestroyConnection(
        this, Status::RuntimeError("Received duplicate call id",
                                   Substitute("$0", call->call_id())));
    return;
  }

  reactor_thread_->reactor()->messenger()->QueueInboundCall(std::move(call));
}

void Connection::HandleCallResponse(unique_ptr<InboundTransfer> transfer) {
  DCHECK(reactor_thread_->IsCurrentThread());
  unique_ptr<CallResponse> resp(new CallResponse);
  CHECK_OK(resp->ParseFrom(std::move(transfer)));

  CallAwaitingResponse* car_ptr = EraseKeyReturnValuePtr(
      &awaiting_response_, resp->call_id());
  if (PREDICT_FALSE(car_ptr == nullptr)) {
    LOG(WARNING) << Substitute(
        "$0: got a response for call id $1 which was not pending, ignoring",
        ToString(), resp->call_id());
    return;
  }

  // The car->timeout_timer ev::timer will be stopped automatically by its destructor.
  scoped_car car(car_pool_.make_scoped_ptr(car_ptr));

  if (PREDICT_FALSE(!car->call)) {
    // The call already failed due to a timeout.
    VLOG(1) << Substitute(
        "got response to call id $0 after client already timed out or cancelled",
         resp->call_id());
    return;
  }

  car->call->SetResponse(std::move(resp));

  // Test cancellation when 'car->call' is in 'FINISHED_SUCCESS' or 'FINISHED_ERROR' state.
  MaybeInjectCancellation(car->call);
}

void Connection::WriteHandler(ev::io& /*watcher*/, int revents) {
  DCHECK(reactor_thread_->IsCurrentThread());

  if (revents & EV_ERROR) {
    reactor_thread_->DestroyConnection(this, Status::NetworkError(ToString() +
          ": writeHandler encountered an error"));
    return;
  }
  DVLOG(3) << Substitute("$0: writeHandler: revents=$1", ToString(), revents);

  if (outbound_transfers_.empty()) {
    LOG(WARNING) << Substitute(
        "$0 got a ready-to-write callback, but there is nothing to write",
        ToString());
    write_io_.stop();
    return;
  }
  if (ProcessOutboundTransfers() == kNoMoreToSend) {
    write_io_.stop();
  }
}

Connection::ProcessOutboundTransfersResult Connection::ProcessOutboundTransfers() {
  while (!outbound_transfers_.empty()) {
    OutboundTransfer* transfer = &outbound_transfers_.front();

    if (!transfer->TransferStarted()) {
      if (transfer->is_for_outbound_call()) {
        CallAwaitingResponse* car = FindOrDie(awaiting_response_, transfer->call_id());
        if (!car->call) {
          // If the call has already timed out or has already been cancelled, the 'call'
          // field would be set to NULL. In that case, don't bother sending it.
          outbound_transfers_.pop_front();
          transfer->Abort(Status::Aborted("already timed out or cancelled"));
          delete transfer;
          continue;
        }

        // If this is the start of the transfer, then check if the server has the
        // required RPC flags. We have to wait until just before the transfer in
        // order to ensure that the negotiation has taken place, so that the flags
        // are available.
        const set<RpcFeatureFlag>& required_features = car->call->required_rpc_features();
        if (!includes(remote_features_.begin(), remote_features_.end(),
                      required_features.begin(), required_features.end())) {
          outbound_transfers_.pop_front();
          Status s = Status::NotSupported("server does not support the required RPC features");
          transfer->Abort(s);
          Phase phase = negotiation_complete_ ? Phase::REMOTE_CALL : Phase::CONNECTION_NEGOTIATION;
          car->call->SetFailed(std::move(s), phase);
          // Test cancellation when 'call_' is in 'FINISHED_ERROR' state.
          MaybeInjectCancellation(car->call);
          car->call.reset();
          delete transfer;
          continue;
        }

        car->call->SetSending();

        // Test cancellation when 'call_' is in 'SENDING' state.
        MaybeInjectCancellation(car->call);
      }
    }

    last_activity_time_ = reactor_thread_->cur_time();
    Status status = transfer->SendBuffer(socket_.get());
    if (PREDICT_FALSE(!status.ok())) {
      LOG(WARNING) << Substitute(
          "$0 send error: $1", ToString(), status.ToString());
      reactor_thread_->DestroyConnection(this, status);
      return kConnectionDestroyed;
    }

    if (!transfer->TransferFinished()) {
      DVLOG(3) << Substitute("$0: writeHandler: xfer not finished", ToString());
      return kMoreToSend;
    }

    outbound_transfers_.pop_front();
    delete transfer;
  }

  return kNoMoreToSend;
}

string Connection::ToString() const {
  // This may be called from other threads, so we cannot
  // include anything in the output about the current state,
  // which might concurrently change from another thread.
  return Substitute("$0 $1",
                    direction_ == SERVER
                        ? "server connection from"
                        : "client connection to", remote_.ToString());
}

// Reactor task that transitions this Connection from connection negotiation to
// regular RPC handling. Destroys Connection on negotiation error.
class NegotiationCompletedTask : public ReactorTask {
 public:
  NegotiationCompletedTask(Connection* conn,
                           Status negotiation_status,
                           std::unique_ptr<ErrorStatusPB> rpc_error)
      : conn_(conn),
        negotiation_status_(std::move(negotiation_status)),
        rpc_error_(std::move(rpc_error)) {
  }

  void Run(ReactorThread* rthread) override {
    rthread->CompleteConnectionNegotiation(conn_,
                                           negotiation_status_,
                                           std::move(rpc_error_));
    delete this;
  }

  void Abort(const Status& status) override {
    DCHECK(conn_->reactor_thread()->reactor()->closing());
    VLOG(1) << Substitute("connection negotiation aborted: $0", status.ToString());
    delete this;
  }

 private:
  scoped_refptr<Connection> conn_;
  const Status negotiation_status_;
  std::unique_ptr<ErrorStatusPB> rpc_error_;
};

void Connection::CompleteNegotiation(Status negotiation_status,
                                     unique_ptr<ErrorStatusPB> rpc_error) {
  auto task = new NegotiationCompletedTask(
      this, std::move(negotiation_status), std::move(rpc_error));
  reactor_thread_->reactor()->ScheduleReactorTask(task);
}

void Connection::MarkNegotiationComplete() {
  DCHECK(reactor_thread_->IsCurrentThread());
  negotiation_complete_ = true;
}

Status Connection::DumpPB(const DumpConnectionsRequestPB& req,
                          RpcConnectionPB* resp) const {
  DCHECK(reactor_thread_->IsCurrentThread());
  resp->set_remote_ip(remote_.ToString());
  if (negotiation_complete_) {
    resp->set_state(RpcConnectionPB::OPEN);
  } else {
    resp->set_state(RpcConnectionPB::NEGOTIATING);
  }

  switch (direction_) {
    case CLIENT:
      for (const auto& [_, c]: awaiting_response_) {
        if (c->call) {
          c->call->DumpPB(req, resp->add_calls_in_flight());
        }
      }
      resp->set_outbound_queue_size(outbound_transfers_.size());
      break;

    case SERVER:
      if (negotiation_complete_) {
        // It's racy to dump credentials while negotiating, since the Connection
        // object is owned by the negotiation thread at that point.
        resp->set_remote_user_credentials(remote_user_.ToString());
      }
      for (const auto& [_, c]: calls_being_handled_) {
        c->DumpPB(req, resp->add_calls_in_flight());
      }
      break;

    default:
      LOG(DFATAL) << Substitute("$0: invalid direction",
                                static_cast<uint16_t>(direction_));
      break;
  }
#if defined(__linux__)
  if (negotiation_complete_ && remote_.is_ip()) {
    // TODO(todd): it's a little strange to not set socket level stats during
    // negotiation, but we don't have access to the socket here until negotiation
    // is complete.
    WARN_NOT_OK(socket_->GetStats(resp->mutable_socket_stats()),
                "could not fill in TCP info for RPC connection");
  }
#endif // #if defined(__linux__) ...

  if (negotiation_complete_ && remote_.is_ip()) {
    WARN_NOT_OK(socket_->GetTransportDetails(resp->mutable_transport_details()),
                "could not fill in transport info for RPC connection");
  }
  return Status::OK();
}

} // namespace rpc
} // namespace kudu
