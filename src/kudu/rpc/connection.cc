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

#include <netinet/in.h>
#include <string.h>

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

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
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
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

#ifdef __linux__
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <linux/tcp.h>
#endif

using std::includes;
using std::set;
using std::shared_ptr;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {
namespace rpc {

typedef OutboundCall::Phase Phase;

namespace {

// tcp_info struct duplicated from linux/tcp.h.
//
// This allows us to decouple the compile-time Linux headers from the
// runtime Linux kernel. The compile-time headers (and kernel) might be
// older than the runtime kernel, in which case an ifdef-based approach
// wouldn't allow us to get all of the info available.
//
// NOTE: this struct has been annotated with some local notes about the
// contents of each field.
struct tcp_info {
  // Various state-tracking information.
  // ------------------------------------------------------------
  uint8_t    tcpi_state;
  uint8_t    tcpi_ca_state;
  uint8_t    tcpi_retransmits;
  uint8_t    tcpi_probes;
  uint8_t    tcpi_backoff;
  uint8_t    tcpi_options;
  uint8_t    tcpi_snd_wscale : 4, tcpi_rcv_wscale : 4;
  uint8_t    tcpi_delivery_rate_app_limited:1;

  // Configurations.
  // ------------------------------------------------------------
  uint32_t   tcpi_rto;
  uint32_t   tcpi_ato;
  uint32_t   tcpi_snd_mss;
  uint32_t   tcpi_rcv_mss;

  // Counts of packets in various states in the outbound queue.
  // At first glance one might think these are monotonic counters, but
  // in fact they are instantaneous counts of queued packets and thus
  // not very useful for our purposes.
  // ------------------------------------------------------------
  // Number of packets outstanding that haven't been acked.
  uint32_t   tcpi_unacked;

  // Number of packets outstanding that have been selective-acked.
  uint32_t   tcpi_sacked;

  // Number of packets outstanding that have been deemed lost (a SACK arrived
  // for a later packet)
  uint32_t   tcpi_lost;

  // Number of packets in the queue that have been retransmitted.
  uint32_t   tcpi_retrans;

  // The number of packets towards the highest SACKed sequence number
  // (some measure of reording, removed in later Linux versions by
  // 737ff314563ca27f044f9a3a041e9d42491ef7ce)
  uint32_t   tcpi_fackets;

  // Times when various events occurred.
  // ------------------------------------------------------------
  uint32_t   tcpi_last_data_sent;
  uint32_t   tcpi_last_ack_sent;     /* Not remembered, sorry. */
  uint32_t   tcpi_last_data_recv;
  uint32_t   tcpi_last_ack_recv;

  // Path MTU.
  uint32_t   tcpi_pmtu;

  // Receiver slow start threshold.
  uint32_t   tcpi_rcv_ssthresh;

  // Smoothed RTT estimate and variance based on the time between sending data and receiving
  // corresponding ACK. See https://tools.ietf.org/html/rfc2988 for details.
  uint32_t   tcpi_rtt;
  uint32_t   tcpi_rttvar;

  // Slow start threshold.
  uint32_t   tcpi_snd_ssthresh;
  // Sender congestion window (in number of MSS-sized packets)
  uint32_t   tcpi_snd_cwnd;
  // Advertised MSS.
  uint32_t   tcpi_advmss;
  // Amount of packet reordering allowed.
  uint32_t   tcpi_reordering;

  // Receiver-side RTT estimate per the Dynamic Right Sizing algorithm:
  //
  // "A system that is only transmitting acknowledgements can still estimate the round-trip
  // time by observing the time between when a byte is first acknowledged and the receipt of
  // data that is at least one window beyond the sequence number that was acknowledged. If the
  // sender is being throttled by the network, this estimate will be valid. However, if the
  // sending application did not have any data to send, the measured time could be much larger
  // than the actual round-trip time. Thus this measurement acts only as an upper-bound on the
  // round-trip time and should be be used only when it is the only source of round-trip time
  // information."
  uint32_t   tcpi_rcv_rtt;
  uint32_t   tcpi_rcv_space;

  // Total number of retransmitted packets.
  uint32_t   tcpi_total_retrans;

  // Pacing-related metrics.
  uint64_t   tcpi_pacing_rate;
  uint64_t   tcpi_max_pacing_rate;

  // Total bytes ACKed by remote peer.
  uint64_t   tcpi_bytes_acked;    /* RFC4898 tcpEStatsAppHCThruOctetsAcked */
  // Total bytes received (for which ACKs have been sent out).
  uint64_t   tcpi_bytes_received; /* RFC4898 tcpEStatsAppHCThruOctetsReceived */
  // Segments sent and received.
  uint32_t   tcpi_segs_out;       /* RFC4898 tcpEStatsPerfSegsOut */
  uint32_t   tcpi_segs_in;        /* RFC4898 tcpEStatsPerfSegsIn */

  // The following metrics are quite new and not in el7.
  // ------------------------------------------------------------
  uint32_t   tcpi_notsent_bytes;
  uint32_t   tcpi_min_rtt;
  uint32_t   tcpi_data_segs_in;      /* RFC4898 tcpEStatsDataSegsIn */
  uint32_t   tcpi_data_segs_out;     /* RFC4898 tcpEStatsDataSegsOut */

  // Calculated rate at which data was delivered.
  uint64_t   tcpi_delivery_rate;

  // Timers for various states.
  uint64_t   tcpi_busy_time;      /* Time (usec) busy sending data */
  uint64_t   tcpi_rwnd_limited;   /* Time (usec) limited by receive window */
  uint64_t   tcpi_sndbuf_limited; /* Time (usec) limited by send buffer */
};

} // anonymous namespace

///
/// Connection
///
Connection::Connection(ReactorThread *reactor_thread,
                       Sockaddr remote,
                       unique_ptr<Socket> socket,
                       Direction direction,
                       CredentialsPolicy policy)
    : reactor_thread_(reactor_thread),
      remote_(remote),
      socket_(std::move(socket)),
      direction_(direction),
      last_activity_time_(MonoTime::Now()),
      is_epoll_registered_(false),
      next_call_id_(1),
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
  DVLOG(4) << "Registering connection for epoll: " << ToString();
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

void Connection::Shutdown(const Status &status,
                          unique_ptr<ErrorStatusPB> rpc_error) {
  DCHECK(reactor_thread_->IsCurrentThread());
  shutdown_status_ = status.CloneAndPrepend("RPC connection failed");

  if (inbound_ && inbound_->TransferStarted()) {
    double secs_since_active =
        (reactor_thread_->cur_time() - last_activity_time_).ToSeconds();
    LOG(WARNING) << "Shutting down " << ToString()
                 << " with pending inbound data ("
                 << inbound_->StatusAsString() << ", last active "
                 << HumanReadableElapsedTime::ToShortString(secs_since_active)
                 << " ago, status=" << status.ToString() << ")";
  }

  // Clear any calls which have been sent and were awaiting a response.
  for (const car_map_t::value_type &v : awaiting_response_) {
    CallAwaitingResponse *c = v.second;
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
    OutboundTransfer *t = &outbound_transfers_.front();
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

void Connection::CancelOutboundCall(const shared_ptr<OutboundCall> &call) {
  CallAwaitingResponse* car = FindPtrOrNull(awaiting_response_, call->call_id());
  if (car != nullptr) {
    // car->call may be NULL if the call has timed out already.
    DCHECK(!car->call || car->call.get() == call.get());
    car->call.reset();
  }
}

// Inject a cancellation when 'call' is in state 'FLAGS_rpc_inject_cancellation_state'.
void inline Connection::MaybeInjectCancellation(const shared_ptr<OutboundCall> &call) {
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
                                 Connection *conn)
      : call_(std::move(call)), conn_(conn) {}

  virtual void NotifyTransferFinished() OVERRIDE {
    // TODO: would be better to cancel the transfer while it is still on the queue if we
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

  virtual void NotifyTransferAborted(const Status &status) OVERRIDE {
    VLOG(1) << "Transfer of RPC call " << call_->ToString() << " aborted: "
            << status.ToString();
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
  int32_t call_id = GetNextCallId();
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

  TransferCallbacks *cb = new CallTransferCallbacks(std::move(call), this);
  awaiting_response_[call_id] = car.release();
  QueueOutbound(unique_ptr<OutboundTransfer>(
      OutboundTransfer::CreateForCallRequest(call_id, tmp_slices, cb)));
}

// Callbacks for sending an RPC call response from the server.
// This takes ownership of the InboundCall object so that, once it has
// been responded to, we can free up all of the associated memory.
struct ResponseTransferCallbacks : public TransferCallbacks {
 public:
  ResponseTransferCallbacks(unique_ptr<InboundCall> call,
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
  unique_ptr<InboundCall> call_;
  Connection *conn_;
};

// Reactor task which puts a transfer on the outbound transfer queue.
class QueueTransferTask : public ReactorTask {
 public:
  QueueTransferTask(unique_ptr<OutboundTransfer> transfer,
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
  unique_ptr<OutboundTransfer> transfer_;
  Connection *conn_;
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

  TransferCallbacks *cb = new ResponseTransferCallbacks(std::move(call), this);
  // After the response is sent, can delete the InboundCall object.
  // We set a dummy call ID and required feature set, since these are not needed
  // when sending responses.
  unique_ptr<OutboundTransfer> t(
      OutboundTransfer::CreateForCallResponse(tmp_slices, cb));

  QueueTransferTask *task = new QueueTransferTask(std::move(t), this);
  reactor_thread_->reactor()->ScheduleReactorTask(task);
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
    Status status = inbound_->ReceiveBuffer(*socket_);
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

void Connection::HandleIncomingCall(unique_ptr<InboundTransfer> transfer) {
  DCHECK(reactor_thread_->IsCurrentThread());

  unique_ptr<InboundCall> call(new InboundCall(this));
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

void Connection::HandleCallResponse(unique_ptr<InboundTransfer> transfer) {
  DCHECK(reactor_thread_->IsCurrentThread());
  unique_ptr<CallResponse> resp(new CallResponse);
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

  if (PREDICT_FALSE(!car->call)) {
    // The call already failed due to a timeout.
    VLOG(1) << "Got response to call id " << resp->call_id() << " after client "
            << "already timed out or cancelled";
    return;
  }

  car->call->SetResponse(std::move(resp));

  // Test cancellation when 'car->call' is in 'FINISHED_SUCCESS' or 'FINISHED_ERROR' state.
  MaybeInjectCancellation(car->call);
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
    Status status = transfer->SendBuffer(*socket_);
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

  virtual void Run(ReactorThread *rthread) OVERRIDE {
    rthread->CompleteConnectionNegotiation(conn_,
                                           negotiation_status_,
                                           std::move(rpc_error_));
    delete this;
  }

  virtual void Abort(const Status &status) OVERRIDE {
    DCHECK(conn_->reactor_thread()->reactor()->closing());
    VLOG(1) << "Failed connection negotiation due to shut down reactor thread: "
            << status.ToString();
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
                          RpcConnectionPB* resp) {
  DCHECK(reactor_thread_->IsCurrentThread());
  resp->set_remote_ip(remote_.ToString());
  if (negotiation_complete_) {
    resp->set_state(RpcConnectionPB::OPEN);
  } else {
    resp->set_state(RpcConnectionPB::NEGOTIATING);
  }

  if (direction_ == CLIENT) {
    for (const car_map_t::value_type& entry : awaiting_response_) {
      CallAwaitingResponse *c = entry.second;
      if (c->call) {
        c->call->DumpPB(req, resp->add_calls_in_flight());
      }
    }

    resp->set_outbound_queue_size(num_queued_outbound_transfers());
  } else if (direction_ == SERVER) {
    if (negotiation_complete_) {
      // It's racy to dump credentials while negotiating, since the Connection
      // object is owned by the negotiation thread at that point.
      resp->set_remote_user_credentials(remote_user_.ToString());
    }
    for (const inbound_call_map_t::value_type& entry : calls_being_handled_) {
      InboundCall* c = entry.second;
      c->DumpPB(req, resp->add_calls_in_flight());
    }
  } else {
    LOG(FATAL);
  }
#ifdef __linux__
  if (negotiation_complete_) {
    // TODO(todd): it's a little strange to not set socket level stats during
    // negotiation, but we don't have access to the socket here until negotiation
    // is complete.
    WARN_NOT_OK(GetSocketStatsPB(resp->mutable_socket_stats()),
                "could not fill in TCP info for RPC connection");
  }
#endif // __linux__
  return Status::OK();
}

#ifdef __linux__
Status Connection::GetSocketStatsPB(SocketStatsPB* pb) const {
  DCHECK(reactor_thread_->IsCurrentThread());
  int fd = socket_->GetFd();
  CHECK_GE(fd, 0);

  // Fetch TCP_INFO statistics from the kernel.
  tcp_info ti;
  memset(&ti, 0, sizeof(ti));
  socklen_t len = sizeof(ti);
  int rc = getsockopt(fd, IPPROTO_TCP, TCP_INFO, &ti, &len);
  if (rc == 0) {
#   define HAS_FIELD(field_name) \
        (len >= offsetof(tcp_info, field_name) + sizeof(ti.field_name))
    if (!HAS_FIELD(tcpi_total_retrans)) {
      // All the fields up through tcpi_total_retrans were present since very old
      // kernel versions, beyond our minimal supported. So, we can just bail if we
      // don't get sufficient data back.
      return Status::NotSupported("bad length returned for TCP_INFO");
    }

    pb->set_rtt(ti.tcpi_rtt);
    pb->set_rttvar(ti.tcpi_rttvar);
    pb->set_snd_cwnd(ti.tcpi_snd_cwnd);
    pb->set_total_retrans(ti.tcpi_total_retrans);

    // The following fields were added later in kernel development history.
    // In RHEL6 they were backported starting in 6.8. Even though they were
    // backported all together as a group, we'll just be safe and check for
    // each individually.
    if (HAS_FIELD(tcpi_pacing_rate)) {
      pb->set_pacing_rate(ti.tcpi_pacing_rate);
    }
    if (HAS_FIELD(tcpi_max_pacing_rate)) {
      pb->set_max_pacing_rate(ti.tcpi_max_pacing_rate);
    }
    if (HAS_FIELD(tcpi_bytes_acked)) {
      pb->set_bytes_acked(ti.tcpi_bytes_acked);
    }
    if (HAS_FIELD(tcpi_bytes_received)) {
      pb->set_bytes_received(ti.tcpi_bytes_received);
    }
    if (HAS_FIELD(tcpi_segs_out)) {
      pb->set_segs_out(ti.tcpi_segs_out);
    }
    if (HAS_FIELD(tcpi_segs_in)) {
      pb->set_segs_in(ti.tcpi_segs_in);
    }

    // Calculate sender bandwidth based on the same logic used by the 'ss' utility.
    if (ti.tcpi_rtt > 0 && ti.tcpi_snd_mss && ti.tcpi_snd_cwnd) {
      // Units:
      //  rtt = usec
      //  cwnd = number of MSS-size packets
      //  mss = bytes / packet
      //
      // Dimensional analysis:
      //   packets * bytes/packet * usecs/sec / usec -> bytes/sec
      static constexpr int kUsecsPerSec = 1000000;
      pb->set_send_bytes_per_sec(static_cast<int64_t>(ti.tcpi_snd_cwnd) *
                                 ti.tcpi_snd_mss * kUsecsPerSec / ti.tcpi_rtt);
    }
  }

  // Fetch the queue sizes.
  int queue_len = 0;
  rc = ioctl(fd, TIOCOUTQ, &queue_len);
  if (rc == 0) {
    pb->set_send_queue_bytes(queue_len);
  }
  rc = ioctl(fd, FIONREAD, &queue_len);
  if (rc == 0) {
    pb->set_receive_queue_bytes(queue_len);
  }
  return Status::OK();
}
#endif // __linux__

} // namespace rpc
} // namespace kudu
