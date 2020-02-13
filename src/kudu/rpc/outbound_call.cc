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

#include "kudu/rpc/outbound_call.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/function.hpp>
#include <gflags/gflags.h>
#include <google/protobuf/message.h>

#include "kudu/gutil/port.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/gutil/walltime.h"
#include "kudu/rpc/constants.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_introspection.pb.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/rpc/serialization.h"
#include "kudu/rpc/transfer.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/kernel_stack_watchdog.h"
#include "kudu/util/net/sockaddr.h"

// 100M cycles should be about 50ms on a 2Ghz box. This should be high
// enough that involuntary context switches don't trigger it, but low enough
// that any serious blocking behavior on the reactor would.
DEFINE_int64(rpc_callback_max_cycles, 100 * 1000 * 1000,
             "The maximum number of cycles for which an RPC callback "
             "should be allowed to run without emitting a warning."
             " (Advanced debugging option)");
TAG_FLAG(rpc_callback_max_cycles, advanced);
TAG_FLAG(rpc_callback_max_cycles, runtime);

// Flag used in debug build for injecting cancellation at different code paths.
DEFINE_int32(rpc_inject_cancellation_state, -1,
             "If this flag is not -1, it is the state in which a cancellation request "
             "will be injected. Should use values in OutboundCall::State only");
TAG_FLAG(rpc_inject_cancellation_state, unsafe);

using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace rpc {

using google::protobuf::Message;
using strings::Substitute;

static const double kMicrosPerSecond = 1000000.0;

///
/// OutboundCall
///

OutboundCall::OutboundCall(const ConnectionId& conn_id,
                           const RemoteMethod& remote_method,
                           google::protobuf::Message* response_storage,
                           RpcController* controller,
                           ResponseCallback callback)
    : state_(READY),
      remote_method_(remote_method),
      conn_id_(conn_id),
      callback_(std::move(callback)),
      controller_(DCHECK_NOTNULL(controller)),
      response_(DCHECK_NOTNULL(response_storage)),
      cancellation_requested_(false) {
  DVLOG(4) << "OutboundCall " << this << " constructed with state_: " << StateName(state_)
           << " and RPC timeout: "
           << (controller->timeout().Initialized() ? controller->timeout().ToString() : "none");
  header_.set_call_id(kInvalidCallId);
  remote_method.ToPB(header_.mutable_remote_method());
  start_time_ = MonoTime::Now();

  if (!controller_->required_server_features().empty()) {
    required_rpc_features_.insert(RpcFeatureFlag::APPLICATION_FEATURE_FLAGS);
  }

  if (controller_->request_id_) {
    header_.set_allocated_request_id(controller_->request_id_.release());
  }
}

OutboundCall::~OutboundCall() {
  DCHECK(IsFinished());
  DVLOG(4) << "OutboundCall " << this << " destroyed with state_: " << StateName(state_);
}

void OutboundCall::SerializeTo(TransferPayload* slices) {
  DCHECK_LT(0, request_buf_.size())
      << "Must call SetRequestPayload() before SerializeTo()";

  const MonoDelta &timeout = controller_->timeout();
  if (timeout.Initialized()) {
    header_.set_timeout_millis(timeout.ToMilliseconds());
  }

  for (uint32_t feature : controller_->required_server_features()) {
    header_.add_required_feature_flags(feature);
  }

  DCHECK_LE(0, sidecar_byte_size_);
  serialization::SerializeHeader(
      header_, sidecar_byte_size_ + request_buf_.size(), &header_buf_);

  slices->clear();
  slices->push_back(header_buf_);
  slices->push_back(request_buf_);
  for (auto& sidecar : sidecars_) {
    sidecar->AppendSlices(slices);
  }
}

void OutboundCall::SetRequestPayload(const Message& req,
    vector<unique_ptr<RpcSidecar>>&& sidecars) {
  DCHECK_EQ(-1, sidecar_byte_size_);

  sidecars_ = move(sidecars);
  DCHECK_LE(sidecars_.size(), TransferLimits::kMaxSidecars);

  // Compute total size of sidecar payload so that extra space can be reserved as part of
  // the request body.
  uint32_t message_size = req.ByteSize();
  sidecar_byte_size_ = 0;
  for (const unique_ptr<RpcSidecar>& car: sidecars_) {
    header_.add_sidecar_offsets(sidecar_byte_size_ + message_size);
    size_t sidecar_bytes = car->TotalSize();
    DCHECK_LE(sidecar_byte_size_, TransferLimits::kMaxTotalSidecarBytes - sidecar_bytes);
    sidecar_byte_size_ += sidecar_bytes;
  }

  serialization::SerializeMessage(req, &request_buf_, sidecar_byte_size_, true);
}

Status OutboundCall::status() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return status_;
}

const ErrorStatusPB* OutboundCall::error_pb() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return error_pb_.get();
}

string OutboundCall::StateName(State state) {
  switch (state) {
    case READY:
      return "READY";
    case ON_OUTBOUND_QUEUE:
      return "ON_OUTBOUND_QUEUE";
    case SENDING:
      return "SENDING";
    case SENT:
      return "SENT";
    case NEGOTIATION_TIMED_OUT:
      return "NEGOTIATION_TIMED_OUT";
    case TIMED_OUT:
      return "TIMED_OUT";
    case CANCELLED:
      return "CANCELLED";
    case FINISHED_NEGOTIATION_ERROR:
      return "FINISHED_NEGOTIATION_ERROR";
    case FINISHED_ERROR:
      return "FINISHED_ERROR";
    case FINISHED_SUCCESS:
      return "FINISHED_SUCCESS";
    default:
      LOG(DFATAL) << "Unknown state in OutboundCall: " << state;
      return StringPrintf("UNKNOWN(%d)", state);
  }
}

void OutboundCall::set_state(State new_state) {
  std::lock_guard<simple_spinlock> l(lock_);
  set_state_unlocked(new_state);
}

OutboundCall::State OutboundCall::state() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return state_;
}

void OutboundCall::set_state_unlocked(State new_state) {
  // Sanity check state transitions.
  DVLOG(3) << "OutboundCall " << this << " (" << ToString() << ") switching from " <<
    StateName(state_) << " to " << StateName(new_state);
  switch (new_state) {
    case ON_OUTBOUND_QUEUE:
      DCHECK_EQ(state_, READY);
      break;
    case SENDING:
      // Allow SENDING to be set idempotently so we don't have to specifically check
      // whether the state is transitioning in the RPC code.
      DCHECK(state_ == ON_OUTBOUND_QUEUE || state_ == SENDING);
      break;
    case SENT:
      DCHECK_EQ(state_, SENDING);
      break;
    case NEGOTIATION_TIMED_OUT:
      DCHECK(state_ == ON_OUTBOUND_QUEUE);
      break;
    case TIMED_OUT:
      DCHECK(state_ == SENT || state_ == ON_OUTBOUND_QUEUE || state_ == SENDING);
      break;
    case CANCELLED:
      DCHECK(state_ == READY || state_ == ON_OUTBOUND_QUEUE || state_ == SENT);
      break;
    case FINISHED_SUCCESS:
      DCHECK_EQ(state_, SENT);
      break;
    default:
      // No sanity checks for others.
      break;
  }

  state_ = new_state;
}

void OutboundCall::Cancel() {
  cancellation_requested_ = true;
  // No lock needed as it's called from reactor thread
  switch (state_) {
    case READY:
    case ON_OUTBOUND_QUEUE:
    case SENT: {
      SetCancelled();
      break;
    }
    case SENDING:
    case NEGOTIATION_TIMED_OUT:
    case TIMED_OUT:
    case CANCELLED:
    case FINISHED_NEGOTIATION_ERROR:
    case FINISHED_ERROR:
    case FINISHED_SUCCESS:
      break;
  }
}

void OutboundCall::CallCallback() {
  // Clear references to outbound sidecars before invoking callback.
  sidecars_.clear();

  int64_t start_cycles = CycleClock::Now();
  {
    SCOPED_WATCH_STACK(100);
    callback_();
    // Clear the callback, since it may be holding onto reference counts
    // via bound parameters. We do this inside the timer because it's possible
    // the user has naughty destructors that block, and we want to account for that
    // time here if they happen to run on this thread.
    callback_ = NULL;
  }
  int64_t end_cycles = CycleClock::Now();
  int64_t wait_cycles = end_cycles - start_cycles;
  if (PREDICT_FALSE(wait_cycles > FLAGS_rpc_callback_max_cycles)) {
    double micros = static_cast<double>(wait_cycles) / base::CyclesPerSecond()
      * kMicrosPerSecond;

    LOG(WARNING) << "RPC callback for " << ToString() << " blocked reactor thread for "
                 << micros << "us";
  }
}

void OutboundCall::SetResponse(unique_ptr<CallResponse> resp) {
  call_response_ = std::move(resp);
  Slice r(call_response_->serialized_response());

  if (call_response_->is_success()) {
    // TODO: here we're deserializing the call response within the reactor thread,
    // which isn't great, since it would block processing of other RPCs in parallel.
    // Should look into a way to avoid this.
    if (!response_->ParseFromArray(r.data(), r.size())) {
      SetFailed(Status::IOError("invalid RPC response, missing fields",
                                response_->InitializationErrorString()));
      return;
    }
    set_state(FINISHED_SUCCESS);
    CallCallback();
  } else {
    // Error
    unique_ptr<ErrorStatusPB> err(new ErrorStatusPB());
    if (!err->ParseFromArray(r.data(), r.size())) {
      SetFailed(Status::IOError("Was an RPC error but could not parse error response",
                                err->InitializationErrorString()));
      return;
    }
    Status s = Status::RemoteError(err->message());
    SetFailed(std::move(s), Phase::REMOTE_CALL, std::move(err));
  }
}

void OutboundCall::SetQueued() {
  set_state(ON_OUTBOUND_QUEUE);
}

void OutboundCall::SetSending() {
  set_state(SENDING);
}

void OutboundCall::SetSent() {
  set_state(SENT);

  // This method is called in the reactor thread, so free the header buf,
  // which was also allocated from this thread. tcmalloc's thread caching
  // behavior is a lot more efficient if memory is freed from the same thread
  // which allocated it -- this lets it keep to thread-local operations instead
  // of taking a mutex to put memory back on the global freelist.
  delete [] header_buf_.release();

  // request_buf_ is also done being used here, but since it was allocated by
  // the caller thread, we would rather let that thread free it whenever it
  // deletes the RpcController.

  // If cancellation was requested, it's now a good time to do the actual cancellation.
  if (cancellation_requested()) {
    SetCancelled();
  }
}

void OutboundCall::SetFailed(Status status,
                             Phase phase,
                             unique_ptr<ErrorStatusPB> err_pb) {
  DCHECK(!status.ok());
  DCHECK(phase == Phase::CONNECTION_NEGOTIATION || phase == Phase::REMOTE_CALL);
  {
    std::lock_guard<simple_spinlock> l(lock_);
    status_ = std::move(status);
    error_pb_ = std::move(err_pb);
    set_state_unlocked(phase == Phase::CONNECTION_NEGOTIATION
        ? FINISHED_NEGOTIATION_ERROR
        : FINISHED_ERROR);
  }
  CallCallback();
}

void OutboundCall::SetTimedOut(Phase phase) {
  static const char* kErrMsgNegotiation =
      "connection negotiation to $1 for RPC $0 timed out after $2 ($3)";
  static const char* kErrMsgCall = "$0 RPC to $1 timed out after $2 ($3)";
  DCHECK(phase == Phase::CONNECTION_NEGOTIATION || phase == Phase::REMOTE_CALL);

  // We have to fetch timeout outside the lock to avoid a lock
  // order inversion between this class and RpcController.
  const MonoDelta timeout = controller_->timeout();
  {
    std::lock_guard<simple_spinlock> l(lock_);
    status_ = Status::TimedOut(
        Substitute((phase == Phase::REMOTE_CALL) ? kErrMsgCall : kErrMsgNegotiation,
                   remote_method_.method_name(),
                   conn_id_.remote().ToString(),
                   timeout.ToString(),
                   StateName(state_)));
    set_state_unlocked((phase == Phase::REMOTE_CALL) ? TIMED_OUT : NEGOTIATION_TIMED_OUT);
  }
  CallCallback();
}

void OutboundCall::SetCancelled() {
  DCHECK(!IsFinished());
  {
    std::lock_guard<simple_spinlock> l(lock_);
    status_ = Status::Aborted(
        Substitute("$0 RPC to $1 is cancelled in state $2",
                   remote_method_.method_name(),
                   conn_id_.remote().ToString(),
                   StateName(state_)));
    set_state_unlocked(CANCELLED);
  }
  CallCallback();
}

bool OutboundCall::IsTimedOut() const {
  std::lock_guard<simple_spinlock> l(lock_);
  switch (state_) {
    case NEGOTIATION_TIMED_OUT:       // fall-through
    case TIMED_OUT:
      return true;
    default:
      return false;
  }
}

bool OutboundCall::IsCancelled() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return state_ == CANCELLED;
}

bool OutboundCall::IsNegotiationError() const {
  std::lock_guard<simple_spinlock> l(lock_);
  switch (state_) {
    case FINISHED_NEGOTIATION_ERROR:  // fall-through
    case NEGOTIATION_TIMED_OUT:
      return true;
    default:
      return false;
  }
}

bool OutboundCall::IsFinished() const {
  std::lock_guard<simple_spinlock> l(lock_);
  switch (state_) {
    case READY:
    case SENDING:
    case ON_OUTBOUND_QUEUE:
    case SENT:
      return false;
    case NEGOTIATION_TIMED_OUT:
    case TIMED_OUT:
    case CANCELLED:
    case FINISHED_NEGOTIATION_ERROR:
    case FINISHED_ERROR:
    case FINISHED_SUCCESS:
      return true;
    default:
      LOG(FATAL) << "Unknown call state: " << state_;
      return false;
  }
}

string OutboundCall::ToString() const {
  return Substitute("RPC call $0 -> $1", remote_method_.ToString(), conn_id_.ToString());
}

void OutboundCall::DumpPB(const DumpConnectionsRequestPB& req,
                          RpcCallInProgressPB* resp) {
  std::lock_guard<simple_spinlock> l(lock_);
  resp->mutable_header()->CopyFrom(header_);
  resp->set_micros_elapsed((MonoTime::Now() - start_time_).ToMicroseconds());

  switch (state_) {
    case READY:
      // Don't bother setting a state for "READY" since we don't expose a call
      // until it's at least on the queue of a connection.
      break;
    case ON_OUTBOUND_QUEUE:
      resp->set_state(RpcCallInProgressPB::ON_OUTBOUND_QUEUE);
      break;
    case SENDING:
      resp->set_state(RpcCallInProgressPB::SENDING);
      break;
    case SENT:
      resp->set_state(RpcCallInProgressPB::SENT);
      break;
    case NEGOTIATION_TIMED_OUT:
      resp->set_state(RpcCallInProgressPB::NEGOTIATION_TIMED_OUT);
      break;
    case TIMED_OUT:
      resp->set_state(RpcCallInProgressPB::TIMED_OUT);
      break;
    case CANCELLED:
      resp->set_state(RpcCallInProgressPB::CANCELLED);
      break;
    case FINISHED_NEGOTIATION_ERROR:
      resp->set_state(RpcCallInProgressPB::FINISHED_NEGOTIATION_ERROR);
      break;
    case FINISHED_ERROR:
      resp->set_state(RpcCallInProgressPB::FINISHED_ERROR);
      break;
    case FINISHED_SUCCESS:
      resp->set_state(RpcCallInProgressPB::FINISHED_SUCCESS);
      break;
  }
}

///
/// CallResponse
///

CallResponse::CallResponse()
 : parsed_(false) {
}

Status CallResponse::GetSidecar(int idx, Slice* sidecar) const {
  DCHECK(parsed_);
  if (idx < 0 || idx >= header_.sidecar_offsets_size()) {
    return Status::InvalidArgument(strings::Substitute(
        "Index $0 does not reference a valid sidecar", idx));
  }
  *sidecar = sidecar_slices_[idx];
  return Status::OK();
}

Status CallResponse::ParseFrom(unique_ptr<InboundTransfer> transfer) {
  CHECK(!parsed_);
  RETURN_NOT_OK(serialization::ParseMessage(transfer->data(), &header_,
                                            &serialized_response_));

  // Use information from header to extract the payload slices.
  RETURN_NOT_OK(RpcSidecar::ParseSidecars(header_.sidecar_offsets(),
          serialized_response_, sidecar_slices_));

  if (header_.sidecar_offsets_size() > 0) {
    serialized_response_ =
        Slice(serialized_response_.data(), header_.sidecar_offsets(0));
  }

  transfer_.swap(transfer);
  parsed_ = true;
  return Status::OK();
}

} // namespace rpc
} // namespace kudu
