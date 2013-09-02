// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <boost/thread/mutex.hpp>
#include <string>
#include <vector>

#include "gutil/stringprintf.h"
#include "rpc/client_call.h"
#include "rpc/constants.h"
#include "rpc/serialization.h"
#include "rpc/transfer.h"

namespace kudu {
namespace rpc {

using google::protobuf::Message;
using google::protobuf::io::CodedOutputStream;

OutboundCall::OutboundCall(const Sockaddr& remote,
                           const string& method,
                           google::protobuf::Message* response_storage,
                           RpcController* controller,
                           const ResponseCallback& callback)
  : state_(READY),
    remote_(remote),
    method_(method),
    callback_(callback),
    controller_(DCHECK_NOTNULL(controller)),
    response_(DCHECK_NOTNULL(response_storage)),
    call_id_(kInvalidCallId) {
  DVLOG(4) << "OutboundCall " << this << " constructed with state_: " << StateName(state_);
}

OutboundCall::~OutboundCall() {
  DCHECK(IsFinished());
  DVLOG(4) << "OutboundCall " << this << " destroyed with state_: " << StateName(state_);
}

Status OutboundCall::SerializeTo(vector<Slice>* slices) {
  size_t param_len = request_buf_.size();
  if (PREDICT_FALSE(param_len == 0)) {
    return Status::InvalidArgument("Must call SetRequestParam() before SerializeTo()");
  }

  RequestHeader header;
  header.set_callid(call_id());
  header.set_methodname(method());

  serialization::SerializeHeader(header, param_len, &header_buf_);

  // Return the concatenated packet.
  slices->push_back(Slice(header_buf_));
  slices->push_back(Slice(request_buf_));
  return Status::OK();
}

Status OutboundCall::SetRequestParam(const Message& message) {
  return serialization::SerializeMessage(message, &request_buf_);
}

Status OutboundCall::status() const {
  boost::lock_guard<simple_spinlock> l(lock_);
  return status_;
}

string OutboundCall::StateName(State state) {
  switch (state) {
    case READY:
      return "READY";
    case ON_OUTBOUND_QUEUE:
      return "ON_OUTBOUND_QUEUE";
    case SENT:
      return "SENT";
    case TIMED_OUT:
      return "TIMED_OUT";
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
  boost::lock_guard<simple_spinlock> l(lock_);
  set_state_unlocked(new_state);
}

OutboundCall::State OutboundCall::state() const {
  boost::lock_guard<simple_spinlock> l(lock_);
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
    case SENT:
      DCHECK_EQ(state_, ON_OUTBOUND_QUEUE);
      break;
    case TIMED_OUT:
      DCHECK(state_ == SENT || state_ == ON_OUTBOUND_QUEUE);
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

void OutboundCall::CallCallback() {
  callback_();
}

void OutboundCall::SetResponse(gscoped_ptr<CallResponse> resp) {
  Slice r(resp->serialized_response());

  if (resp->is_success()) {

    // TODO: here we're deserializing the call response within the reactor thread,
    // which isn't great, since it would block processing of other RPCs in parallel.
    // Should look into a way to avoid this.
    if (!response_->ParseFromArray(r.data(), r.size())) {
      SetFailed(Status::IOError("Invalid response, missing fields", response_->InitializationErrorString()));
      return;
    }
    set_state(FINISHED_SUCCESS);
    CallCallback();
  } else {
    // Error
    ErrorStatusPB err;
    if (!err.ParseFromArray(r.data(), r.size())) {
      SetFailed(Status::IOError("Was an RPC error but could not parse error response",
                                err.InitializationErrorString()));
      return;
    }

    SetFailed(Status::RuntimeError("RPC error", err.message()));
  }
}

void OutboundCall::SetQueued() {
  set_state(ON_OUTBOUND_QUEUE);
}

void OutboundCall::SetSent() {
  set_state(SENT);

  // This method is called in the reactor thread, so free the header buf,
  // which was also allocated from this thread. tcmalloc's thread caching
  // behavior is a lot more efficient if memory is freed from the same thread
  // which allocated it -- this lets it keep to thread-local operations instead
  // of taking a mutex to put memory back on the global freelist.

  // TODO: Uncomment this once the faststring::release() impl is committed.
  //header_buf_.release();

  // request_buf_ is also done being used here, but since it was allocated by
  // the caller thread, we would rather let that thread free it whenever it
  // deletes the RpcController.
}

void OutboundCall::SetFailed(const Status &status) {
  {
    boost::lock_guard<simple_spinlock> l(lock_);
    status_ = status;
    set_state_unlocked(FINISHED_ERROR);
  }
  CallCallback();
}

void OutboundCall::SetTimedOut() {
  {
    boost::lock_guard<simple_spinlock> l(lock_);
    // TODO: have a better error message which includes the call timeout,
    // remote host, how long it spent in the queue, other useful stuff.
    status_ = Status::RuntimeError("Call timed out");
    set_state_unlocked(TIMED_OUT);
  }
  CallCallback();
}

bool OutboundCall::IsTimedOut() const {
  boost::lock_guard<simple_spinlock> l(lock_);
  return state_ == TIMED_OUT;
}

bool OutboundCall::IsFinished() const {
  boost::lock_guard<simple_spinlock> l(lock_);
  switch (state_) {
    case READY:
    case ON_OUTBOUND_QUEUE:
    case SENT:
      return false;
    case TIMED_OUT:
    case FINISHED_ERROR:
    case FINISHED_SUCCESS:
      return true;
    default:
      LOG(FATAL) << "Unknown call state: " << state_;
      return false;
  }
}

string OutboundCall::ToString() const {
  return StringPrintf("RPC call %s -> %s",
                      method_.c_str(), remote_.ToString().c_str());
}

CallResponse::CallResponse()
 : parsed_(false) {
}

Status CallResponse::ParseFrom(gscoped_ptr<InboundTransfer> transfer) {
  CHECK(!parsed_);
  RETURN_NOT_OK(serialization::ParseMessage(transfer->data(), &header_, &serialized_response_));
  transfer_.swap(transfer);
  parsed_ = true;
  return Status::OK();
}

} // namespace rpc
} // namespace kudu
