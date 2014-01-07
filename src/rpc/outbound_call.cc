// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <string>
#include <vector>
#include <boost/functional/hash.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>

#include "gutil/stringprintf.h"
#include "rpc/outbound_call.h"
#include "rpc/constants.h"
#include "rpc/rpc_controller.h"
#include "rpc/serialization.h"
#include "rpc/transfer.h"

namespace kudu {
namespace rpc {

using google::protobuf::Message;
using google::protobuf::io::CodedOutputStream;

///
/// OutboundCall
///

OutboundCall::OutboundCall(const ConnectionId& conn_id,
                           const string& method,
                           google::protobuf::Message* response_storage,
                           RpcController* controller,
                           const ResponseCallback& callback)
  : state_(READY),
    conn_id_(conn_id),
    method_(method),
    callback_(callback),
    controller_(DCHECK_NOTNULL(controller)),
    response_(DCHECK_NOTNULL(response_storage)),
    call_id_(kInvalidCallId) {
  DVLOG(4) << "OutboundCall " << this << " constructed with state_: " << StateName(state_)
           << " and RPC timeout: " << controller->timeout().ToString();
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
  header.set_call_id(call_id());
  header.set_method_name(method());
  header.set_timeout_millis(controller_->timeout().ToMilliseconds());

  CHECK_OK(serialization::SerializeHeader(header, param_len, &header_buf_));

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

const ErrorStatusPB* OutboundCall::error_pb() const {
  boost::lock_guard<simple_spinlock> l(lock_);
  return error_pb_.get();
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
      SetFailed(Status::IOError("Invalid response, missing fields",
                                response_->InitializationErrorString()));
      return;
    }
    set_state(FINISHED_SUCCESS);
    CallCallback();
  } else {
    // Error
    gscoped_ptr<ErrorStatusPB> err(new ErrorStatusPB());
    if (!err->ParseFromArray(r.data(), r.size())) {
      SetFailed(Status::IOError("Was an RPC error but could not parse error response",
                                err->InitializationErrorString()));
      return;
    }
    ErrorStatusPB* err_raw = err.release();
    SetFailed(Status::RemoteError(err_raw->message()), err_raw);
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
  delete [] header_buf_.release();

  // request_buf_ is also done being used here, but since it was allocated by
  // the caller thread, we would rather let that thread free it whenever it
  // deletes the RpcController.
}

void OutboundCall::SetFailed(const Status &status,
                             ErrorStatusPB* err_pb) {
  {
    boost::lock_guard<simple_spinlock> l(lock_);
    status_ = status;
    if (status_.IsRemoteError()) {
      CHECK(err_pb);
      error_pb_.reset(err_pb);
    } else {
      CHECK(!err_pb);
    }
    set_state_unlocked(FINISHED_ERROR);
  }
  CallCallback();
}

void OutboundCall::SetTimedOut() {
  {
    boost::lock_guard<simple_spinlock> l(lock_);
    // TODO: have a better error message which includes the call timeout,
    // remote host, how long it spent in the queue, other useful stuff.
    status_ = Status::TimedOut("Call timed out");
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
                      method_.c_str(), conn_id_.ToString().c_str());
}

///
/// UserCredentials
///

UserCredentials::UserCredentials() {}

bool UserCredentials::has_effective_user() const {
  return !eff_user_.empty();
}

void UserCredentials::set_effective_user(const string& eff_user) {
  eff_user_ = eff_user;
}

bool UserCredentials::has_real_user() const {
  return !real_user_.empty();
}

void UserCredentials::set_real_user(const string& real_user) {
  real_user_ = real_user;
}

bool UserCredentials::has_password() const {
  return !password_.empty();
}

void UserCredentials::set_password(const string& password) {
  password_ = password;
}

void UserCredentials::CopyFrom(const UserCredentials& other) {
  eff_user_ = other.eff_user_;
  real_user_ = other.real_user_;
  password_ = other.password_;
}

string UserCredentials::ToString() const {
  // Does not print the password.
  return StringPrintf("{real_user=%s, eff_user=%s}", real_user_.c_str(), eff_user_.c_str());
}

size_t UserCredentials::HashCode() const {
  size_t seed = 0;
  if (has_effective_user()) {
    boost::hash_combine(seed, effective_user());
  }
  if (has_real_user()) {
    boost::hash_combine(seed, real_user());
  }
  if (has_password()) {
    boost::hash_combine(seed, password());
  }
  return seed;
}

bool UserCredentials::Equals(const UserCredentials& other) const {
  return (effective_user() == other.effective_user()
       && real_user() == other.real_user()
       && password() == other.password());
}

///
/// ConnectionId
///

ConnectionId::ConnectionId() {}

ConnectionId::ConnectionId(const ConnectionId& other) {
  DoCopyFrom(other);
}

ConnectionId::ConnectionId(const Sockaddr& remote, const string& service_name,
                           const UserCredentials& user_credentials) {
  remote_ = remote;
  service_name_ = service_name;
  user_credentials_.CopyFrom(user_credentials);
}

void ConnectionId::set_remote(const Sockaddr& remote) {
  remote_ = remote;
}

void ConnectionId::set_service_name(const string& service_name) {
  service_name_ = service_name;
}

void ConnectionId::set_user_credentials(const UserCredentials& user_credentials) {
  user_credentials_.CopyFrom(user_credentials);
}

void ConnectionId::CopyFrom(const ConnectionId& other) {
  DoCopyFrom(other);
}

string ConnectionId::ToString() const {
  // Does not print the password.
  return StringPrintf("{remote=%s, service_name=%s, user_credentials=%s}",
      remote_.ToString().c_str(), service_name_.c_str(),
      user_credentials_.ToString().c_str());
}

void ConnectionId::DoCopyFrom(const ConnectionId& other) {
  remote_ = other.remote_;
  service_name_ = other.service_name_;
  user_credentials_.CopyFrom(other.user_credentials_);
}

size_t ConnectionId::HashCode() const {
  size_t seed = 0;
  boost::hash_combine(seed, remote_.HashCode());
  boost::hash_combine(seed, service_name_);
  boost::hash_combine(seed, user_credentials_.HashCode());
  return seed;
}

bool ConnectionId::Equals(const ConnectionId& other) const {
  return (remote() == other.remote()
       && service_name() == other.service_name()
       && user_credentials().Equals(other.user_credentials()));
}

size_t ConnectionIdHash::operator() (const ConnectionId& conn_id) const {
  return conn_id.HashCode();
}

bool ConnectionIdEqual::operator() (const ConnectionId& cid1, const ConnectionId& cid2) const {
  return cid1.Equals(cid2);
}

///
/// CallResponse
///

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
