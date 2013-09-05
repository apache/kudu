// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_RPC_CLIENT_CALL_H
#define KUDU_RPC_CLIENT_CALL_H

#include <string>
#include <vector>
#include <tr1/memory>

#include <glog/logging.h>

#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "rpc/constants.h"
#include "rpc/rpc_header.pb.h"
#include "rpc/response_callback.h"
#include "util/locks.h"
#include "util/net/sockaddr.h"
#include "util/slice.h"
#include "util/status.h"

namespace google {
namespace protobuf {
class Message;
} // namespace protobuf
} // namespace google

namespace kudu {
namespace rpc {

class CallResponse;
class Connection;
class InboundTransfer;
class RpcController;

// Client-side user credentials, such as a user's username & password.
// In the future, we will add Kerberos credentials.
class UserCredentials {
 public:
   UserCredentials();

  // Effective user, in cases where impersonation is supported.
  // If impersonation is not supported, this should be left empty.
  bool has_effective_user() const;
  void set_effective_user(const std::string& eff_user);
  const std::string& effective_user() const { return eff_user_; }

  // Real user.
  bool has_real_user() const;
  void set_real_user(const std::string& real_user);
  const std::string& real_user() const { return real_user_; }

  // The real user's password.
  bool has_password() const;
  void set_password(const std::string& password);
  const std::string& password() const { return password_; }

  // Copy state from another object to this one.
  void CopyFrom(const UserCredentials& other);

  // Returns a string representation of the object, not including the password field.
  std::string ToString() const;

  std::size_t HashCode() const;
  bool Equals(const UserCredentials& other) const;

 private:
  // Remember to update HashCode() and Equals() when new fields are added.
  std::string eff_user_;
  std::string real_user_;
  std::string password_;

  DISALLOW_COPY_AND_ASSIGN(UserCredentials);
};

// Used to key on Connection information.
// For use as a key in an unordered STL collection, use ConnectionIdHash and ConnectionIdEqual.
// This class is copyable for STL compatibility, but not assignable (use CopyFrom() for that).
class ConnectionId {
 public:
  ConnectionId();

  // Copy constructor required for use with STL unordered_map.
  ConnectionId(const ConnectionId& other);

  // Convenience constructor.
  ConnectionId(const Sockaddr& remote, const std::string& service_name, const UserCredentials& user_cred);

  // The remote address.
  void set_remote(const Sockaddr& remote);
  const Sockaddr& remote() const { return remote_; }

  // The identifying name of the RPC service.
  void set_service_name(const std::string& service_name);
  const std::string& service_name() const { return service_name_; }

  // The credentials of the user associated with this connection, if any.
  void set_user_cred(const UserCredentials& user_cred);
  const UserCredentials& user_cred() const { return user_cred_; }
  UserCredentials* mutable_user_cred() { return &user_cred_; }

  // Copy state from another object to this one.
  void CopyFrom(const ConnectionId& other);

  // Returns a string representation of the object, not including the password field.
  std::string ToString() const;

  size_t HashCode() const;
  bool Equals(const ConnectionId& other) const;

 private:
  // Remember to update HashCode() and Equals() when new fields are added.
  Sockaddr remote_;
  std::string service_name_;
  UserCredentials user_cred_;

  // Implementation of CopyFrom that can be shared with copy constructor.
  void DoCopyFrom(const ConnectionId& other);

  // Disable assignment operator.
  void operator=(const ConnectionId&);
};

class ConnectionIdHash {
 public:
  std::size_t operator() (const ConnectionId& conn_id) const;
};

class ConnectionIdEqual {
 public:
  bool operator() (const ConnectionId& cid1, const ConnectionId& cid2) const;
};

// Tracks the status of a call on the client side.
//
// This is an internal-facing class -- clients interact with the
// RpcController class.
//
// This is allocated by the Proxy when a call is first created,
// then passed to the reactor thread to send on the wire. It's typically
// kept using a shared_ptr because a call may terminate in any number
// of different threads, making it tricky to enforce single ownership.
class OutboundCall {
 public:

  OutboundCall(const ConnectionId& conn_id,
               const string& method,
               google::protobuf::Message* response_storage,
               RpcController* controller,
               const ResponseCallback& callback);

  ~OutboundCall();

  // Serialize the given request PB into this call's internal storage.
  //
  // Because the data is fully serialized by this call, 'req' may be
  // subsequently mutated with no ill effects.
  Status SetRequestParam(const google::protobuf::Message& req);

  // Assign the call ID for this call. This is called from the reactor
  // thread once a connection has been assigned. Must only be called once.
  void set_call_id(int32_t call_id) {
    DCHECK_EQ(call_id_, kInvalidCallId) << "Already has a call ID";
    call_id_ = call_id;
  }

  // Serialize the call for the wire. Requires that SetRequestParam()
  // is called first. This is called from the Reactor thread.
  Status SerializeTo(std::vector<Slice>* slices);

  // Callback after the call has been put on the outbound connection queue.
  void SetQueued();

  // Update the call state to show that the request has been sent.
  void SetSent();

  // Mark the call as failed. This also triggers the callback to notify
  // the caller.
  void SetFailed(const Status& status);

  // Mark the call as timed out. This also triggers the callback to notify
  // the caller.
  void SetTimedOut();
  bool IsTimedOut() const;

  // Is the call finished?
  bool IsFinished() const;

  // Fill in the call response.
  void SetResponse(gscoped_ptr<CallResponse> resp);

  std::string ToString() const;


  ////////////////////////////////////////////////////////////
  // Getters
  ////////////////////////////////////////////////////////////

  const ConnectionId& conn_id() const { return conn_id_; }
  const std::string& method() const { return method_; }
  const ResponseCallback &callback() const { return callback_; }
  RpcController* controller() { return controller_; }
  const RpcController* controller() const { return controller_; }

  // Return true if a call ID has been assigned to this call.
  bool call_id_assigned() const {
    return call_id_ != kInvalidCallId;
  }

  int32_t call_id() const {
    DCHECK(call_id_assigned());
    return call_id_;
  }

 private:
  friend class RpcController;

  // Various states the call propagates through.
  // NB: if adding another state, be sure to update OutboundCall::IsFinished()
  // and OutboundCall::StateName(State state) as well.
  enum State {
    READY = 0,
    ON_OUTBOUND_QUEUE = 1,
    SENT = 2,
    TIMED_OUT = 3,
    FINISHED_ERROR = 4,
    FINISHED_SUCCESS = 5
  };

  static string StateName(State state);

  void set_state(State new_state);
  State state() const;

  // Same as set_state, but requires that the caller already holds
  // lock_
  void set_state_unlocked(State new_state);

  // return current status
  Status status() const;

  // Lock for state_ and status_ fields, since they
  // may be mutated by the reactor thread while the client thread
  // reads them.
  mutable simple_spinlock lock_;
  State state_;
  Status status_;

  // Call the user-provided callback.
  void CallCallback();

  ConnectionId conn_id_;
  std::string method_;
  ResponseCallback callback_;
  RpcController* controller_;

  // Pointer for the protobuf where the response should be written.
  google::protobuf::Message* response_;

  // Call ID -- only assigned once this call has been passed to the reactor
  // thread and assigned a connection.
  int32_t call_id_;

  // Buffers for storing segments of the wire-format request.
  faststring header_buf_;
  faststring request_buf_;

  // Once a response has been received for this call, contains that response.
  // Otherwise NULL.
  gscoped_ptr<CallResponse> call_response_;

  DISALLOW_COPY_AND_ASSIGN(OutboundCall);
};

// A response to a call, on the client side.
// Upon receiving a response, this is allocated in the reactor thread and filled
// into the OutboundCall instance via OutboundCall::SetResponse.
//
// This may either be a success or error response.
class CallResponse {
 public:
  CallResponse();

  // Parse the response received from a call. This must be called before any
  // other methods on this object.
  Status ParseFrom(gscoped_ptr<InboundTransfer> transfer);

  // Return true if the call succeeded.
  bool is_success() const {
    DCHECK(parsed_);
    return !header_.is_error();
  }

  // Return the call ID that this response is related to.
  int32_t call_id() const {
    DCHECK(parsed_);
    return header_.callid();
  }

  // Return the serialized response data. This is just the response "body" --
  // either a serialized ErrorStatusPB, or the serialized user response protobuf.
  const Slice &serialized_response() const {
    DCHECK(parsed_);
    return serialized_response_;
  }

 private:
  // True once ParseFrom() is called.
  bool parsed_;

  // The parsed header.
  ResponseHeader header_;

  // The slice of data for the encoded protobuf response.
  // This slice refers to memory allocated by transfer_
  Slice serialized_response_;

  // The incoming transfer data - retained because serialized_response_
  // refers into its data.
  gscoped_ptr<InboundTransfer> transfer_;

  DISALLOW_COPY_AND_ASSIGN(CallResponse);
};

} // namespace rpc
} // namespace kudu

#endif
