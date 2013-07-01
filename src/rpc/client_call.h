// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_RPC_CLIENT_CALL_H
#define KUDU_RPC_CLIENT_CALL_H

#include <glog/logging.h>
#include <string>
#include <tr1/memory>

#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "rpc/rpc_header.pb.h"
#include "rpc/response_callback.h"
#include "rpc/sockaddr.h"
#include "util/slice.h"
#include "util/status.h"

namespace google { namespace protobuf {
class Message;
}
}

namespace kudu { namespace rpc {

class CallResponse;
class Connection;
class InboundTransfer;
class RpcController;

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
  static const uint64_t INVALID_CALL_ID = 0;

  OutboundCall(const Sockaddr &remote,
               const string &method,
               google::protobuf::Message *response_storage,
               RpcController *controller,
               const ResponseCallback &callback);

  // Serialize the given request PB into this call's internal storage.
  //
  // Because the data is fully serialized by this call, 'req' may be
  // subsequently mutated with no ill effects.
  Status SetRequestParam(const google::protobuf::Message &req);

  // Assign the call ID for this call. This is called from the reactor
  // thread once a connection has been assigned. Must only be called once.
  void set_call_id(uint64_t call_id) {
    DCHECK_EQ(call_id_, INVALID_CALL_ID) << "Already has a call ID";
    call_id_ = call_id;
  }

  // Serialize the call for the wire. Requires that SetRequestParam()
  // is called first. This is called from the Reactor thread.
  Status SerializeTo(std::vector<Slice> *slices);

  // Callback after the call has been put on the outbound connection queue.
  void CallQueued();

  // Callback after the call has been sent.
  // Used to update the controller state
  void CallSent();

  // Mark the call as failed. This also triggers the callback to notify
  // the caller.
  void SetFailed(const Status &status);

  // Mark the call as timed out. This also triggers the callback to notify
  // the caller.
  void TimedOut();

  // Fill in the call response.
  void SetResponse(gscoped_ptr<CallResponse> resp);

  std::string ToString() const;


  ////////////////////////////////////////////////////////////
  // Getters
  ////////////////////////////////////////////////////////////

  const Sockaddr &remote() const { return remote_; }
  const std::string &method() const { return method_; }
  const ResponseCallback &callback() const { return callback_; }
  RpcController *controller() { return controller_; }
  const RpcController *controller() const { return controller_; }

  // Return true if a call ID has been assigned to this call.
  bool call_id_assigned() const {
    return call_id_ != INVALID_CALL_ID;
  }

  // Return the serialized request, including the varint length delimiter
  Slice serialized_request() const {
    DCHECK_GT(request_size_, 0);
    return Slice(request_buf_.get(), request_size_);
  }

  uint64_t call_id() const {
    DCHECK(call_id_assigned());
    return call_id_;
  }

 private:

  // Call the user-provided callback.
  void CallCallback();

  Sockaddr remote_;
  std::string method_;
  ResponseCallback callback_;
  RpcController *controller_;

  // Pointer for the protobuf where the response should be written.
  google::protobuf::Message *response_;

  // Call ID -- only assigned once this call has been passed to the reactor
  // thread and assigned a connection.
  uint64_t call_id_;

  // Buffers for storing segments of the wire-format request.
  gscoped_ptr<uint8_t[]> header_buf_;
  gscoped_ptr<uint8_t[]> request_buf_;
  size_t request_size_;

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
  uint64_t call_id() const {
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
