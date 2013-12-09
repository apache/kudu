// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_RPC_RPC_CONTEXT_H
#define KUDU_RPC_RPC_CONTEXT_H

#include <string>

#include "gutil/gscoped_ptr.h"
#include "rpc/service_if.h"
#include "util/status.h"

namespace google {
namespace protobuf {
class Message;
} // namespace protobuf
} // namespace google

namespace kudu {

class Sockaddr;
class Trace;

namespace rpc {

class UserCredentials;
class InboundCall;

// The context provided to a generated ServiceIf. This provides
// methods to respond to the RPC. In the future, this will also
// include methods to access information about the caller: e.g
// authentication info, tracing info, and cancellation status.
//
// This is the server-side analogue to the RpcController class.
class RpcContext {
 public:
  // Create an RpcContext. This is called only from generated code
  // and is not a public API.
  RpcContext(InboundCall *call,
             const google::protobuf::Message *request_pb,
             google::protobuf::Message *response_pb,
             RpcMethodMetrics metrics);

  ~RpcContext();

  // Return the trace buffer for this call.
  Trace* trace();

  // Send a response to the call. The service may call this method
  // before or after returning from the original handler method,
  // and it may call this method from a different thread.
  //
  // The response should be prepared already in the response PB pointer
  // which was passed to the handler method.
  //
  // After this method returns, this RpcContext object is destroyed. The request
  // and response protobufs are also destroyed.
  void RespondSuccess();

  // Respond with an error to the client. This should not be used for general
  // application errors, but instead only for unexpected cases where the
  // client code shouldn't be expected to interpret the error.
  //
  // After this method returns, this RpcContext object is destroyed. The request
  // and response protobufs are also destroyed.
  void RespondFailure(const Status &status);

  // Return the credentials of the remote user who made this call.
  const UserCredentials& user_credentials() const;

  // Return the remote IP address and port which sent the current RPC call.
  const Sockaddr& remote_address() const;

  // A string identifying the requestor -- both the user info and the IP address.
  // Suitable for use in log messages.
  std::string requestor_string() const;

  const google::protobuf::Message *request_pb() const { return request_pb_.get(); }
  google::protobuf::Message *response_pb() const { return response_pb_.get(); }

 private:
  InboundCall *call_;
  const gscoped_ptr<const google::protobuf::Message> request_pb_;
  const gscoped_ptr<google::protobuf::Message> response_pb_;
  RpcMethodMetrics metrics_;
};

} // namespace rpc
} // namespace kudu
#endif
