// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_RPC_INBOUND_CALL_H
#define KUDU_RPC_INBOUND_CALL_H

#include <glog/logging.h>
#include <tr1/memory>

#include <string>
#include <vector>

#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "rpc/rpc_header.pb.h"
#include "rpc/transfer.h"
#include "util/faststring.h"
#include "util/monotime.h"
#include "util/slice.h"
#include "util/status.h"

namespace google {
namespace protobuf {
class Message;
} // namespace protobuf
} // namespace google

namespace kudu {

class Histogram;
class Trace;

namespace rpc {

class Connection;
class UserCredentials;

struct InboundCallTiming {
  MonoTime time_received;   // Time the call was first accepted.
  MonoTime time_handled;    // Time the call handler was kicked off.
  MonoTime time_completed;  // Time the call handler completed.
};

// Inbound call on server
class InboundCall {
 public:
  explicit InboundCall(const std::tr1::shared_ptr<Connection>& conn);
  ~InboundCall();

  // Parse an inbound call message.
  //
  // This only deserializes the call header, populating the 'header_' and
  // 'serialized_request_' member variables. The actual call parameter is
  // not deserialized, as this may be CPU-expensive, and this is called
  // from the reactor thread.
  Status ParseFrom(gscoped_ptr<InboundTransfer> transfer);

  // Return the serialized request parameter protobuf.
  const Slice &serialized_request() const {
    return serialized_request_;
  }

  const std::string &method_name() const {
    return header_.method_name();
  }

  const int32_t call_id() const {
    return header_.call_id();
  }

  // Serializes 'response' into the InboundCall's internal buffer, and marks
  // the call as a success. Enqueues the response back to the connection
  // that made the call.
  //
  // This method deletes the InboundCall object, so no further calls may be
  // made after this one.
  void RespondSuccess(const google::protobuf::MessageLite& response);

  // Serializes a failure response into the internal buffer, marking the
  // call as a failure. Enqueues the response back to the connection that
  // made the call.
  //
  // This method deletes the InboundCall object, so no further calls may be
  // made after this one.
  void RespondFailure(ErrorStatusPB::RpcErrorCodePB error_code,
                      const Status &status);

  void RespondApplicationError(int error_ext_id, const std::string& message,
                               const google::protobuf::MessageLite& app_error_pb);

  // Serialize the response packet for the finished call.
  // The resulting slices refer to memory in this object.
  void SerializeResponseTo(std::vector<Slice>* slices) const;

  std::string ToString() const;

  const UserCredentials& user_credentials() const;

  const Sockaddr& remote_address() const;

  Trace* trace();

  // When this InboundCall was received (instantiated).
  // Should only be called once on a given instance.
  // Not thread-safe. Should only be called by the current "owner" thread.
  void RecordCallReceived();

  // When RPC call Handle() was called on the server side.
  // Updates the Histogram with time elapsed since the call was received,
  // and should only be called once on a given instance.
  // Not thread-safe. Should only be called by the current "owner" thread.
  void RecordHandlingStarted(Histogram* incoming_queue_time);

  // When RPC call Handle() completed execution on the server side.
  // Updates the Histogram with time elapsed since the call was started,
  // and should only be called once on a given instance.
  // Not thread-safe. Should only be called by the current "owner" thread.
  void RecordHandlingCompleted(Histogram* handler_run_time);

 private:
  // Serialize and queue the response.
  void Respond(const google::protobuf::MessageLite& response,
               bool is_success);

  // Serialize a response message for either success or failure. If it is a success,
  // 'response' should be the user-defined response type for the call. If it is a
  // failure, 'response' should be an ErrorStatusPB instance.
  Status SerializeResponseBuffer(const google::protobuf::MessageLite& response,
                                 bool is_success);

  // Log a WARNING message if the RPC response was slow enough that the
  // client likely timed out. This is based on the client-provided timeout
  // value.
  void LogIfSlow() const;

  // The connection on which this inbound call arrived.
  std::tr1::shared_ptr<Connection> conn_;

  // The header of the incoming call. Set by ParseFrom()
  RequestHeader header_;

  // The serialized bytes of the request param protobuf. Set by ParseFrom().
  // This references memory held by 'transfer_'.
  Slice serialized_request_;

  // The transfer that produced the call.
  // This is kept around because it retains the memory referred to
  // by 'serialized_request_' above.
  gscoped_ptr<InboundTransfer> transfer_;

  // The buffers for serialized response. Set by SerializeResponseBuffer().
  faststring response_hdr_buf_;
  faststring response_msg_buf_;

  // The trace buffer.
  gscoped_ptr<Trace> trace_;

  // Timing information related to this RPC call.
  InboundCallTiming timing_;

  DISALLOW_COPY_AND_ASSIGN(InboundCall);
};

} // namespace rpc
} // namespace kudu

#endif
