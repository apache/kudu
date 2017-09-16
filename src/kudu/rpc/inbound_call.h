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
#ifndef KUDU_RPC_INBOUND_CALL_H
#define KUDU_RPC_INBOUND_CALL_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/remote_method.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/service_if.h"
#include "kudu/rpc/transfer.h"
#include "kudu/util/faststring.h"
#include "kudu/util/monotime.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace google {
namespace protobuf {
class MessageLite;
} // namespace protobuf
} // namespace google

namespace kudu {

class Histogram;
class Sockaddr;
class Trace;

namespace rpc {

class Connection;
class DumpRunningRpcsRequestPB;
class RemoteUser;
class RpcCallInProgressPB;
class RpcSidecar;

struct InboundCallTiming {
  MonoTime time_received;   // Time the call was first accepted.
  MonoTime time_handled;    // Time the call handler was kicked off.
  MonoTime time_completed;  // Time the call handler completed.

  MonoDelta TotalDuration() const {
    return time_completed - time_received;
  }
};

// Inbound call on server
class InboundCall {
 public:
  explicit InboundCall(Connection* conn);
  ~InboundCall();

  // Parse an inbound call message.
  //
  // This only deserializes the call header, populating the 'header_' and
  // 'serialized_request_' member variables. The actual call parameter is
  // not deserialized, as this may be CPU-expensive, and this is called
  // from the reactor thread.
  Status ParseFrom(gscoped_ptr<InboundTransfer> transfer);

  // Return the serialized request parameter protobuf.
  const Slice& serialized_request() const {
    DCHECK(transfer_) << "Transfer discarded before parameter parsing";
    return serialized_request_;
  }

  const RemoteMethod& remote_method() const {
    return remote_method_;
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

  void RespondUnsupportedFeature(const std::vector<uint32_t>& unsupported_features);

  void RespondApplicationError(int error_ext_id, const std::string& message,
                               const google::protobuf::MessageLite& app_error_pb);

  // Convert an application error extension to an ErrorStatusPB.
  // These ErrorStatusPB objects are what are returned in application error responses.
  static void ApplicationErrorToPB(int error_ext_id, const std::string& message,
                                   const google::protobuf::MessageLite& app_error_pb,
                                   ErrorStatusPB* err);

  // Serialize the response packet for the finished call into 'slices'.
  // The resulting slices refer to memory in this object.
  // Returns the number of slices in the serialized response.
  size_t SerializeResponseTo(TransferPayload* slices) const;

  // See RpcContext::AddRpcSidecar()
  Status AddOutboundSidecar(std::unique_ptr<RpcSidecar> car, int* idx);

  std::string ToString() const;

  void DumpPB(const DumpRunningRpcsRequestPB& req, RpcCallInProgressPB* resp);

  const RemoteUser& remote_user() const;

  const Sockaddr& remote_address() const;

  const scoped_refptr<Connection>& connection() const;

  Trace* trace();

  const InboundCallTiming& timing() const {
    return timing_;
  }

  const RequestHeader& header() const {
    return header_;
  }

  // Associate this call with a particular method that will be invoked
  // by the service.
  void set_method_info(scoped_refptr<RpcMethodInfo> info) {
    method_info_ = std::move(info);
  }

  // Return the method associated with this call. This is set just before
  // the call is enqueued onto the service queue, and therefore may be
  // 'nullptr' for much of the lifecycle of a call.
  RpcMethodInfo* method_info() {
    return method_info_.get();
  }

  // When this InboundCall was received (instantiated).
  // Should only be called once on a given instance.
  // Not thread-safe. Should only be called by the current "owner" thread.
  void RecordCallReceived();

  // When RPC call Handle() was called on the server side.
  // Updates the Histogram with time elapsed since the call was received,
  // and should only be called once on a given instance.
  // Not thread-safe. Should only be called by the current "owner" thread.
  void RecordHandlingStarted(scoped_refptr<Histogram> incoming_queue_time);

  // Return true if the deadline set by the client has already elapsed.
  // In this case, the server may stop processing the call, since the
  // call response will be ignored anyway.
  bool ClientTimedOut() const;

  // Return an upper bound on the client timeout deadline. This does not
  // account for transmission delays between the client and the server.
  // If the client did not specify a deadline, returns MonoTime::Max().
  MonoTime GetClientDeadline() const;

  // Return the time when this call was received.
  MonoTime GetTimeReceived() const;

  // Returns the set of application-specific feature flags required to service
  // the RPC.
  std::vector<uint32_t> GetRequiredFeatures() const;

  // Get a sidecar sent as part of the request. If idx < 0 || idx > num sidecars - 1,
  // returns an error.
  Status GetInboundSidecar(int idx, Slice* sidecar) const;

  // Releases the buffer that contains the request + sidecar data. It is an error to
  // access sidecars or serialized_request() after this method is called.
  void DiscardTransfer();

  // Returns the size of the transfer buffer that backs this call. If the transfer does
  // not exist (e.g. GetTransferSize() is called after DiscardTransfer()), returns 0.
  size_t GetTransferSize();

 private:
  friend class RpczStore;

  // Serialize and queue the response.
  void Respond(const google::protobuf::MessageLite& response,
               bool is_success);

  // Serialize a response message for either success or failure. If it is a success,
  // 'response' should be the user-defined response type for the call. If it is a
  // failure, 'response' should be an ErrorStatusPB instance.
  void SerializeResponseBuffer(const google::protobuf::MessageLite& response,
                               bool is_success);

  // When RPC call Handle() completed execution on the server side.
  // Updates the Histogram with time elapsed since the call was started,
  // and should only be called once on a given instance.
  // Not thread-safe. Should only be called by the current "owner" thread.
  void RecordHandlingCompleted();

  // The connection on which this inbound call arrived.
  scoped_refptr<Connection> conn_;

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

  // Vector of additional sidecars that are tacked on to the call's response
  // after serialization of the protobuf. See rpc/rpc_sidecar.h for more info.
  std::vector<std::unique_ptr<RpcSidecar>> outbound_sidecars_;

  // Inbound sidecars from the request. The slices are views onto transfer_. There are as
  // many slices as header_.sidecar_offsets_size().
  Slice inbound_sidecar_slices_[TransferLimits::kMaxSidecars];

  // The trace buffer.
  scoped_refptr<Trace> trace_;

  // Timing information related to this RPC call.
  InboundCallTiming timing_;

  // Proto service this calls belongs to. Used for routing.
  // This field is filled in when the inbound request header is parsed.
  RemoteMethod remote_method_;

  // After the method has been looked up within the service, this is filled in
  // to point to the information about this method. Acts as a pointer back to
  // per-method info such as tracing.
  scoped_refptr<RpcMethodInfo> method_info_;

  DISALLOW_COPY_AND_ASSIGN(InboundCall);
};

} // namespace rpc
} // namespace kudu

#endif
