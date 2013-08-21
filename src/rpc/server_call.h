// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_RPC_SERVER_CALL_H
#define KUDU_RPC_SERVER_CALL_H

#include <glog/logging.h>
#include <tr1/memory>

#include <string>
#include <vector>

#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "rpc/rpc_header.pb.h"
#include "rpc/transfer.h"
#include "util/faststring.h"
#include "util/slice.h"
#include "util/status.h"

namespace google { namespace protobuf {
class Message;
}
}

namespace kudu { namespace rpc {

class Connection;

// Inbound call on server
class InboundCall {
 public:
  explicit InboundCall(const std::tr1::shared_ptr<Connection> &conn);

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
    return header_.methodname();
  }

  const int32_t call_id() const {
    return header_.callid();
  }

  // Serializes 'response' into the InboundCall's internal buffer, and marks
  // the call as a success. Enqueues the response back to the connection
  // that made the call.
  //
  // This method deletes the InboundCall object, so no further calls may be
  // made after this one.
  void RespondSuccess(const google::protobuf::MessageLite &response);

  // Serializes a failure response into the internal buffer, marking the
  // call as a failure. Enqueues the response back to the connection that
  // made the call.
  //
  // This method deletes the InboundCall object, so no further calls may be
  // made after this one.
  void RespondFailure(const Status &status);

  // Serialize the response packet for the finished call.
  // The resulting slices refer to memory in this object.
  Status SerializeResponseTo(std::vector<Slice> *slices) const;

  std::string ToString() const;

 private:
  // Serialize a response message for either success or failure. If it is a success,
  // 'response' should be the user-defined response type for the call. If it is a
  // failure, 'response' should be an ErrorStatusPB instance.
  Status SerializeResponseBuffer(const google::protobuf::MessageLite &response,
                                 bool is_success);

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

  DISALLOW_COPY_AND_ASSIGN(InboundCall);
};

} // namespace rpc
} // namespace kudu

#endif
