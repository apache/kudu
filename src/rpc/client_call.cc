// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <vector>

#include "rpc/call-inl.h"
#include "rpc/client_call.h"
#include "rpc/rpc_controller.h"

namespace kudu { namespace rpc {

using google::protobuf::Message;
using google::protobuf::MessageLite;
using google::protobuf::io::CodedOutputStream;

OutboundCall::OutboundCall(const Sockaddr &remote,
                           const string &method,
                           google::protobuf::Message *response_storage,
                           RpcController *controller,
                           const ResponseCallback &callback)
  : remote_(remote),
    method_(method),
    callback_(callback),
    controller_(DCHECK_NOTNULL(controller)),
    response_(DCHECK_NOTNULL(response_storage)),
    call_id_(INVALID_CALL_ID),
    request_size_(0)
{}

Status OutboundCall::SerializeTo(vector<Slice> *slices) {
  DCHECK(serialized_request().data() != NULL) <<
    "Must call SetRequestParam() before SerializeTo()";

  RequestHeader header;
  header.set_callid(call_id());
  header.set_methodname(method());
  DCHECK(header.IsInitialized());

  // Compute all the lengths for the packet.
  int header_pb_len = header.ByteSize();
  size_t header_len = InboundTransfer::kLengthPrefixLength // for the int prefix for the totallength
    + CodedOutputStream::VarintSize32(header_pb_len) // for the varint delimiter for header PB
    + header_pb_len; // for the header PB itself
  int param_len = serialized_request().size();
  int total_size = header_len + param_len;

  // Prepare the header.
  header_buf_.reset(new uint8_t[header_len]);
  uint8_t *dst = header_buf_.get();

  // 1. The length for the whole request, not including the 4-byte
  // length prefix.
  NetworkByteOrder::Store32(dst, total_size - 4);
  dst += sizeof(uint32_t);

  // 2. The varint-prefixed RequestHeader PB
  dst = CodedOutputStream::WriteVarint32ToArray(header_pb_len, dst);
  dst = header.SerializeWithCachedSizesToArray(dst);

  // We should have used the whole buffer we allocated.
  CHECK_EQ(dst, header_buf_.get() + header_len);

  // Return the concatenated packet.
  slices->push_back(Slice(header_buf_.get(), header_len));
  slices->push_back(serialized_request());
  return Status::OK();
}


Status OutboundCall::SetRequestParam(const Message &message) {
  if (PREDICT_FALSE(!message.IsInitialized())) {
    return Status::InvalidArgument("RPC argument missing required fields",
                                   message.InitializationErrorString());
  }
  int size = message.ByteSize();
  int size_with_delim = size + CodedOutputStream::VarintSize32(size);


  gscoped_ptr<uint8_t[]> buf(new uint8_t[size_with_delim]);
  uint8_t *dst = buf.get();
  dst = CodedOutputStream::WriteVarint32ToArray(size, dst);
  dst = message.SerializeWithCachedSizesToArray(dst);
  DCHECK_EQ(dst, &buf[size_with_delim]);

  request_buf_.swap(buf);
  request_size_ = size_with_delim;

  return Status::OK();
}

void OutboundCall::SetFailed(const Status &status) {
  controller_->SetFailed(status);
  CallCallback();
}

void OutboundCall::TimedOut() {
  controller_->SetTimedOut();
  CallCallback();
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
    controller_->set_state(RpcController::FINISHED_SUCCESS);
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

void OutboundCall::CallQueued() {
  controller_->set_state(RpcController::ON_OUTBOUND_QUEUE);
}

void OutboundCall::CallSent() {
  controller_->set_state(RpcController::SENT);
  // This method is called in the reactor thread, so free the header buf,
  // which was also allocated from this thread. tcmalloc's thread caching
  // behavior is a lot more efficient if memory is freed from the same thread
  // which allocated it -- this lets it keep to thread-local operations instead
  // of taking a mutex to put memory back on the global freelist.
  header_buf_.reset();

  // request_buf_ is also done being used here, but since it was allocated by
  // the caller thread, we would rather let that thread free it whenever it
  // deletes the RpcController.
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
  RETURN_NOT_OK(ParseMessage(*transfer, &header_, &serialized_response_));
  transfer_.swap(transfer);
  parsed_ = true;
  return Status::OK();
}

} // namespace rpc
} // namespace kudu
