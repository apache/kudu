// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <tr1/memory>
#include <vector>

#include "rpc/call-inl.h"
#include "rpc/connection.h"
#include "rpc/server_call.h"

using google::protobuf::Message;
using google::protobuf::MessageLite;
using google::protobuf::io::CodedOutputStream;
using std::tr1::shared_ptr;
using std::vector;

namespace kudu { namespace rpc {

InboundCall::InboundCall(const shared_ptr<Connection> &conn)
  : conn_(conn),
    response_buf_(0)
{}

Status InboundCall::ParseFrom(gscoped_ptr<InboundTransfer> transfer) {
  RETURN_NOT_OK(ParseMessage(*transfer, &header_, &serialized_request_));
  // retain the buffer that we have a view into
  transfer_.swap(transfer);
  return Status::OK();
}

void InboundCall::RespondSuccess(const MessageLite &response) {
  SerializeResponseBuffer(response, true);

  conn_->QueueResponseForCall(gscoped_ptr<InboundCall>(this).Pass());
}

void InboundCall::RespondFailure(const Status &status) {
  ErrorStatusPB err;
  err.set_message(status.ToString());
  SerializeResponseBuffer(err, false);

  conn_->QueueResponseForCall(gscoped_ptr<InboundCall>(this).Pass());
}

void InboundCall::SerializeResponseBuffer(const MessageLite &response,
                                          bool is_success) {
  CHECK(response.IsInitialized()) << "Non-initialized response: " <<
    response.InitializationErrorString();

  ResponseHeader resp_hdr;
  resp_hdr.set_callid(header_.callid());
  resp_hdr.set_is_error(!is_success);

  int resp_hdr_len = resp_hdr.ByteSize();
  int resp_hdr_len_with_delim = resp_hdr_len + CodedOutputStream::VarintSize32(resp_hdr_len);

  int resp_len = response.ByteSize();
  int resp_len_with_delim = resp_len + CodedOutputStream::VarintSize32(resp_len);

  response_buf_.resize(InboundTransfer::kLengthPrefixLength
                       + resp_hdr_len_with_delim + resp_len_with_delim);

  uint8_t *dst = response_buf_.data();

  // 1. The length prefix for the whole request (not including its own length)
  NetworkByteOrder::Store32(dst, resp_hdr_len_with_delim + resp_len_with_delim);
  dst += InboundTransfer::kLengthPrefixLength;

  // 2. The header (vint-delimited)
  dst = CodedOutputStream::WriteVarint32ToArray(resp_hdr_len, dst);
  dst = resp_hdr.SerializeWithCachedSizesToArray(dst);

  // 3. The response itself (vint-delimited)
  dst = CodedOutputStream::WriteVarint32ToArray(resp_len, dst);
  dst = response.SerializeWithCachedSizesToArray(dst);

  CHECK_EQ(dst, response_buf_.data() + response_buf_.size());
}

Status InboundCall::SerializeResponseTo(vector<Slice> *slices) const {
  CHECK_GT(response_buf_.size(), 0);
  slices->push_back(Slice(response_buf_));
  return Status::OK();
}

string InboundCall::ToString() const {
  return StringPrintf("Call %s from %s (#%d)", method_name().c_str(),
                      conn_->remote().ToString().c_str(),
                      header_.callid());
}

} // namespace rpc
} // namespace kudu
