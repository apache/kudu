// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
//
// Utility functions shared between client and server call implementations.
#ifndef KUDU_RPC_CALL_INL_H
#define KUDU_RPC_CALL_INL_H

#include <glog/logging.h>

#include "gutil/endian.h"
#include "rpc/transfer.h"
#include "util/status.h"

namespace kudu { namespace rpc {

template<class HeaderPBType>
static Status ParseMessage(const InboundTransfer &transfer,
                           HeaderPBType *parsed_header,
                           Slice *parsed_main_message) {
  Slice buf(transfer.data());

  // First grab the total length
  if (PREDICT_FALSE(buf.size() < InboundTransfer::kLengthPrefixLength)) {
    return Status::Corruption("Invalid packet: not enough bytes for length header",
                              buf.ToDebugString());
  }

  int total_len = NetworkByteOrder::Load32(buf.data());
  DCHECK_EQ(total_len + InboundTransfer::kLengthPrefixLength, buf.size())
    << "Got mis-sized buffer: " << buf.ToDebugString();

  google::protobuf::io::CodedInputStream in(buf.data(), buf.size());
  in.Skip(InboundTransfer::kLengthPrefixLength);

  uint32_t header_len;
  if (PREDICT_FALSE(!in.ReadVarint32(&header_len))) {
    return Status::Corruption("Invalid packet: missing header delimiter",
                              buf.ToDebugString());
  }

  google::protobuf::io::CodedInputStream::Limit l;
  l = in.PushLimit(header_len);
  if (PREDICT_FALSE(!parsed_header->ParseFromCodedStream(&in))) {
    return Status::Corruption("Invalid packet: header too short",
                              buf.ToDebugString());
  }
  in.PopLimit(l);

  uint32_t main_msg_len;
  if (PREDICT_FALSE(!in.ReadVarint32(&main_msg_len))) {
    return Status::Corruption("Invalid packet: missing main msg length",
                              buf.ToDebugString());
  }

  if (PREDICT_FALSE(!in.Skip(main_msg_len))) {
    return Status::Corruption(
      StringPrintf("Invalid packet: data too short, expected %d byte main_msg", main_msg_len),
      buf.ToDebugString());
  }

  if (PREDICT_FALSE(in.BytesUntilLimit() > 0)) {
    return Status::Corruption(
      StringPrintf("Invalid packet: %d extra bytes at end of packet", in.BytesUntilLimit()),
      buf.ToDebugString());
  }

  *parsed_main_message = Slice(buf.data() + buf.size() - main_msg_len,
                               main_msg_len);
  return Status::OK();
}

} // namespace rpc
} // namespace kudu
#endif
