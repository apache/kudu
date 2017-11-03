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

#include "kudu/rpc/blocking_ops.h"

#include <cstdint>
#include <cstring>
#include <ostream>

#include <glog/logging.h>
#include <google/protobuf/message_lite.h>

#include "kudu/gutil/endian.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/constants.h"
#include "kudu/rpc/serialization.h"
#include "kudu/rpc/transfer.h"
#include "kudu/util/faststring.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {
namespace rpc {

using google::protobuf::MessageLite;

const char kHTTPHeader[] = "HTTP";

Status CheckInBlockingMode(const Socket* sock) {
  bool is_nonblocking;
  RETURN_NOT_OK(sock->IsNonBlocking(&is_nonblocking));
  if (is_nonblocking) {
    static const char* const kErrMsg = "socket is not in blocking mode";
    LOG(DFATAL) << kErrMsg;
    return Status::IllegalState(kErrMsg);
  }
  return Status::OK();
}

Status SendFramedMessageBlocking(Socket* sock, const MessageLite& header, const MessageLite& msg,
    const MonoTime& deadline) {
  DCHECK(sock != nullptr);
  DCHECK(header.IsInitialized()) << "header protobuf must be initialized";
  DCHECK(msg.IsInitialized()) << "msg protobuf must be initialized";

  // Ensure we are in blocking mode.
  // These blocking calls are typically not in the fast path, so doing this for all build types.
  RETURN_NOT_OK(CheckInBlockingMode(sock));

  // Serialize message
  faststring param_buf;
  serialization::SerializeMessage(msg, &param_buf);

  // Serialize header and initial length
  faststring header_buf;
  serialization::SerializeHeader(header, param_buf.size(), &header_buf);

  // Write header & param to stream
  size_t nsent;
  RETURN_NOT_OK(sock->BlockingWrite(header_buf.data(), header_buf.size(), &nsent, deadline));
  RETURN_NOT_OK(sock->BlockingWrite(param_buf.data(), param_buf.size(), &nsent, deadline));

  return Status::OK();
}

Status ReceiveFramedMessageBlocking(Socket* sock, faststring* recv_buf,
    MessageLite* header, Slice* param_buf, const MonoTime& deadline) {
  DCHECK(sock != nullptr);
  DCHECK(recv_buf != nullptr);
  DCHECK(header != nullptr);
  DCHECK(param_buf != nullptr);

  RETURN_NOT_OK(CheckInBlockingMode(sock));

  // Read the message prefix, which specifies the length of the payload.
  recv_buf->clear();
  recv_buf->resize(kMsgLengthPrefixLength);
  size_t recvd = 0;
  RETURN_NOT_OK(sock->BlockingRecv(recv_buf->data(), kMsgLengthPrefixLength, &recvd, deadline));
  uint32_t payload_len = NetworkByteOrder::Load32(recv_buf->data());

  // Verify that the payload size isn't out of bounds.
  // This can happen because of network corruption, or a naughty client.
  if (PREDICT_FALSE(payload_len > FLAGS_rpc_max_message_size)) {
    // A common user mistake is to try to speak the Kudu RPC protocol to an
    // HTTP endpoint, or vice versa.
    if (memcmp(recv_buf->data(), kHTTPHeader, strlen(kHTTPHeader)) == 0) {
      return Status::IOError(
          "received invalid RPC message which appears to be an HTTP response. "
          "Verify that you have specified a valid RPC port and not an HTTP port.");
    }

    return Status::IOError(
        strings::Substitute(
            "received invalid message of size $0 which exceeds"
            " the rpc_max_message_size of $1 bytes",
            payload_len, FLAGS_rpc_max_message_size));
  }

  // Read the message payload.
  recvd = 0;
  recv_buf->resize(payload_len + kMsgLengthPrefixLength);
  RETURN_NOT_OK(sock->BlockingRecv(recv_buf->data() + kMsgLengthPrefixLength,
                payload_len, &recvd, deadline));
  RETURN_NOT_OK(serialization::ParseMessage(Slice(*recv_buf), header, param_buf));
  return Status::OK();
}

} // namespace rpc
} // namespace kudu
