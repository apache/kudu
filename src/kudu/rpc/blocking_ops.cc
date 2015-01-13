// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
// All rights reserved.

#include "kudu/rpc/blocking_ops.h"

#include <stdint.h>

#include <glog/logging.h>
#include <google/protobuf/message_lite.h>

#include "kudu/gutil/endian.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/constants.h"
#include "kudu/rpc/serialization.h"
#include "kudu/rpc/transfer.h"
#include "kudu/util/faststring.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/status.h"

namespace kudu {
namespace rpc {

using google::protobuf::MessageLite;

Status EnsureBlockingMode(const Socket* const sock) {
  bool is_nonblocking;
  RETURN_NOT_OK(sock->IsNonBlocking(&is_nonblocking));
  if (is_nonblocking) {
    return Status::IllegalState("Underlying socket is not set to blocking mode!");
  }
  return Status::OK();
}

Status SendFramedMessageBlocking(Socket* sock, const MessageLite& header, const MessageLite& msg,
    const MonoTime& deadline) {
  DCHECK(sock != NULL);
  DCHECK(header.IsInitialized()) << "header protobuf must be initialized";
  DCHECK(msg.IsInitialized()) << "msg protobuf must be initialized";

  RETURN_NOT_OK(EnsureBlockingMode(sock));

  // Ensure we are in blocking mode.
  // These blocking calls are typically not in the fast path, so doing this for all build types.
  bool is_non_blocking = false;
  RETURN_NOT_OK(sock->IsNonBlocking(&is_non_blocking));
  DCHECK(!is_non_blocking) << "Socket must be in blocking mode to use SendFramedMessage";

  // Serialize message
  faststring param_buf;
  RETURN_NOT_OK(serialization::SerializeMessage(msg, &param_buf));

  // Serialize header and initial length
  faststring header_buf;
  RETURN_NOT_OK(serialization::SerializeHeader(header, param_buf.size(), &header_buf));

  // Write header & param to stream
  size_t nsent;
  RETURN_NOT_OK(sock->BlockingWrite(header_buf.data(), header_buf.size(), &nsent, deadline));
  RETURN_NOT_OK(sock->BlockingWrite(param_buf.data(), param_buf.size(), &nsent, deadline));

  return Status::OK();
}

Status ReceiveFramedMessageBlocking(Socket* sock, faststring* recv_buf,
    MessageLite* header, Slice* param_buf, const MonoTime& deadline) {
  DCHECK(sock != NULL);
  DCHECK(recv_buf != NULL);
  DCHECK(header != NULL);
  DCHECK(param_buf != NULL);

  RETURN_NOT_OK(EnsureBlockingMode(sock));

  // Read the message prefix, which specifies the length of the payload.
  recv_buf->clear();
  recv_buf->resize(kMsgLengthPrefixLength);
  size_t recvd = 0;
  RETURN_NOT_OK(sock->BlockingRecv(recv_buf->data(), kMsgLengthPrefixLength, &recvd, deadline));
  uint32_t payload_len = NetworkByteOrder::Load32(recv_buf->data());

  // Verify that the payload size isn't out of bounds.
  // This can happen because of network corruption, or a naughty client.
  if (PREDICT_FALSE(payload_len > FLAGS_rpc_max_message_size)) {
    return Status::IOError(
        strings::Substitute(
            "Received invalid message of size $0 which exceeds"
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
