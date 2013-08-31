// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#ifndef KUDU_RPC_BLOCKING_OPS_H
#define KUDU_RPC_BLOCKING_OPS_H

#include <set>
#include <string>

namespace google {
namespace protobuf {
class MessageLite;
} // namespace protobuf
} // namespace google

namespace kudu {

class faststring;
class Slice;
class Sockaddr;
class Socket;
class Status;

namespace rpc {

class SaslMessagePB;

// Returns OK if socket is in blocking mode. Otherwise, returns an error.
Status EnsureBlockingMode(const Socket* const sock);

// Encode and send a message over a socket.
Status SendFramedMessageBlocking(Socket* sock, const google::protobuf::MessageLite& header,
    const google::protobuf::MessageLite& msg);

// Receive a full message frame from the server.
// recv_buf: buffer to use for reading the data from the socket.
// header: Response header protobuf.
// param_buf: Slice into recv_buf containing unparsed RPC param protobuf data.
Status ReceiveFramedMessageBlocking(Socket* sock, faststring* recv_buf,
    google::protobuf::MessageLite* header, Slice* param_buf);

} // namespace rpc
} // namespace kudu

#endif  // KUDU_RPC_BLOCKING_OPS_H
