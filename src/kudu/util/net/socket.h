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
#pragma once

#include <cstddef>
#include <cstdint>

#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"

#include <gtest/gtest_prod.h>

struct iovec;

namespace kudu {

namespace rpc {
class RpcAcceptorBench_MeasureAcceptorDispatchTimes_Test;
}

class SocketStatsPB;
class TransportDetailsPB;

class MonoDelta;
class MonoTime;
class Sockaddr;

class Socket {
 public:
  static const int FLAG_NONBLOCKING = 0x1;

  // Create a new invalid Socket object.
  Socket();

  // Start managing a socket.
  explicit Socket(int fd);
  Socket(Socket&& other) noexcept;

  // Close the socket.  Errors will be ignored.
  virtual ~Socket();

  // Close the Socket, checking for errors.
  virtual Status Close();

  // call shutdown() on the socket
  Status Shutdown(bool shut_read, bool shut_write);

  // Start managing a socket.
  void Reset(int fd);

  // Stop managing the socket and return it.
  int Release();

  // Get the raw file descriptor, or -1 if there is no file descriptor being
  // managed.
  int GetFd() const;

  // Returns true if the error is temporary and will go away if we retry on
  // the socket.
  static bool IsTemporarySocketError(int err);

  // Init the socket for use with the given family (eg AF_INET or AF_UNIX).
  Status Init(int family, int flags); // See FLAG_NONBLOCKING

  // Set or clear TCP_NODELAY
  Status SetNoDelay(bool enabled);

  // Set or clear TCP_CORK
  Status SetTcpCork(bool enabled);

  // Set or clear O_NONBLOCK
  Status SetNonBlocking(bool enabled);
  Status IsNonBlocking(bool* is_nonblock) const;

  // Set SO_SENDTIMEO to the specified value. Should only be used for blocking sockets.
  Status SetSendTimeout(const MonoDelta& timeout);

  // Set SO_RCVTIMEO to the specified value. Should only be used for blocking sockets.
  Status SetRecvTimeout(const MonoDelta& timeout);

  // Sets SO_REUSEADDR to 'flag'. Should be used prior to Bind().
  Status SetReuseAddr(bool flag);

  // Sets SO_REUSEPORT to 'flag'. Should be used prior to Bind().
  Status SetReusePort(bool flag);

  // Convenience method to invoke the common sequence:
  // 1) SetReuseAddr(true)
  // 2) Bind()
  // 3) Listen()
  Status BindAndListen(const Sockaddr& sockaddr, int listen_queue_size);

  // Start listening for new connections, with the given backlog size.
  // Requires that the socket has already been bound using Bind().
  Status Listen(int listen_queue_size);

  // Call getsockname to get the address of this socket.
  Status GetSocketAddress(Sockaddr* cur_addr) const;

  // Call getpeername to get the address of the connected peer.
  // It is virtual so that tests can override.
  virtual Status GetPeerAddress(Sockaddr* cur_addr) const;

  // Return true if this socket is determined to be a loopback connection
  // (i.e. the local and remote peer share an IP address).
  //
  // If any error occurs while determining this, returns false.
  bool IsLoopbackConnection() const;

  // Call bind() to bind the socket to a given address.
  // If bind() fails and indicates that the requested port is already in use,
  // generates an informative log message by calling 'lsof' if available.
  Status Bind(const Sockaddr& bind_addr);

  // Call accept(2) to get a new connection.
  Status Accept(Socket* new_conn, Sockaddr* remote, int flags);

  // start connecting this socket to a remote address.
  Status Connect(const Sockaddr& remote);

  // Write up to 'amt' bytes from 'buf' to the socket. The number of bytes
  // actually written will be stored in 'nwritten'. If an error is returned,
  // the value of 'nwritten' is undefined.
  virtual Status Write(const uint8_t* buf, int32_t amt, int32_t* nwritten);

  // Vectorized Write.
  // If there is an error, that error needs to be resolved before calling again.
  // If there was no error, but not all the bytes were written, the unwritten
  // bytes must be retried. See writev(2) for more information.
  virtual Status Writev(const struct ::iovec* iov, int iov_len, int64_t* nwritten);

  // Blocking Write call, returns IOError unless full buffer is sent.
  // Underlying Socket expected to be in blocking mode. Fails if any Write() sends 0 bytes.
  // Returns OK if buflen bytes were sent, otherwise IOError.
  // Upon return, nwritten will contain the number of bytes actually written.
  // See also writen() from Stevens (2004) or Kerrisk (2010)
  Status BlockingWrite(const uint8_t* buf, size_t buflen, size_t* nwritten,
      const MonoTime& deadline);

  virtual Status Recv(uint8_t* buf, int32_t amt, int32_t* nread);

  // Blocking Recv call, returns IOError unless requested amt bytes are read.
  // Underlying Socket expected to be in blocking mode. Fails if any Recv() reads 0 bytes.
  // Returns OK if amt bytes were read, otherwise IOError.
  // Upon return, nread will contain the number of bytes actually read.
  // See also readn() from Stevens (2004) or Kerrisk (2010)
  Status BlockingRecv(uint8_t* buf, size_t amt, size_t* nread, const MonoTime& deadline);

  // Enable TCP keepalive for the underlying socket. A TCP keepalive probe will be sent
  // to the remote end after the connection has been idle for 'idle_time_s' seconds.
  // It will retry sending probes up to 'num_retries' number of times until an ACK is
  // heard from peer. 'retry_time_s' is the sleep time in seconds between successive
  // keepalive probes.
  Status SetTcpKeepAlive(int idle_time_s, int retry_time_s, int num_retries);

  Status GetStats(SocketStatsPB* pb) const;

  virtual Status GetTransportDetails(TransportDetailsPB* pb) const;

 private:
  FRIEND_TEST(rpc::RpcAcceptorBench, MeasureAcceptorDispatchTimes);

  // Called internally from SetSend/RecvTimeout().
  Status SetTimeout(int opt, const char* optname, const MonoDelta& timeout);

  // Called internally during socket setup.
  Status SetCloseOnExec();

  // Set SO_LINGER (SO_LINGER_SEC on macOS): turn on/off the "linger on close"
  // behavior for this socket according to the 'enable' parameter, setting
  // the linger timeout (in seconds) to 'linger_timeout_sec'.
  //
  // Enabling the "lingering on close" behavior with zero linger timeout allows
  // for short-circuiting the standard way of closing TCP sockets. For such a
  // socket, the TCP stack discards any data from the socket's buffer and sends
  // an RST packet to the peer immediately upon calling Close(). With that, the
  // socket skips being in the TIME_WAIT state for the necessary period of time.
  //
  // The "short-circuiting on close" might be useful in various performance
  // tests involving a lot of socket churn. To avoid unexpected complications,
  // don't use it in the code that's a part of a system running in production
  // environment.
  Status SetLinger(bool enable, int linger_timeout_sec = 0);

  // Bind the socket to a local address before making an outbound connection,
  // based on the value of FLAGS_local_ip_for_outbound_sockets.
  Status BindForOutgoingConnection();

  // Set an option on the socket.
  template<typename T>
  Status SetSockOpt(int level, int option, const T& value);

  int fd_;

  DISALLOW_COPY_AND_ASSIGN(Socket);
};

} // namespace kudu
