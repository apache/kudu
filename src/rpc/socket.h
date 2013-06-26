// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_SOCKET_H
#define KUDU_SOCKET_H

#include <sys/uio.h>

#include "gutil/macros.h"
#include "util/status.h"

namespace kudu {
namespace rpc {

class Sockaddr;

class Socket {
 public:
  static const int FLAG_NONBLOCKING = 0x1;

  // Create a new invalid Socket object.
  Socket();

  // Start managing a socket.
  explicit Socket(int fd);

  // Close the socket.  Errors will be ignored.
  ~Socket();

  // Close the Socket, checking for errors.
  Status Close();

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

  Status Init(int flags); // See FLAG_NONBLOCKING

  // Set or clear TCP_NODELAY
  Status SetNoDelay(bool enabled);

  // Calls bind(2) followed by listen(2).
  Status BindAndListen(const Sockaddr &sockaddr, int listen_queue_size);

  // Call getsockname to get the address of this socket.
  Status GetSocketAddress(Sockaddr *cur_addr);

  // Call accept(2) to get a new connection.
  Status Accept(Socket *new_conn, Sockaddr *remote, int flags);

  // start connecting this socket to a remote address.
  Status Connect(const Sockaddr &remote);

  // get the error status using getsockopt(2)
  Status GetSockError();

  Status Write(uint8_t *buf, int32_t amt, int32_t *nwritten);

  Status Writev(const struct ::iovec *iov, int iov_len, int32_t *nwritten);

  Status Recv(uint8_t *buf, int32_t amt, int32_t *nread);

 private:
  int fd_;

  DISALLOW_COPY_AND_ASSIGN(Socket);
};

} // namespace rpc
} // namespace kudu

#endif
