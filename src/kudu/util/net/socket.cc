// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/net/socket.h"

#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <limits>
#include <string>

#include <glog/logging.h>

#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/errno.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"

DEFINE_string(local_ip_for_outbound_sockets, "",
              "IP to bind to when making outgoing socket connections. "
              "This must be an IP address of the form A.B.C.D, not a hostname. "
              "Advanced parameter, subject to change.");
TAG_FLAG(local_ip_for_outbound_sockets, experimental);

namespace kudu {

Socket::Socket()
  : fd_(-1) {
}

Socket::Socket(int fd)
  : fd_(fd) {
}

void Socket::Reset(int fd) {
  ignore_result(Close());
  fd_ = fd;
}

int Socket::Release() {
  int fd = fd_;
  fd_ = -1;
  return fd;
}

Socket::~Socket() {
  ignore_result(Close());
}

Status Socket::Close() {
  if (fd_ < 0)
    return Status::OK();
  int err, fd = fd_;
  fd_ = -1;
  if (::close(fd) < 0) {
    err = errno;
    return Status::NetworkError(std::string("close error: ") +
                                ErrnoToString(err), Slice(), err);
  }
  fd = -1;
  return Status::OK();
}

Status Socket::Shutdown(bool shut_read, bool shut_write) {
  DCHECK_GE(fd_, 0);
  int flags = 0;
  if (shut_read && shut_write) {
    flags |= SHUT_RDWR;
  } else if (shut_read) {
    flags |= SHUT_RD;
  } else if (shut_write) {
    flags |= SHUT_WR;
  }
  if (::shutdown(fd_, flags) < 0) {
    int err = errno;
    return Status::NetworkError(std::string("shutdown error: ") +
                                ErrnoToString(err), Slice(), err);
  }
  return Status::OK();
}

int Socket::GetFd() const {
  return fd_;
}

bool Socket::IsTemporarySocketError(int err) {
  return ((err == EAGAIN) || (err == EWOULDBLOCK) || (err == EINTR));
}

#ifdef __linux

Status Socket::Init(int flags) {
  int nonblocking_flag = (flags & FLAG_NONBLOCKING) ? SOCK_NONBLOCK : 0;
  Reset(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC | nonblocking_flag, 0));
  if (fd_ < 0) {
    int err = errno;
    return Status::NetworkError(std::string("error opening socket: ") +
                                ErrnoToString(err), Slice(), err);
  }

  return Status::OK();
}

#else

#error This code has never been tested. Best of luck!

Status Socket::Init(int flags) {
  Reset(::socket(AF_INET, SOCK_STREAM, 0));
  if (fd_ < 0) {
    int err = errno;
    return Status::NetworkError(std::string("error opening socket: ") +
                                ErrnoToString(err), Slice(), err);
  }
  int curflags = fcntl(fd_, F_GETFL, 0);
  if (curflags == -1) {
    int err = errno;
    Reset(-1);
    return Status::NetworkError(std::string("fcntl(F_GETFL) error: ") +
                                ErrnoToString(err), Slice(), err);
  }
  int nonblocking_flag = (flags & FLAG_NONBLOCKING) ? O_NONBLOCK : 0;
  if (fcntl(fd_, F_SETFL, curflags | nonblocking_flag | FD_CLOEXEC) == -1) {
    int err = errno;
    Reset(-1);
    return Status::NetworkError(std::string("fcntl(F_SETFL) error: ") +
                                ErrnoToString(err), Slice(), err);
  }

  // Disable SIGPIPE.
  RETURN_NOT_OK(DisableSigPipe());

  return Status::OK();
}

#endif

Status Socket::SetNoDelay(bool enabled) {
  int flag = enabled ? 1 : 0;
  if (setsockopt(fd_, SOL_TCP, TCP_NODELAY, &flag, sizeof(flag)) == -1) {
    int err = errno;
    return Status::NetworkError(std::string("failed to set TCP_NODELAY: ") +
                                ErrnoToString(err), Slice(), err);
  }
  return Status::OK();
}

Status Socket::SetNonBlocking(bool enabled) {
  int curflags = ::fcntl(fd_, F_GETFL, 0);
  if (curflags == -1) {
    int err = errno;
    return Status::NetworkError(
        StringPrintf("Failed to get file status flags on fd %d", fd_),
        ErrnoToString(err), err);
  }
  int newflags = (enabled) ? (curflags | O_NONBLOCK) : (curflags & ~O_NONBLOCK);
  if (::fcntl(fd_, F_SETFL, newflags) == -1) {
    int err = errno;
    if (enabled) {
      return Status::NetworkError(
          StringPrintf("Failed to set O_NONBLOCK on fd %d", fd_),
          ErrnoToString(err), err);
    } else {
      return Status::NetworkError(
          StringPrintf("Failed to clear O_NONBLOCK on fd %d", fd_),
          ErrnoToString(err), err);
    }
  }
  return Status::OK();
}

Status Socket::IsNonBlocking(bool* is_nonblock) const {
  int curflags = ::fcntl(fd_, F_GETFL, 0);
  if (curflags == -1) {
    int err = errno;
    return Status::NetworkError(
        StringPrintf("Failed to get file status flags on fd %d", fd_),
        ErrnoToString(err), err);
  }
  *is_nonblock = ((curflags & O_NONBLOCK) != 0);
  return Status::OK();
}

Status Socket::SetSendTimeout(const MonoDelta& timeout) {
  return SetTimeout(SO_SNDTIMEO, "SO_SNDTIMEO", timeout);
}

Status Socket::SetRecvTimeout(const MonoDelta& timeout) {
  return SetTimeout(SO_RCVTIMEO, "SO_RCVTIMEO", timeout);
}

Status Socket::SetReuseAddr(bool flag) {
  int err;
  int int_flag = flag ? 1 : 0;
  if (setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &int_flag, sizeof(int_flag)) == -1) {
    err = errno;
    return Status::NetworkError(std::string("failed to set SO_REUSEADDR: ") +
                                ErrnoToString(err), Slice(), err);
  }
  return Status::OK();
}

Status Socket::BindAndListen(const Sockaddr &sockaddr,
                             int listenQueueSize) {
  RETURN_NOT_OK(SetReuseAddr(true));
  RETURN_NOT_OK(Bind(sockaddr));
  RETURN_NOT_OK(Listen(listenQueueSize));
  return Status::OK();
}

Status Socket::Listen(int listen_queue_size) {
  if (listen(fd_, listen_queue_size)) {
    int err = errno;
    return Status::NetworkError("listen() error", ErrnoToString(err));
  }
  return Status::OK();
}

Status Socket::GetSocketAddress(Sockaddr *cur_addr) const {
  struct sockaddr_in sin;
  socklen_t len = sizeof(sin);
  DCHECK_GE(fd_, 0);
  if (::getsockname(fd_, (struct sockaddr *)&sin, &len) == -1) {
    int err = errno;
    return Status::NetworkError(string("getsockname error: ") +
                                ErrnoToString(err), Slice(), err);
  }
  *cur_addr = sin;
  return Status::OK();
}

Status Socket::GetPeerAddress(Sockaddr *cur_addr) const {
  struct sockaddr_in sin;
  socklen_t len = sizeof(sin);
  DCHECK_GE(fd_, 0);
  if (::getpeername(fd_, (struct sockaddr *)&sin, &len) == -1) {
    int err = errno;
    return Status::NetworkError(string("getpeername error: ") +
                                ErrnoToString(err), Slice(), err);
  }
  *cur_addr = sin;
  return Status::OK();
}

Status Socket::Bind(const Sockaddr& bind_addr) {
  struct sockaddr_in addr = bind_addr.addr();

  DCHECK_GE(fd_, 0);
  if (PREDICT_FALSE(bind(fd_, (struct sockaddr*) &addr, sizeof(addr)))) {
    int err = errno;
    Status s = Status::NetworkError(
        strings::Substitute("error binding socket to $0: $1",
                            bind_addr.ToString(), ErrnoToString(err)),
        Slice(), err);

    if (s.IsNetworkError() && s.posix_code() == EADDRINUSE && bind_addr.port() != 0) {
      TryRunLsof(bind_addr);
    }
    return s;
  }

  return Status::OK();
}

Status Socket::Accept(Socket *new_conn, Sockaddr *remote, int flags) {
  struct sockaddr_in addr;
  socklen_t olen = sizeof(addr);
  int accept_flags = SOCK_CLOEXEC;
  if (flags & FLAG_NONBLOCKING) {
    accept_flags |= SOCK_NONBLOCK;
  }
  DCHECK_GE(fd_, 0);
  // TODO: add #ifdef accept4, etc.
  new_conn->Reset(::accept4(fd_, (struct sockaddr*)&addr,
                &olen, accept_flags));
  if (new_conn->GetFd() < 0) {
    int err = errno;
    return Status::NetworkError(std::string("accept4(2) error: ") +
                                ErrnoToString(err), Slice(), err);
  }

  *remote = addr;
  return Status::OK();
}

Status Socket::BindForOutgoingConnection() {
  Sockaddr bind_host;
  Status s = bind_host.ParseString(FLAGS_local_ip_for_outbound_sockets, 0);
  CHECK(s.ok() && bind_host.port() == 0)
    << "Invalid local IP set for 'local_ip_for_outbound_sockets': '"
    << FLAGS_local_ip_for_outbound_sockets << "': " << s.ToString();

  RETURN_NOT_OK(Bind(bind_host));
  return Status::OK();
}

Status Socket::Connect(const Sockaddr &remote) {
  if (PREDICT_FALSE(!FLAGS_local_ip_for_outbound_sockets.empty())) {
    RETURN_NOT_OK(BindForOutgoingConnection());
  }

  struct sockaddr_in addr;
  memcpy(&addr, &remote.addr(), sizeof(sockaddr_in));
  DCHECK_GE(fd_, 0);
  if (::connect(fd_, (const struct sockaddr*)&addr, sizeof(addr)) < 0) {
    int err = errno;
    return Status::NetworkError(std::string("connect(2) error: ") +
                                ErrnoToString(err), Slice(), err);
  }
  return Status::OK();
}

Status Socket::GetSockError() const {
  int val = 0, ret;
  socklen_t val_len = sizeof(val);
  DCHECK_GE(fd_, 0);
  ret = ::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &val, &val_len);
  if (ret) {
    int err = errno;
    return Status::NetworkError(std::string("getsockopt(SO_ERROR) failed: ") +
                                ErrnoToString(err), Slice(), err);
  }
  if (val != 0) {
    return Status::NetworkError(ErrnoToString(val), Slice(), val);
  }
  return Status::OK();
}

Status Socket::Write(const uint8_t *buf, int32_t amt, int32_t *nwritten) {
  if (amt <= 0) {
    return Status::NetworkError(
              StringPrintf("invalid send of %"PRId32" bytes",
                           amt), Slice(), EINVAL);
  }
  DCHECK_GE(fd_, 0);
  int res = ::send(fd_, buf, amt, MSG_NOSIGNAL);
  if (res < 0) {
    int err = errno;
    return Status::NetworkError(std::string("write error: ") +
                                ErrnoToString(err), Slice(), err);
  }
  *nwritten = res;
  return Status::OK();
}

Status Socket::Writev(const struct ::iovec *iov, int iov_len,
                      int32_t *nwritten) {
  if (PREDICT_FALSE(iov_len <= 0)) {
    return Status::NetworkError(
                StringPrintf("writev: invalid io vector length of %d",
                             iov_len),
                Slice(), EINVAL);
  }
  DCHECK_GE(fd_, 0);

  struct msghdr msg;
  memset(&msg, 0, sizeof(struct msghdr));
  msg.msg_iov = const_cast<iovec *>(iov);
  msg.msg_iovlen = iov_len;
  int res = ::sendmsg(fd_, &msg, MSG_NOSIGNAL);
  if (PREDICT_FALSE(res < 0)) {
    int err = errno;
    return Status::NetworkError(std::string("sendmsg error: ") +
                                ErrnoToString(err), Slice(), err);
  }

  *nwritten = res;
  return Status::OK();
}

// Mostly follows writen() from Stevens (2004) or Kerrisk (2010).
Status Socket::BlockingWrite(const uint8_t *buf, size_t buflen, size_t *nwritten,
    const MonoTime& deadline) {
  DCHECK_LE(buflen, std::numeric_limits<int32_t>::max()) << "Writes > INT32_MAX not supported";
  DCHECK(nwritten);

  size_t tot_written = 0;
  while (tot_written < buflen) {
    int32_t inc_num_written = 0;
    int32_t num_to_write = buflen - tot_written;
    MonoDelta timeout = deadline.GetDeltaSince(MonoTime::Now(MonoTime::FINE));
    if (PREDICT_FALSE(timeout.ToNanoseconds() <= 0)) {
      return Status::TimedOut("BlockingWrite timed out");
    }
    RETURN_NOT_OK(SetSendTimeout(timeout));
    Status s = Write(buf, num_to_write, &inc_num_written);
    tot_written += inc_num_written;
    buf += inc_num_written;
    *nwritten = tot_written;

    if (PREDICT_FALSE(!s.ok())) {
      // Continue silently when the syscall is interrupted.
      if (s.posix_code() == EINTR) {
        continue;
      }
      if (s.posix_code() == EAGAIN) {
        return Status::TimedOut("");
      }
      return s.CloneAndPrepend("BlockingWrite error");
    }
    if (PREDICT_FALSE(inc_num_written == 0)) {
      // Shouldn't happen on Linux with a blocking socket. Maybe other Unices.
      break;
    }
  }

  if (tot_written < buflen) {
    return Status::IOError("Wrote zero bytes on a BlockingWrite() call",
        StringPrintf("Transferred %zu of %zu bytes", tot_written, buflen));
  }
  return Status::OK();
}

Status Socket::Recv(uint8_t *buf, int32_t amt, int32_t *nread) {
  if (amt <= 0) {
    return Status::NetworkError(
          StringPrintf("invalid recv of %d bytes", amt), Slice(), EINVAL);
  }
  DCHECK_GE(fd_, 0);
  int res = ::recv(fd_, buf, amt, 0);
  if (res <= 0) {
    if (res == 0) {
      return Status::NetworkError("Recv() got EOF from remote", Slice(), ESHUTDOWN);
    }
    int err = errno;
    return Status::NetworkError(std::string("recv error: ") +
                                ErrnoToString(err), Slice(), err);
  }
  *nread = res;
  return Status::OK();
}

// Mostly follows readn() from Stevens (2004) or Kerrisk (2010).
// One place where we deviate: we consider EOF a failure if < amt bytes are read.
Status Socket::BlockingRecv(uint8_t *buf, size_t amt, size_t *nread, const MonoTime& deadline) {
  DCHECK_LE(amt, std::numeric_limits<int32_t>::max()) << "Reads > INT32_MAX not supported";
  DCHECK(nread);
  size_t tot_read = 0;
  while (tot_read < amt) {
    int32_t inc_num_read = 0;
    int32_t num_to_read = amt - tot_read;
    MonoDelta timeout = deadline.GetDeltaSince(MonoTime::Now(MonoTime::FINE));
    if (PREDICT_FALSE(timeout.ToNanoseconds() <= 0)) {
      return Status::TimedOut("");
    }
    RETURN_NOT_OK(SetRecvTimeout(timeout));
    Status s = Recv(buf, num_to_read, &inc_num_read);
    tot_read += inc_num_read;
    buf += inc_num_read;
    *nread = tot_read;

    if (PREDICT_FALSE(!s.ok())) {
      // Continue silently when the syscall is interrupted.
      if (s.posix_code() == EINTR) {
        continue;
      }
      if (s.posix_code() == EAGAIN) {
        return Status::TimedOut("");
      }
      return s.CloneAndPrepend("BlockingRecv error");
    }
    if (PREDICT_FALSE(inc_num_read == 0)) {
      // EOF.
      break;
    }
  }

  if (PREDICT_FALSE(tot_read < amt)) {
    return Status::IOError("Read zero bytes on a blocking Recv() call",
        StringPrintf("Transferred %zu of %zu bytes", tot_read, amt));
  }
  return Status::OK();
}

Status Socket::SetTimeout(int opt, std::string optname, const MonoDelta& timeout) {
  if (PREDICT_FALSE(timeout.ToNanoseconds() < 0)) {
    return Status::InvalidArgument("Timeout specified as negative to SetTimeout",
                                   timeout.ToString());
  }
  struct timeval tv;
  timeout.ToTimeVal(&tv);
  socklen_t optlen = sizeof(tv);
  if (::setsockopt(fd_, SOL_SOCKET, opt, &tv, optlen) == -1) {
    int err = errno;
    return Status::NetworkError(
        StringPrintf("Failed to set %s to %s", optname.c_str(), timeout.ToString().c_str()),
        ErrnoToString(err), err);
  }
  return Status::OK();
}

} // namespace kudu
