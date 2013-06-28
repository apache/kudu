// Copyright (c) 2013, Cloudera, inc.

#include "rpc/socket.h"

#include <errno.h>
#include <glog/logging.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "rpc/sockaddr.h"
#include "util/errno.h"

namespace kudu {
namespace rpc {

Socket::Socket()
  : fd_(-1)
{
}

Socket::Socket(int fd)
  : fd_(fd)
{
}

void Socket::Reset(int fd) {
  Close();
  fd_ = fd;
}

int Socket::Release() {
  int fd = fd_;
  fd_ = -1;
  return fd;
}

Socket::~Socket()
{
  Close();
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

Status Socket::BindAndListen(const Sockaddr &sockaddr,
                             int listenQueueSize) {
  int err;
  struct sockaddr_in addr = sockaddr.addr();

  DCHECK_GE(fd_, 0);
  if (bind(fd_, (struct sockaddr*) &addr, sizeof(addr))) {
    err = errno;
    return Status::NetworkError(
        StringPrintf("error binding socket to %s: %s",
            sockaddr.ToString().c_str(), ErrnoToString(err).c_str()),
        Slice(), err);
  }
  if (listen(fd_, listenQueueSize)) {
    err = errno;
    return Status::NetworkError(
        StringPrintf("error listening on %s: %s",
            sockaddr.ToString().c_str(), ErrnoToString(err).c_str()),
        Slice(), err);
  }
  return Status::OK();
}

Status Socket::GetSocketAddress(Sockaddr *cur_addr) {
  struct sockaddr_in sin;
  socklen_t len = sizeof(sin);
  DCHECK_GE(fd_, 0);
  if (getsockname(fd_, (struct sockaddr *)&sin, &len) == -1) {
    int err = errno;
    return Status::NetworkError(string("getsockname error: ") +
                                ErrnoToString(err), Slice(), err);
  }
  *cur_addr = sin;
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

Status Socket::Connect(const Sockaddr &remote) {
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

Status Socket::GetSockError() {
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

Status Socket::Write(uint8_t *buf, int32_t amt, int32_t *nwritten) {
  if (amt <= 0) {
    return Status::NetworkError(
              StringPrintf("invalid send of %"PRId32" bytes",
                           amt), Slice(), EINVAL);
  }
  DCHECK_GE(fd_, 0);
  int res = ::write(fd_, buf, amt);
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
  int res = sendmsg(fd_, &msg, MSG_NOSIGNAL);
  if (PREDICT_FALSE(res < 0)) {
    int err = errno;
    return Status::NetworkError(std::string("sendmsg error: ") +
                                ErrnoToString(err), Slice(), err);
  }

  *nwritten = res;
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
      return Status::NetworkError("shut down by remote end", Slice(),
                                  ESHUTDOWN);
    }
    int err = errno;
    return Status::NetworkError(std::string("recv error: ") +
                                ErrnoToString(err), Slice(), err);
  }
  *nread = res;
  return Status::OK();
}

}
}
