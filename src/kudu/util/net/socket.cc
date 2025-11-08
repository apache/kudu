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

#include "kudu/util/net/socket.h"

#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <sys/time.h>
#include <unistd.h>

#include <cerrno>
#include <cinttypes>
#include <cstring>
#include <limits>
#include <ostream>
#include <string>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/errno.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket_info.pb.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/slice.h"

DEFINE_string(local_ip_for_outbound_sockets, "",
              "IP to bind to when making outgoing socket connections. "
              "This must be an IP address of the form A.B.C.D or A:B:C:D:E:F:G:H, "
              "not a hostname. Advanced parameter, subject to change.");
TAG_FLAG(local_ip_for_outbound_sockets, experimental);

DEFINE_bool(socket_inject_short_recvs, false,
            "Inject short recv() responses which return less data than "
            "requested");
TAG_FLAG(socket_inject_short_recvs, hidden);
TAG_FLAG(socket_inject_short_recvs, unsafe);

using std::string;
using std::numeric_limits;
using strings::Substitute;

namespace kudu {

namespace {

Status ParseIpAddress(const string& addr_str, Sockaddr* result) {
  DCHECK(!addr_str.empty());
  Sockaddr bind_host;
  const auto s = bind_host.ParseString(addr_str, 0);
  if (PREDICT_FALSE(!s.ok() || bind_host.port() != 0)) {
    if (!s.ok()) {
      return Status::InvalidArgument(
          Substitute("$0: invalid local IP address", addr_str), s.ToString());
    }
    return Status::InvalidArgument(
        Substitute("$0: unexpected port with IP address", addr_str));
  }

  if (result) {
    *result = std::move(bind_host);
  }
  return Status::OK();
}

bool ValidateLocalIpForOutboundSockets(
    const char* flagname, const string& value) {
  if (value.empty()) {
    // The default value should pass the validation.
    return true;
  }

  if (auto s = ParseIpAddress(value, nullptr); !s.ok()) {
    LOG(ERROR) << Substitute("invalid local IP '$0' for --$1: $2",
                             value, flagname, s.ToString());
    return false;
  }
  return true;
}
DEFINE_validator(local_ip_for_outbound_sockets,
                 &ValidateLocalIpForOutboundSockets);

} // anonymous namespace


namespace {

// tcp_info struct duplicated from linux/tcp.h.
//
// This allows us to decouple the compile-time Linux headers from the
// runtime Linux kernel. The compile-time headers (and kernel) might be
// older than the runtime kernel, in which case an ifdef-based approach
// wouldn't allow us to get all of the info available.
//
// NOTE: this struct has been annotated with some local notes about the
// contents of each field.
struct tcp_info { // NOLINT(readability-identifier-naming)
  // Various state-tracking information.
  // ------------------------------------------------------------
  uint8_t    tcpi_state;
  uint8_t    tcpi_ca_state;
  uint8_t    tcpi_retransmits;
  uint8_t    tcpi_probes;
  uint8_t    tcpi_backoff;
  uint8_t    tcpi_options;
  uint8_t    tcpi_snd_wscale : 4, tcpi_rcv_wscale : 4;
  uint8_t    tcpi_delivery_rate_app_limited:1;

  // Configurations.
  // ------------------------------------------------------------
  uint32_t   tcpi_rto;
  uint32_t   tcpi_ato;
  uint32_t   tcpi_snd_mss;
  uint32_t   tcpi_rcv_mss;

  // Counts of packets in various states in the outbound queue.
  // At first glance one might think these are monotonic counters, but
  // in fact they are instantaneous counts of queued packets and thus
  // not very useful for our purposes.
  // ------------------------------------------------------------
  // Number of packets outstanding that haven't been acked.
  uint32_t   tcpi_unacked;

  // Number of packets outstanding that have been selective-acked.
  uint32_t   tcpi_sacked;

  // Number of packets outstanding that have been deemed lost (a SACK arrived
  // for a later packet)
  uint32_t   tcpi_lost;

  // Number of packets in the queue that have been retransmitted.
  uint32_t   tcpi_retrans;

  // The number of packets towards the highest SACKed sequence number
  // (some measure of reording, removed in later Linux versions by
  // 737ff314563ca27f044f9a3a041e9d42491ef7ce)
  uint32_t   tcpi_fackets;

  // Times when various events occurred.
  // ------------------------------------------------------------
  uint32_t   tcpi_last_data_sent;
  uint32_t   tcpi_last_ack_sent;     /* Not remembered, sorry. */
  uint32_t   tcpi_last_data_recv;
  uint32_t   tcpi_last_ack_recv;

  // Path MTU.
  uint32_t   tcpi_pmtu;

  // Receiver slow start threshold.
  uint32_t   tcpi_rcv_ssthresh;

  // Smoothed RTT estimate and variance based on the time between sending data and receiving
  // corresponding ACK. See https://tools.ietf.org/html/rfc2988 for details.
  uint32_t   tcpi_rtt;
  uint32_t   tcpi_rttvar;

  // Slow start threshold.
  uint32_t   tcpi_snd_ssthresh;
  // Sender congestion window (in number of MSS-sized packets)
  uint32_t   tcpi_snd_cwnd;
  // Advertised MSS.
  uint32_t   tcpi_advmss;
  // Amount of packet reordering allowed.
  uint32_t   tcpi_reordering;

  // Receiver-side RTT estimate per the Dynamic Right Sizing algorithm:
  //
  // "A system that is only transmitting acknowledgements can still estimate the round-trip
  // time by observing the time between when a byte is first acknowledged and the receipt of
  // data that is at least one window beyond the sequence number that was acknowledged. If the
  // sender is being throttled by the network, this estimate will be valid. However, if the
  // sending application did not have any data to send, the measured time could be much larger
  // than the actual round-trip time. Thus this measurement acts only as an upper-bound on the
  // round-trip time and should be be used only when it is the only source of round-trip time
  // information."
  uint32_t   tcpi_rcv_rtt;
  uint32_t   tcpi_rcv_space;

  // Total number of retransmitted packets.
  uint32_t   tcpi_total_retrans;

  // Pacing-related metrics.
  uint64_t   tcpi_pacing_rate;
  uint64_t   tcpi_max_pacing_rate;

  // Total bytes ACKed by remote peer.
  uint64_t   tcpi_bytes_acked;    /* RFC4898 tcpEStatsAppHCThruOctetsAcked */
  // Total bytes received (for which ACKs have been sent out).
  uint64_t   tcpi_bytes_received; /* RFC4898 tcpEStatsAppHCThruOctetsReceived */
  // Segments sent and received.
  uint32_t   tcpi_segs_out;       /* RFC4898 tcpEStatsPerfSegsOut */
  uint32_t   tcpi_segs_in;        /* RFC4898 tcpEStatsPerfSegsIn */

  // The following metrics are quite new and not in el7.
  // ------------------------------------------------------------
  uint32_t   tcpi_notsent_bytes;
  uint32_t   tcpi_min_rtt;
  uint32_t   tcpi_data_segs_in;      /* RFC4898 tcpEStatsDataSegsIn */
  uint32_t   tcpi_data_segs_out;     /* RFC4898 tcpEStatsDataSegsOut */

  // Calculated rate at which data was delivered.
  uint64_t   tcpi_delivery_rate;

  // Timers for various states.
  uint64_t   tcpi_busy_time;      /* Time (usec) busy sending data */
  uint64_t   tcpi_rwnd_limited;   /* Time (usec) limited by receive window */
  uint64_t   tcpi_sndbuf_limited; /* Time (usec) limited by send buffer */
};

} // anonymous namespace

Socket::Socket()
    : fd_(-1) {
}

Socket::Socket(int fd)
    : fd_(fd) {
}

Socket::Socket(Socket&& other) noexcept
    : fd_(other.Release()) {
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
  if (PREDICT_FALSE(fd_ < 0)) {
    return Status::OK();
  }
  const int fd = fd_;
  fd_ = -1;

  Status s;
  if (PREDICT_FALSE(::close(fd) != 0)) {
    const int err = errno;
    s = Status::NetworkError("close error", ErrnoToString(err), err);
  }
  return s;
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
    return Status::NetworkError("shutdown error", ErrnoToString(err), err);
  }
  return Status::OK();
}

int Socket::GetFd() const {
  return fd_;
}

bool Socket::IsTemporarySocketError(int err) {
  return ((err == EAGAIN) || (err == EWOULDBLOCK) || (err == EINTR));
}

#if defined(__linux__)

Status Socket::Init(int family, int flags) {
  int nonblocking_flag = (flags & FLAG_NONBLOCKING) ? SOCK_NONBLOCK : 0;
  Reset(::socket(family, SOCK_STREAM | SOCK_CLOEXEC | nonblocking_flag, 0));
  if (fd_ < 0) {
    int err = errno;
    return Status::NetworkError("error opening socket", ErrnoToString(err), err);
  }

  return Status::OK();
}

#else

Status Socket::Init(int family, int flags) {
  Reset(::socket(family, SOCK_STREAM, 0));
  if (fd_ < 0) {
    int err = errno;
    return Status::NetworkError("error opening socket", ErrnoToString(err), err);
  }
  RETURN_NOT_OK(SetNonBlocking(flags & FLAG_NONBLOCKING));
  RETURN_NOT_OK(SetCloseOnExec());

  // Disable SIGPIPE.
  int set = 1;
  RETURN_NOT_OK_PREPEND(SetSockOpt(SOL_SOCKET, SO_NOSIGPIPE, set),
                        "failed to set SO_NOSIGPIPE");
  return Status::OK();
}

#endif // defined(__linux__)

Status Socket::SetNoDelay(bool enabled) {
  int flag = enabled ? 1 : 0;
  RETURN_NOT_OK_PREPEND(SetSockOpt(IPPROTO_TCP, TCP_NODELAY, flag),
                        "failed to set TCP_NODELAY");
  return Status::OK();
}

Status Socket::SetTcpCork(bool enabled) {
#if defined(__linux__)
  int flag = enabled ? 1 : 0;
  RETURN_NOT_OK_PREPEND(SetSockOpt(IPPROTO_TCP, TCP_CORK, flag),
                        "failed to set TCP_CORK");
#endif // defined(__linux__)
  // TODO(unknown): Use TCP_NOPUSH for OSX if perf becomes an issue.
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

Status Socket::SetCloseOnExec() {
  int curflags = fcntl(fd_, F_GETFD, 0);
  if (curflags == -1) {
    int err = errno;
    Reset(-1);
    return Status::NetworkError("fcntl(F_GETFD) error", ErrnoToString(err), err);
  }
  if (fcntl(fd_, F_SETFD, curflags | FD_CLOEXEC) == -1) {
    int err = errno;
    Reset(-1);
    return Status::NetworkError("fcntl(F_SETFD) error", ErrnoToString(err), err);
  }
  return Status::OK();
}

Status Socket::SetLinger(bool enable, int linger_timeout_sec) {
#if defined(__APPLE__)
  #ifdef SO_LINGER_SEC
    struct linger arg = { enable ? 1 : 0, linger_timeout_sec };
    RETURN_NOT_OK_PREPEND(SetSockOpt(SOL_SOCKET, SO_LINGER_SEC, arg),
                          "failed to set SO_LINGER_SEC");
    return Status::OK();
  #else
    return Status::NotSupported("failed to set SO_LINGER_SEC: protocol not available");
  #endif
#else
  #ifdef SO_LINGER
    struct linger arg = { enable ? 1 : 0, linger_timeout_sec };
    RETURN_NOT_OK_PREPEND(SetSockOpt(SOL_SOCKET, SO_LINGER, arg),
                          "failed to set SO_LINGER");
    return Status::OK();
  #else
    return Status::NotSupported("failed to set SO_LINGER: protocol not available");
  #endif
#endif // #if defined(__APPLE__) ... #else ...
}

Status Socket::SetSendTimeout(const MonoDelta& timeout) {
  return SetTimeout(SO_SNDTIMEO, "SO_SNDTIMEO", timeout);
}

Status Socket::SetRecvTimeout(const MonoDelta& timeout) {
  return SetTimeout(SO_RCVTIMEO, "SO_RCVTIMEO", timeout);
}

Status Socket::SetReuseAddr(bool flag) {
  int int_flag = flag ? 1 : 0;
  RETURN_NOT_OK_PREPEND(SetSockOpt(SOL_SOCKET, SO_REUSEADDR, int_flag),
                        "failed to set SO_REUSEADDR");
  return Status::OK();
}

Status Socket::SetReusePort(bool flag) {
  int int_flag = flag ? 1 : 0;
  RETURN_NOT_OK_PREPEND(SetSockOpt(SOL_SOCKET, SO_REUSEPORT, int_flag),
                        "failed to set SO_REUSEPORT");
  return Status::OK();
}

Status Socket::SetIPv6Only(bool flag) {
  int int_flag = flag ? 1 : 0;
  RETURN_NOT_OK_PREPEND(SetSockOpt(IPPROTO_IPV6, IPV6_V6ONLY, int_flag),
                        "failed to set IPV6_V6ONLY");
  return Status::OK();
}

Status Socket::BindAndListen(const Sockaddr& sockaddr,
                             int listen_queue_size) {
  RETURN_NOT_OK(SetReuseAddr(true));
  RETURN_NOT_OK(Bind(sockaddr));
  return Listen(listen_queue_size);
}

Status Socket::Listen(int listen_queue_size) {
  if (listen(fd_, listen_queue_size)) {
    int err = errno;
    return Status::NetworkError("listen() error", ErrnoToString(err));
  }
  return Status::OK();
}

Status Socket::GetSocketAddress(Sockaddr* cur_addr) const {
  struct sockaddr_storage ss;
  socklen_t len = sizeof(ss);
  DCHECK_GE(fd_, 0);
  if (::getsockname(fd_, reinterpret_cast<struct sockaddr*>(&ss), &len) == -1) {
    int err = errno;
    return Status::NetworkError("getsockname error", ErrnoToString(err), err);
  }
  *cur_addr = Sockaddr(reinterpret_cast<struct sockaddr&>(ss), len);
  return Status::OK();
}

Status Socket::GetPeerAddress(Sockaddr* cur_addr) const {
  struct sockaddr_storage addr;
  socklen_t len = sizeof(addr);
  DCHECK_GE(fd_, 0);
  if (::getpeername(fd_, reinterpret_cast<struct sockaddr*>(&addr), &len) == -1) {
    int err = errno;
    return Status::NetworkError("getpeername error", ErrnoToString(err), err);
  }
  *cur_addr = Sockaddr(reinterpret_cast<const sockaddr&>(addr), len);
  return Status::OK();
}

bool Socket::IsLoopbackConnection() const {
  Sockaddr local, remote;
  if (!GetSocketAddress(&local).ok()) return false;
  if (!GetPeerAddress(&remote).ok()) return false;
  // Check if remote address is in 127.0.0.0/8 subnet.
  if (remote.IsAnyLocalAddress()) {
    return true;
  }
  // Compare local and remote addresses without comparing ports.
  local.set_port(0);
  remote.set_port(0);
  return local == remote;
}

Status Socket::Bind(const Sockaddr& bind_addr) {
  DCHECK_GE(fd_, 0);
  if (PREDICT_FALSE(::bind(fd_, bind_addr.addr(), bind_addr.addrlen()))) {
    int err = errno;
    Status s = Status::NetworkError(
        strings::Substitute("error binding socket to $0: $1",
                            bind_addr.ToString(), ErrnoToString(err)),
        Slice(), err);

    if (s.IsNetworkError() && bind_addr.is_ip() &&
        s.posix_code() == EADDRINUSE && bind_addr.port() != 0) {
      TryRunLsof(bind_addr);
    }
    return s;
  }

  return Status::OK();
}

Status Socket::Accept(Socket* new_conn, Sockaddr* remote, int flags) {
  TRACE_EVENT0("net", "Socket::Accept");
  struct sockaddr_storage addr;
  socklen_t olen = sizeof(addr);
  DCHECK_GE(fd_, 0);
#if defined(__linux__)
  int accept_flags = SOCK_CLOEXEC;
  if (flags & FLAG_NONBLOCKING) {
    accept_flags |= SOCK_NONBLOCK;
  }
  int fd = -1;
  RETRY_ON_EINTR(fd, accept4(fd_, (struct sockaddr*)&addr,
                             &olen, accept_flags));
  if (fd < 0) {
    int err = errno;
    return Status::NetworkError("accept4(2) error", ErrnoToString(err), err);
  }
  new_conn->Reset(fd);

#else
  int fd = -1;
  RETRY_ON_EINTR(fd, accept(fd_, (struct sockaddr*)&addr, &olen));
  if (fd < 0) {
    int err = errno;
    return Status::NetworkError("accept(2) error", ErrnoToString(err), err);
  }
  new_conn->Reset(fd);
  RETURN_NOT_OK(new_conn->SetNonBlocking(flags & FLAG_NONBLOCKING));
  RETURN_NOT_OK(new_conn->SetCloseOnExec());
#endif // defined(__linux__)

  *remote = Sockaddr(reinterpret_cast<const sockaddr&>(addr), olen);
  TRACE_EVENT_INSTANT1("net", "Accepted", TRACE_EVENT_SCOPE_THREAD,
                       "remote", remote->ToString());
  return Status::OK();
}

Status Socket::BindForOutgoingConnection() {
  Sockaddr bind_host;
  RETURN_NOT_OK(ParseIpAddress(FLAGS_local_ip_for_outbound_sockets, &bind_host));
  return Bind(bind_host);
}

Status Socket::Connect(const Sockaddr& remote) {
  TRACE_EVENT1("net", "Socket::Connect",
               "remote", remote.ToString());
  if (PREDICT_FALSE(!FLAGS_local_ip_for_outbound_sockets.empty())) {
    RETURN_NOT_OK(BindForOutgoingConnection());
  }

  DCHECK_GE(fd_, 0);
  int ret;
  RETRY_ON_EINTR(ret, ::connect(fd_, remote.addr(), remote.addrlen()));
  if (ret < 0) {
    int err = errno;
    return Status::NetworkError("connect(2) error", ErrnoToString(err), err);
  }
  return Status::OK();
}

Status Socket::Write(const uint8_t* buf, int32_t amt, int32_t* nwritten) {
  if (amt <= 0) {
    return Status::NetworkError(
              StringPrintf("invalid send of %" PRId32 " bytes",
                           amt), Slice(), EINVAL);
  }
  DCHECK_GE(fd_, 0);
  int res;
  RETRY_ON_EINTR(res, ::send(fd_, buf, amt, MSG_NOSIGNAL));
  if (res < 0) {
    int err = errno;
    return Status::NetworkError("write error", ErrnoToString(err), err);
  }
  *nwritten = res;
  return Status::OK();
}

Status Socket::Writev(const struct ::iovec* iov,
                      int iov_len,
                      int64_t* nwritten) {
  if (PREDICT_FALSE(iov_len <= 0)) {
    return Status::NetworkError(
                StringPrintf("writev: invalid io vector length of %d",
                             iov_len),
                Slice(), EINVAL);
  }
  DCHECK_GE(fd_, 0);

  struct msghdr msg;
  memset(&msg, 0, sizeof(struct msghdr));
  msg.msg_iov = const_cast<iovec*>(iov);
  msg.msg_iovlen = iov_len;
  ssize_t res;
  RETRY_ON_EINTR(res, ::sendmsg(fd_, &msg, MSG_NOSIGNAL));
  if (PREDICT_FALSE(res < 0)) {
    int err = errno;
    return Status::NetworkError("sendmsg error", ErrnoToString(err), err);
  }

  *nwritten = res;
  return Status::OK();
}

// Mostly follows writen() from Stevens (2004) or Kerrisk (2010).
Status Socket::BlockingWrite(const uint8_t* buf, size_t buflen, size_t* nwritten,
    const MonoTime& deadline) {
  DCHECK_LE(buflen, numeric_limits<int32_t>::max()) << "Writes > INT32_MAX not supported";
  DCHECK(nwritten);

  size_t tot_written = 0;
  while (tot_written < buflen) {
    int32_t inc_num_written = 0;
    int32_t num_to_write = buflen - tot_written;
    MonoDelta timeout = deadline - MonoTime::Now();
    if (PREDICT_FALSE(timeout.ToNanoseconds() <= 0)) {
      return Status::TimedOut(Substitute("sent $0 of $1 requested bytes",
                                         tot_written, buflen));
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
        return Status::TimedOut(Substitute("sent $0 of $1 requested bytes",
                                           tot_written, buflen));
      }
      return s.CloneAndPrepend("BlockingWrite error");
    }
    if (PREDICT_FALSE(inc_num_written == 0)) {
      // Shouldn't happen on Linux with a blocking socket. Maybe other Unices.
      break;
    }
  }

  if (PREDICT_FALSE(tot_written < buflen)) {
    return Status::IOError("Wrote zero bytes on a BlockingWrite() call",
        StringPrintf("Transferred %zu of %zu bytes", tot_written, buflen));
  }
  return Status::OK();
}

Status Socket::Recv(uint8_t* buf, int32_t amt, int32_t* nread) {
  if (PREDICT_FALSE(amt <= 0)) {
    return Status::NetworkError(
          StringPrintf("invalid recv of %d bytes", amt), Slice(), EINVAL);
  }

  // The recv() call can return fewer than the requested number of bytes.
  // Especially when 'amt' is small, this is very unlikely to happen in
  // the context of unit tests. So, we provide an injection hook which
  // simulates the same behavior.
  if (PREDICT_FALSE(FLAGS_socket_inject_short_recvs && amt > 1)) {
    Random r(GetRandomSeed32());
    amt = 1 + r.Uniform(amt - 1);
  }

  DCHECK_GE(fd_, 0);
  int res;
  RETRY_ON_EINTR(res, recv(fd_, buf, amt, 0));
  if (res <= 0) {
    Sockaddr remote;
    Status get_addr_status = GetPeerAddress(&remote);
    string remote_str = get_addr_status.ok() ? remote.ToString() : "unknown peer";
    if (res == 0) {
      string error_message = Substitute("recv got EOF from $0", remote_str);
      return Status::NetworkError(error_message, Slice(), ESHUTDOWN);
    }
    int err = errno;
    string error_message = Substitute("recv error from $0", remote_str);
    return Status::NetworkError(error_message, ErrnoToString(err), err);
  }
  *nread = res;
  return Status::OK();
}

// Mostly follows readn() from Stevens (2004) or Kerrisk (2010).
// One place where we deviate: we consider EOF a failure if < amt bytes are read.
Status Socket::BlockingRecv(uint8_t* buf, size_t amt, size_t* nread, const MonoTime& deadline) {
  DCHECK_LE(amt, numeric_limits<int32_t>::max()) << "Reads > INT32_MAX not supported";
  DCHECK(nread);
  size_t tot_read = 0;
  while (tot_read < amt) {
    int32_t inc_num_read = 0;
    int32_t num_to_read = amt - tot_read;
    MonoDelta timeout = deadline - MonoTime::Now();
    if (PREDICT_FALSE(timeout.ToNanoseconds() <= 0)) {
      return Status::TimedOut(Substitute("received $0 of $1 requested bytes",
                                         tot_read, amt));
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
        return Status::TimedOut(Substitute("received $0 of $1 requested bytes",
                                           tot_read, amt));
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

#if defined(__linux__)
Status Socket::GetStats(SocketStatsPB* pb) const {
  DCHECK_GE(fd_, 0);

#define HAS_FIELD(field_name) \
        (len >= offsetof(tcp_info, field_name) + sizeof(ti.field_name))

  // Fetch TCP_INFO statistics from the kernel.
  tcp_info ti = {};
  socklen_t len = sizeof(ti);
  if (int rc = getsockopt(fd_, IPPROTO_TCP, TCP_INFO, &ti, &len); rc == 0) {
    if (!HAS_FIELD(tcpi_total_retrans)) {
      // All the fields up through tcpi_total_retrans were present since very old
      // kernel versions, beyond our minimal supported. So, we can just bail if we
      // don't get sufficient data back.
      return Status::NotSupported("bad length returned for TCP_INFO");
    }

    pb->set_rtt(ti.tcpi_rtt);
    pb->set_rttvar(ti.tcpi_rttvar);
    pb->set_snd_cwnd(ti.tcpi_snd_cwnd);
    pb->set_total_retrans(ti.tcpi_total_retrans);

    // The following fields were added later in kernel development history.
    // In RHEL6 they were backported starting in 6.8. Even though they were
    // backported all together as a group, we'll just be safe and check for
    // each individually.
    if (HAS_FIELD(tcpi_pacing_rate)) {
      pb->set_pacing_rate(ti.tcpi_pacing_rate);
    }
    if (HAS_FIELD(tcpi_max_pacing_rate)) {
      pb->set_max_pacing_rate(ti.tcpi_max_pacing_rate);
    }
    if (HAS_FIELD(tcpi_bytes_acked)) {
      pb->set_bytes_acked(ti.tcpi_bytes_acked);
    }
    if (HAS_FIELD(tcpi_bytes_received)) {
      pb->set_bytes_received(ti.tcpi_bytes_received);
    }
    if (HAS_FIELD(tcpi_segs_out)) {
      pb->set_segs_out(ti.tcpi_segs_out);
    }
    if (HAS_FIELD(tcpi_segs_in)) {
      pb->set_segs_in(ti.tcpi_segs_in);
    }

    // Calculate sender bandwidth based on the same logic used by the 'ss' utility.
    if (ti.tcpi_rtt > 0 && ti.tcpi_snd_mss && ti.tcpi_snd_cwnd) {
      // Units:
      //  rtt = usec
      //  cwnd = number of MSS-size packets
      //  mss = bytes / packet
      //
      // Dimensional analysis:
      //   packets * bytes/packet * usecs/sec / usec -> bytes/sec
      static constexpr int kUsecsPerSec = 1000000;
      pb->set_send_bytes_per_sec(static_cast<int64_t>(ti.tcpi_snd_cwnd) *
                                 ti.tcpi_snd_mss * kUsecsPerSec / ti.tcpi_rtt);
    }
  }
#undef HAS_FIELD

  // Fetch the queue sizes.
  int queue_len = 0;
  if (int rc = ioctl(fd_, TIOCOUTQ, &queue_len); rc == 0) {
    pb->set_send_queue_bytes(queue_len);
  }
  if (int rc = ioctl(fd_, FIONREAD, &queue_len); rc == 0) {
    pb->set_receive_queue_bytes(queue_len);
  }
  return Status::OK();
}
#endif // #if defined(__linux__) ...

Status Socket::GetTransportDetails(TransportDetailsPB* pb) const {
  DCHECK(pb);
  DCHECK_GE(fd_, 0);
  int32_t max_seg_size = 0;
  socklen_t optlen = sizeof(max_seg_size);
  int ret = ::getsockopt(fd_, IPPROTO_TCP, TCP_MAXSEG, &max_seg_size, &optlen);
  if (ret) {
    int err = errno;
    return Status::NetworkError(
        "getsockopt(TCP_MAXSEG) failed", ErrnoToString(err), err);
  }
  pb->mutable_tcp()->set_max_segment_size(max_seg_size);

  return Status::OK();
}

Status Socket::SetTimeout(int opt, const char* optname, const MonoDelta& timeout) {
  if (PREDICT_FALSE(timeout.ToNanoseconds() < 0)) {
    return Status::InvalidArgument("Timeout specified as negative to SetTimeout",
                                   timeout.ToString());
  }
  struct timeval tv;
  timeout.ToTimeVal(&tv);
  RETURN_NOT_OK_PREPEND(SetSockOpt(SOL_SOCKET, opt, tv),
                        Substitute("failed to set socket option $0 to $1",
                                   optname, timeout.ToString()));
  return Status::OK();
}

Status Socket::SetTcpKeepAlive(int idle_time_s, int retry_time_s, int num_retries) {
  static const char* const err_string = "failed to set socket option $0 to $1";
  DCHECK_GT(idle_time_s, 0);
#if defined(__linux__)
  RETURN_NOT_OK_PREPEND(SetSockOpt(IPPROTO_TCP, TCP_KEEPIDLE, idle_time_s),
      Substitute(err_string, "TCP_KEEPIDLE", idle_time_s));
#else
  RETURN_NOT_OK_PREPEND(SetSockOpt(IPPROTO_TCP, TCP_KEEPALIVE, idle_time_s),
      Substitute(err_string, "TCP_KEEPALIVE", idle_time_s));
#endif
  DCHECK_GT(retry_time_s, 0);
  RETURN_NOT_OK_PREPEND(SetSockOpt(IPPROTO_TCP, TCP_KEEPINTVL, retry_time_s),
      Substitute(err_string, "TCP_KEEPINTVL", retry_time_s));
  DCHECK_GT(num_retries, 0);
  RETURN_NOT_OK_PREPEND(SetSockOpt(IPPROTO_TCP, TCP_KEEPCNT, num_retries),
      Substitute(err_string, "TCP_KEEPCNT", num_retries));
  RETURN_NOT_OK_PREPEND(SetSockOpt(SOL_SOCKET, SO_KEEPALIVE, 1),
      "failed to enable TCP KeepAlive socket option");
  return Status::OK();
}

template<typename T>
Status Socket::SetSockOpt(int level, int option, const T& value) {
  if (::setsockopt(fd_, level, option, &value, sizeof(T)) == -1) {
    int err = errno;
    return Status::NetworkError(ErrnoToString(err), Slice(), err);
  }
  return Status::OK();
}

} // namespace kudu
