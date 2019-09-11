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

#include "kudu/clock/builtin_ntp.h"

#include <netinet/in.h>
#include <sched.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <deque>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/errno.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/locks.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/random_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

// The default value for the --builtin_ntp_servers flag assumes the machine
// where a process with built-in NTP client is run has access to the Internet.
// The default value for the flag is set as prescribed by
// https://www.ntppool.org/en/use.html
DEFINE_string(builtin_ntp_servers,
              "0.pool.ntp.org,"
              "1.pool.ntp.org,"
              "2.pool.ntp.org,"
              "3.pool.ntp.org",
              "The NTP servers used by the built-in NTP client, in format "
              "<FQDN|IP>[:PORT]. These will only be used if the built-in NTP "
              "client is enabled.");
TAG_FLAG(builtin_ntp_servers, experimental);

// In the 'Best practices' section, RFC 4330 states that 15 seconds is the
// minimum allowed polling interval.
//
// NOTE: as of version 4.2.8, ntpd allows setting minpoll as low as 3
// (2^3 = 8 seconds), and chronyd of version 3.5 supports minpoll as low as
// -6 (2^-6 = 1/64 second), so 16 seconds looks like a reasonble default for
// a client which tries to drift as less as possible from the source NTP servers
// but keeping the polling interval reasonable and conforming to RFC 4330
// (RFC 5905 scrapped the whole 'Best practices' section).
DEFINE_uint32(builtin_ntp_poll_interval_ms, 16000,
              "The time between successive polls of a single NTP server "
              "(in milliseconds)");
TAG_FLAG(builtin_ntp_poll_interval_ms, experimental);
TAG_FLAG(builtin_ntp_poll_interval_ms, runtime);

DEFINE_uint32(builtin_ntp_request_timeout_ms, 3000,
              "Timeout for requests sent to NTP servers (in milliseconds)");
TAG_FLAG(builtin_ntp_request_timeout_ms, experimental);
TAG_FLAG(builtin_ntp_request_timeout_ms, runtime);

DEFINE_uint32(builtin_ntp_true_time_refresh_max_interval_s, 3600,
              "Maximum allowed time interval without refreshing computed "
              "true time estimation (in seconds)");
TAG_FLAG(builtin_ntp_true_time_refresh_max_interval_s, experimental);
TAG_FLAG(builtin_ntp_true_time_refresh_max_interval_s, runtime);

DEFINE_string(builtin_ntp_client_bind_address, "0.0.0.0",
              "Local address to bind client UDP socket used to send and "
              "receive NTP packets. The default value '0.0.0.0' is equivalent "
              "to '0.0.0.0:0' meaning 'bind to all available IPv4 interfaces "
              "using ephemeral ports (i.e. port 0)'.");
TAG_FLAG(builtin_ntp_client_bind_address, experimental);

using std::deque;
using std::lock_guard;
using std::pair;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace clock {

constexpr int kStandardNtpPort = 123;

// Number of seconds between Jan 1 1900 and the unix epoch start.
constexpr uint64_t kNtpTimestampDelta = 2208988800ull;

// Keep the last 8 polls from each server.
constexpr int kResponsesToRememberPerServer = 8;

constexpr int kMinNtpVersion = 1;
constexpr int kNtpVersion = 3;  // This is NTPv3 implementation (client).
constexpr uint8_t kInvalidStratum = 0;
constexpr uint8_t kMaxStratum = 16;
// Maximum allowed dispersion (in microseconds) per RFC 5905.
constexpr uint64_t kMaxDispersionUs = 16000000; // 16 seconds

DEFINE_validator(builtin_ntp_servers,
                 [](const char* name, const string& val) {
  vector<HostPort> hps;
  Status s = HostPort::ParseStrings(val, kStandardNtpPort, &hps);
  if (!s.ok()) {
    LOG(ERROR) << Substitute("could not parse $0 flag: $1",
                             name, s.message().ToString());
    return false;
  }
  return true;
});

DEFINE_validator(builtin_ntp_client_bind_address,
                 [](const char* name, const string& val) {
  HostPort hp;
  Status s = hp.ParseString(val, 0);
  if (!s.ok()) {
    LOG(ERROR) << Substitute("could not parse $0 flag: $1",
                             name, s.message().ToString());
    return false;
  }
  return true;
});

// A glossary of NTP-related terms is available at:
//   http://support.ntp.org/bin/view/Support/NTPRelatedDefinitions
//
// Revelant RFC references:
//   https://tools.ietf.org/html/rfc5905
//   https://tools.ietf.org/html/rfc4330 (obsoleted by RFC 5905)
struct BuiltInNtp::NtpPacket {
  // Bitfield: AABBBCCC
  // AA : leap indicator
  // BBB: version number of the protocol
  // CCC: mode.
  uint8_t lvm;

  // Stratum level of the local clock.
  uint8_t stratum;

  // Maximum interval between successive messages.
  uint8_t poll;

  // Precision of the local clock.
  int8_t precision;

  // Total round trip delay time.
  uint32_t root_delay;
  // Max error aloud from primary clock source.
  uint32_t root_dispersion;
  // Reference clock identifier.
  uint32_t ref_id;

  // Reference time-stamp: seconds and fraction of a second.
  uint32_t ref_time_s;
  uint32_t ref_time_f;

  // Originate time-stamp: seconds and fraction of a second.
  uint32_t orig_time_s;
  uint32_t orig_time_f;

  // Received time-stamp: seconds and fraction of a second.
  uint32_t recv_time_s;
  uint32_t recv_time_f;

  // Transmit time-stamp: seconds and fraction of a second.
  uint32_t transmit_time_s;
  uint32_t transmit_time_f;

  // TODO(KUDU-2941): consider to add message digest and key identifier

  enum LeapIndicator {
    kNoWarning = 0,
    kLastMinuteHas61Secs = 1,
    kLastMinuteHas59Secs = 2,
    kAlarm = 3
  };

  enum ProtocolMode {
    kReserved = 0,
    kSymmetricActive = 1,
    kSymmetricPassive = 2,
    kClient = 3,
    kServer = 4,
    kBroadcast = 5,
    kReservedControlMessage = 6,
    kReservedPrivateUse = 7
  };

  static uint64_t convert_timestamp(uint32_t secs, uint32_t frac) {
    uint64_t micros = (ntohl(secs) - kNtpTimestampDelta) * MonoTime::kMicrosecondsPerSecond;
    micros += (ntohl(frac) * MonoTime::kMicrosecondsPerSecond) >> 32;
    return micros;
  }

  static uint64_t convert_delay(uint32_t val) {
    // convert 32-bit unsigned 16.16 fixed-point to seconds.
    val = ntohl(val);
    uint32_t secs = val >> 16;
    uint32_t frac = val & 0xffff;
    return secs * MonoTime::kMicrosecondsPerSecond +
        ((frac * MonoTime::kMicrosecondsPerSecond) >> 16);
  }

  static uint8_t generate_lvm(uint8_t leap, uint8_t version, uint8_t mode) {
    return  (((leap << 6) & 0xc0) | ((version << 3) & 0x38) | (mode & 0x07));
  }

  LeapIndicator leap_indicator() const {
    return static_cast<LeapIndicator>(lvm >> 6);
  }

  int version_number() const {
    return (lvm >> 3) & 0x07;
  }

  ProtocolMode protocol_mode() const {
    return static_cast<ProtocolMode>(lvm & 0x07);
  }

  uint64_t ref_timestamp_us() const {
    return convert_timestamp(ref_time_s, ref_time_f);
  }

  uint64_t server_receive_timestamp_us() const {
    return convert_timestamp(recv_time_s, recv_time_f);
  }

  uint64_t server_transmit_timestamp_us() const {
    return convert_timestamp(transmit_time_s, transmit_time_f);
  }

  uint64_t root_delay_us() const {
    return convert_delay(root_delay);
  }

  uint64_t root_dispersion_us() const {
    return convert_delay(root_dispersion);
  }

  string ToString() const {
    string ret;
    StrAppend(&ret, "leap=", leap_indicator(), "\n");
    StrAppend(&ret, "version=", version_number(), "\n");
    StrAppend(&ret, "mode=", protocol_mode(), "\n");
    StrAppend(&ret, "stratum=", static_cast<int>(stratum), "\n");
    StrAppend(&ret, "poll=", static_cast<int>(poll), "\n");
    StrAppend(&ret, "precision=", static_cast<int>(precision), "\n");
    StrAppend(&ret, "root_delay=", root_delay_us(), "\n");
    StrAppend(&ret, "root_disp=", root_dispersion_us(), "\n");
    StrAppend(&ret, "ref_id=", ntohl(ref_id), "\n");

    StrAppend(&ret, "ref_time_s=", ntohl(ref_time_s), "\n");
    StrAppend(&ret, "ref_time_f=", ntohl(ref_time_f), "\n");

    StrAppend(&ret, "orig_time_s=", ntohl(orig_time_s), "\n");
    StrAppend(&ret, "orig_time_f=", ntohl(orig_time_f), "\n");

    StrAppend(&ret, "recv_time=", server_receive_timestamp_us(), "\n");
    StrAppend(&ret, "transmit_time=", server_transmit_timestamp_us(), "\n");
    return ret;
  }
};

// Keeps track of a previously-sent request to an NTP server which is currently
// awaiting a response.
struct BuiltInNtp::PendingRequest {
  // The request that we sent.
  NtpPacket request;
  // The server to which the request was sent.
  ServerState* server;

  // The specific resolved address to which the request was sent.
  // A hostname may resolve to several addresses (eg in the case of NTP pools)
  // and we have to pick one.
  Sockaddr addr;

  // The monotonic timestamp when we sent the request.
  int64_t send_time_mono_us;

  // Check that the computed size at compilation time matches the expected size
  // based on the RFC. See https://tools.ietf.org/html/rfc5905, page 18 for the
  // structure of NTP packet (NTPv4). This implementation targets NTPv3 and
  // the optional key identifier and message digest extended fields are not
  // handled; the same for the optional authenticator fields.
  static_assert(sizeof(NtpPacket) == 48, "unexpected size of NtpPacket");
};

// A time measurement recorded after receiving a response from an NTP server.
struct BuiltInNtp::RecordedResponse {
  // The server which provided the response.
  Sockaddr addr;
  // The server's transmit timestamp.
  uint64_t tx_timestamp;
  // The time at which the response was recorded.
  int64_t monotime;
  // The calculated estimated offset between our monotime and the server's wall-clock time.
  int64_t offset_us;
  // The estimated maximum error between our time and the server's time.
  int64_t error_us;
};

class BuiltInNtp::ServerState {
 public:
  explicit ServerState(HostPort host) :
      host_(std::move(host)),
      addr_idx_(0),
      next_poll_(MonoTime::Now()),
      i_pkt_total_num_(0),
      i_pkt_valid_num_(0),
      o_pkt_timedout_num_(0),
      o_pkt_total_num_(0) {
  }

  Status Init() {
    return ReresolveAddrs();
  }

  MonoTime next_poll() const {
    shared_lock<rw_spinlock> l(lock_);
    return next_poll_;
  }

  void UpdateNextPoll(MonoTime next) {
    lock_guard<rw_spinlock> l(lock_);

    // Increment the counter of NTP packets sent.
    ++o_pkt_total_num_;

    // Update the time for next poll.
    next_poll_ = next;
  }

  const Sockaddr& cur_addr() const {
    shared_lock<rw_spinlock> l(lock_);
    return addrs_[addr_idx_ % addrs_.size()];
  }

  void TimeoutAndSwitchNextServer() {
    lock_guard<rw_spinlock> l(lock_);
    ++o_pkt_timedout_num_;
    ++addr_idx_;
  }

  void InvalidatePacket() {
    lock_guard<rw_spinlock> l(lock_);
    ++i_pkt_total_num_;
  }

  void RecordPacket(const RecordedResponse& rr) {
    VLOG(1) << Substitute("NTP from $0 ($1):  $2 +/- $3us", host_.ToString(),
                          rr.addr.ToString(), rr.offset_us, rr.error_us);
    lock_guard<rw_spinlock> l(lock_);
    ++i_pkt_total_num_;
    ++i_pkt_valid_num_;
    responses_.emplace_back(rr);
    if (responses_.size() > kResponsesToRememberPerServer) {
      responses_.pop_front();
    }
  }

  bool IsReplayedPacket(const Sockaddr& from, const NtpPacket& packet) const {
    lock_guard<rw_spinlock> l(lock_);
    if (responses_.empty()) {
      return false;
    }
    const auto& last_response = responses_.back();
    return from == last_response.addr &&
        packet.server_transmit_timestamp_us() == last_response.tx_timestamp;
  }

  Status GetBestResponse(RecordedResponse* response) const {
    shared_lock<rw_spinlock> l(lock_);
    // For now, just return the freshest response.
    // TODO(KUDU-2939): when the dispersion of the samples is being updated
    //                  over time, select the best sample among all available
    //                  w.r.t. jitter and delay metrics.
    if (i_pkt_total_num_ == 0 && o_pkt_timedout_num_ == 0) {
      // Haven't gotten a chance to communicated with the server at least once
      // from the very start. The strategy here is to early detect as much of
      // misconfiguration as possible.
      return Status::Incomplete(Substitute(
          "has not communicated with server $0 yet (current address $1)",
          host_.ToString(), cur_addr().ToString()));
    }
    if (responses_.empty()) {
      return Status::NotFound("not a single valid response from server yet");
    }
    *response = responses_.back();
    return Status::OK();
  }

  Status ReresolveAddrs() {
    // RFC4330 states in its '10. Best practices' chapter on page 21:
    //
    // 7.  A client SHOULD re-resolve the server IP address at periodic
    //     intervals, but not at intervals less than the time-to-live field
    //     in the DNS response.
    //
    // However, this recommendation along with the whole 'Best practices'
    // chapter is gone from RFC 5905 which obsoletes RFC 4330. Also, as can be
    // seen from chrony source code, chronyd NTP server peforms re-resolution
    // of servers' addresses only when replacing 'bad' servers or by command
    // request (e.g originated from chronyc CLI utility).
    vector<Sockaddr> addrs;
    auto s = host_.ResolveAddresses(&addrs);
    {
      lock_guard<rw_spinlock> l(lock_);
      addrs_ = std::move(addrs);
    }
    return s;
  }

  void DumpDiagnostics(string* diag) const {
    DCHECK(diag);
    shared_lock<rw_spinlock> l(lock_);
    StrAppend(diag, "server ", host_.ToString(), ": ");
    auto addrs_list = JoinMapped(
        addrs_, [](const Sockaddr& addr) { return addr.ToString(); }, ",");
    StrAppend(diag, "addresses=", addrs_list, " ");
    StrAppend(diag, "current_address=", cur_addr().ToString(), " ");
    StrAppend(diag, "i_pkt_total_num=", i_pkt_total_num_, " ");
    StrAppend(diag, "i_pkt_valid_num=", i_pkt_valid_num_, " ");
    StrAppend(diag, "o_pkt_total_num=", o_pkt_total_num_, " ");
    StrAppend(diag, "o_pkt_timedout_num=", o_pkt_timedout_num_, "\n");
  }

 private:

  // Lock to protect internal state below from concurrent access.
  mutable rw_spinlock lock_;

  // The user-specified hostname.
  const HostPort host_;

  // The resolved addresses for this hostname.
  vector<Sockaddr> addrs_;

  // The current address index within addrs_ that we are trying to poll. If an
  // address is inaccessible, we cycle to the next address in the list.
  // TODO(aserbin): if run out of addresses, re-resolve
  //                (e.g., using DnsResolver::ResolveAddressesAsync())
  int addr_idx_;

  // Queue of the last responses from this server.
  // The latest response will be added to the end.
  deque<RecordedResponse> responses_;

  // Scheduled time for the next request to sent.
  MonoTime next_poll_;

  // Diagnostic counters.
  size_t i_pkt_total_num_;    // total number of NTP responses received
  size_t i_pkt_valid_num_;    // number of valid NTP responses received
  size_t o_pkt_timedout_num_; // number of timed out NTP requests
  size_t o_pkt_total_num_;    // number of NTP requests sent
};

const BuiltInNtp::Interval BuiltInNtp::kIntervalNone = { -1, -1 };

BuiltInNtp::BuiltInNtp()
    : rng_(GetRandomSeed32()) {
}

BuiltInNtp::BuiltInNtp(vector<HostPort> servers)
    : rng_(GetRandomSeed32()) {
  CHECK_OK(PopulateServers(std::move(servers)));
}

BuiltInNtp::~BuiltInNtp() {
  Shutdown();
}

Status BuiltInNtp::Init() {
  MutexLock l(state_lock_);
  CHECK_EQ(kUninitialized, state_);

  RETURN_NOT_OK(InitImpl());
  state_ = kStarting;
  return Status::OK();
}

Status BuiltInNtp::WalltimeWithError(uint64_t* now_usec, uint64_t* error_usec) {
  WalltimeSnapshot last;
  {
    shared_lock<rw_spinlock> l(last_computed_lock_);
    last = last_computed_;
  }

  if (!last.is_synchronized) {
    return Status::ServiceUnavailable("wallclock is not synchronized",
        (last.mono == 0) ? "no valid NTP responses yet"
                         : "synchronization lost");
  }

  const auto mono = GetMonoTimeMicrosRaw();
  DCHECK_GE(mono, last.mono);

  const int64_t delta_t = mono - last.mono;
  if (PREDICT_FALSE(MonoDelta::FromSeconds(
          FLAGS_builtin_ntp_true_time_refresh_max_interval_s) <
                    MonoDelta::FromMicroseconds(delta_t))) {
    return Status::ServiceUnavailable(Substitute(
        "$0: too long after last true time refresh",
        MonoDelta::FromMicroseconds(delta_t).ToString()));
  }

  // TODO(KUDU-2940): apply measured local clock skew against the true time
  //                  clock when computing projected wallclock reading and error
  *now_usec = delta_t + last.wall;
  *error_usec = last.error + delta_t * kSkewPpm / 1e6;
  return Status::OK();
}

void BuiltInNtp::DumpDiagnostics(vector<string>* log) const {
  // TODO(aserbin): maybe, JSON format would be a better choice?
  string diag;
  for (const auto& s : servers_) {
    s->DumpDiagnostics(&diag);
  }
  WalltimeSnapshot last;
  {
    shared_lock<rw_spinlock> l(last_computed_lock_);
    last = last_computed_;
  }
  StrAppend(&diag, "is_synchronized=",
            last.is_synchronized ? "true" : "false", "\n");
  StrAppend(&diag, "last_mono=", last.mono, "\n");
  StrAppend(&diag, "last_wall=", last.wall , "\n");
  StrAppend(&diag, "last_error=", last.error, "\n");
  StrAppend(&diag, "now_mono=", GetMonoTimeMicrosRaw(), "\n");
  LOG_STRING(INFO, log) << diag;
}

BuiltInNtp::Interval BuiltInNtp::FindIntersection(
    const vector<RecordedResponse>& responses, int64_t reftime) {
  vector<pair<int64_t, int>> interval_endpoints;
  for (const auto& r : responses) {
    int64_t wall = reftime + r.offset_us;
    int64_t error = r.error_us + (reftime - r.monotime) * kSkewPpm / 1e6;
    DCHECK_GE(reftime, r.monotime);
    DCHECK_GE(error, 0);
    interval_endpoints.emplace_back(wall - error, -1);
    interval_endpoints.emplace_back(wall + error, 1);
    VLOG(2) << Substitute("correctness interval ($0, $1) from $2",
                          wall - error, wall + error, r.addr.ToString());
  }

  if (responses.size() == 1) {
    // Short-circuiting the search since the algorithm below doesn't handle
    // single interval.
    CHECK_EQ(2, interval_endpoints.size());
    return std::make_pair(interval_endpoints[0].first,
                          interval_endpoints[1].first);
  }

  std::sort(interval_endpoints.begin(), interval_endpoints.end());

  int best = 1; // for an intersection, at least 2 intervals are needed
  int count_overlap = 0;
  Interval best_interval = kIntervalNone;
  for (int i = 1; i < interval_endpoints.size(); i++) {
    const auto& cur = interval_endpoints[i - 1];
    const auto& next = interval_endpoints[i];
    count_overlap -= cur.second;
    // TODO(aserbin): in the layouts like the following, which interval is
    //                better to choose? Right now, the first is chosen,
    //                but maybe it's better to randomize the choice to avoid
    //                bias or simply choose some wider interval which covers
    //                both intersections intervals?
    //
    // source A     :   <---->
    // source B     :           <--->
    // source C     :  <-------------->
    // intersection :   <====>  <===>
    if (count_overlap > best) {
      best = count_overlap;
      best_interval = std::make_pair(cur.first, next.first);
    }
  }
  return best_interval;
}

Status BuiltInNtp::InitImpl() {
  // TODO(KUDU-2937) implement 'iburst' mode and use it for initial time sync
  state_lock_.AssertAcquired();
  CHECK_EQ(kUninitialized, state_);
  CHECK_EQ(-1, socket_.GetFd());

  if (servers_.empty()) {
    // That's the case when this object has been created using the default
    // constructor.
    vector<HostPort> hps;
    RETURN_NOT_OK_PREPEND(HostPort::ParseStrings(FLAGS_builtin_ntp_servers,
                                                 kStandardNtpPort, &hps),
                          "could not parse --builtin_ntp_servers flag");
    RETURN_NOT_OK(PopulateServers(std::move(hps)));
  }
  for (const auto& s : servers_) {
    RETURN_NOT_OK(s->Init());
  }

  // Set up a socket for sending and receiving UDP packets.
  int socket_fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
  if (socket_fd == -1) {
    int err = errno;
    return Status::NetworkError("could not create UDP socket", ErrnoToString(err));
  }

  Sockaddr to_bind;
  RETURN_NOT_OK_PREPEND(
      to_bind.ParseString(FLAGS_builtin_ntp_client_bind_address, 0),
      "could not parse --builtin_ntp_client_bind_address");

  socket_.Reset(socket_fd);
  RETURN_NOT_OK_PREPEND(socket_.Bind(to_bind), "could not bind UDP socket");
  // The IO loop of this implementation (see BuiltInNtp::PollThread() method)
  // doesn't allow for IO multiplexing, so this short SO_RCVTIMEO timeout is set
  // to avoid blocking the IO loop at the receiving phase when there is data
  // to be sent.
  // TODO(aserbin): use IO multiplexing and non-blocking IO via libev
  RETURN_NOT_OK_PREPEND(socket_.SetRecvTimeout(MonoDelta::FromSeconds(0.5)),
                        "could not set socket recv timeout");
  RETURN_NOT_OK_PREPEND(
      Thread::Create("ntp", "ntp client", &BuiltInNtp::PollThread, this, &thread_),
      "could not start NTP client thread");
  return Status::OK();
}

Status BuiltInNtp::PopulateServers(std::vector<HostPort> servers) {
  // This method is to be called only once.
  CHECK(servers_.empty());
  if (servers.empty()) {
    return Status::InvalidArgument("empty set of source NTP servers");
  }
  for (auto& s : servers) {
    servers_.emplace_back(new ServerState(std::move(s)));
  }
  return Status::OK();
}

bool BuiltInNtp::is_shutdown() const {
  MutexLock l(state_lock_);
  return state_ == kShutdown;
}

void BuiltInNtp::Shutdown() {
  {
    MutexLock l(state_lock_);
    if (state_ == kShutdown) {
      return;
    }

    state_ = kShutdown;
  }

  if (socket_.GetFd() >= 0) {
    // Shutting down the socket without closing it ensures that any attempt
    // to call sendmsg() will get EPIPE.
    socket_.Shutdown(true, true);
  }
  if (thread_) {
    thread_->Join();
  }
  WARN_NOT_OK(socket_.Close(), "could not close UDP socket");
}

bool BuiltInNtp::TryReceivePacket() {
  struct sockaddr_in si_other;
  socklen_t slen = sizeof(si_other);
  // TODO(todd) use recvfrom and SO_TIMESTAMP to get the most accurate recv time stamp?
  // unclear what clock those come from
  int n;
  NtpPacket resp;
  RETRY_ON_EINTR(n, recvfrom(socket_.GetFd(), &resp, sizeof(resp), /*flags=*/0,
                             reinterpret_cast<sockaddr*>(&si_other), &slen));
  if (n == -1) {
    if (Socket::IsTemporarySocketError(errno)) {
      // Indicates a timeout (EINTR is already handled by retries above).
      return false;
    }
    KPLOG_EVERY_N(WARNING, 10) << "NTP recv error";
    return false;
  }

  if (n <= 0) {
    // locally shut down
    return false;
  }
  const auto recv_time_mono_us = GetMonoTimeMicrosRaw();

  Sockaddr from_server(si_other);
  if (n < sizeof(NtpPacket)) {
    VLOG(1) << Substitute("$0: invalid NTP packet size from $0",
                          n, from_server.ToString());
    return true;
  }

  VLOG(2) << Substitute("received response from $0: $1",
                        from_server.ToString(), resp.ToString());
  // 1.  When the IP source and destination addresses are available for
  //     the client request, they should match the interchanged addresses
  //     in the server reply.

  // 2.  When the UDP source and destination ports are available for the
  //     client request, they should match the interchanged ports in the
  //     server reply.

  // 3.  The Originate Timestamp in the server reply should match the
  //     Transmit Timestamp used in the client request.

  // 4.  The server reply should be discarded if any of the Stratum
  //     or Transmit Timestamp fields is 0 or the Mode field is not 4
  //     (unicast) or 5 (broadcast).

  // Verify the above three by looking up our pending request based on the
  // server IP and origination timestamp.
  unique_ptr<PendingRequest> p = RemovePending(from_server, resp);
  if (!p) {
    VLOG(1) << Substitute("received response from $0 but no request "
                          "was pending (already timed out?)",
                          from_server.ToString());
    return true;
  }

  auto cleanup = MakeScopedCleanup([&]() {
    p->server->InvalidatePacket();
  });

  if (resp.version_number() < kMinNtpVersion ||
      resp.version_number() > kNtpVersion) {
    VLOG(1) << Substitute("$0: unexpected protocol version in response packet "
                          "from NTP server at $1",
                          resp.version_number(), from_server.ToString());
    return true;
  }
  if ((resp.transmit_time_s == 0 && resp.transmit_time_f == 0) ||
      !(resp.protocol_mode() == NtpPacket::kServer ||
        resp.protocol_mode() == NtpPacket::kBroadcast)) {
    VLOG(1) << Substitute("unexpected data in response packet from $0: $1",
                          from_server.ToString(), resp.ToString());
    return true;
  }
  // According to https://www.eecis.udel.edu/~mills/ntp/html/select.html
  // servers with stratum >15 should be ignored.
  if (resp.stratum == kInvalidStratum || resp.stratum >= kMaxStratum ||
      resp.leap_indicator() == NtpPacket::kAlarm) {
    VLOG(1) << Substitute("NTP server $0 is unsynchronized",
                          from_server.ToString());
    return true;
  }

  // 5.  The root distance should be less than the defined maximum.
  uint64_t root_distance = resp.root_delay_us() / 2 + resp.root_dispersion_us();
  if (root_distance >= kMaxDispersionUs) {
    VLOG(1) << Substitute("root distance of server $0 is too large: $1",
                          from_server.ToString(), root_distance);
    return true;
  }

  // 6.  Sanity check: server's transmit timestamp should not be less than
  //     its reference timestamp.
  if (resp.server_transmit_timestamp_us() < resp.ref_timestamp_us()) {
    VLOG(1) << Substitute("$0: server transmit timestamp ($1) is behind "
                          "its reference timestamp ($2)",
                          from_server.ToString(),
                          resp.server_transmit_timestamp_us(),
                          resp.ref_timestamp_us());
    return true;
  }

  // Ignore replayed packets, i.e. those where the server transmit timestamp is
  // the same as in previously received NTP packet from the same server.
  if (p->server->IsReplayedPacket(from_server, resp)) {
    VLOG(1) << Substitute("replayed packet from $0", from_server.ToString());
    return true;
  }

  // TODO(KUDU-2938): handle "kiss-of-death" (RFC 5905 section 7.4)
  // TODO(aserbin): add more validators regarding consistency of measurements
  //                (reported precision and measured intervals, delays, etc.)

  // From RFC 4330:
  //
  //  When the server reply is received, the client determines a
  //  Destination Timestamp variable as the time of arrival according to
  //  its clock in NTP timestamp format.  The following table summarizes
  //  the four timestamps.
  //
  //    Timestamp Name          ID   When Generated
  //    ------------------------------------------------------------
  //    Originate Timestamp     T1   time request sent by client
  //    Receive Timestamp       T2   time request received by server
  //    Transmit Timestamp      T3   time reply sent by server
  //    Destination Timestamp   T4   time reply received by client

  // The roundtrip delay d and system clock offset t are defined as:
  //   d = (T4 - T1) - (T3 - T2)     t = ((T2 - T1) + (T3 - T4)) / 2.
  //
  // In other words, the roundtrip delay is the difference between the local
  // and the remote measurement time intervals; the offset is the difference
  // between averaged (two samples) remote and local wallclock times.
  int64_t local_interval_us = recv_time_mono_us - p->send_time_mono_us;
  int64_t remote_interval_us =
      resp.server_transmit_timestamp_us() - resp.server_receive_timestamp_us();
  if (local_interval_us < remote_interval_us) {
    VLOG(1) << Substitute("inconsistency from $0: local sample interval "
                          "is less than remote sample interval ($1 vs $2)",
                          from_server.ToString(),
                          local_interval_us, remote_interval_us);
    return true;
  }
  int64_t roundtrip_delay_us = local_interval_us - remote_interval_us;

  // The time of the sample is chosen as a midway through the local measurement
  // period. It's assumed the relative frequency local-vs-remote clock
  // is constant during the measurement for each sample.
  int64_t sample_local_time = (p->send_time_mono_us + recv_time_mono_us) / 2;

  // The offset between two clocks is estimated given two samples at each side:
  //   local:
  //     T0: p->send_time_mono_us
  //     T1: recv_time_mono_us
  //   remote:
  //     T0: resp.server_receive_timestamp_us()
  //     T1: resp.server_transmit_timestamp_us()
  //
  // The estimated offset is the difference between the mid-points:
  //   offset = (remote.T0 + remote.T1) / 2 - (local.T0 + local.T1) / 2
  int64_t clock_offset_us =
      ((resp.server_receive_timestamp_us() - p->send_time_mono_us) +
       (resp.server_transmit_timestamp_us() - recv_time_mono_us)) / 2;

  cleanup.cancel();

  RecordedResponse rr;
  rr.addr = from_server;
  rr.tx_timestamp = resp.server_transmit_timestamp_us();
  rr.monotime = sample_local_time;
  rr.offset_us = clock_offset_us;
  rr.error_us = (roundtrip_delay_us + 1) / 2 + resp.root_dispersion_us();
  RecordResponse(p->server, rr);

  return true;
}

void BuiltInNtp::TimeOutRequests() {
  const auto now = GetMonoTimeMicrosRaw();
  for (auto it = pending_.begin(); it != pending_.end();) {
    auto& r = *it;
    if (now - r->send_time_mono_us >
        FLAGS_builtin_ntp_request_timeout_ms * 1000) {
      VLOG(1) << Substitute("timed out NTP request to server $0",
                            r->addr.ToString());
      // Switch to the next IP address associated with this server's hostname.
      r->server->TimeoutAndSwitchNextServer();
      it = pending_.erase(it);
    } else {
      ++it;
    }
  }
}

Status BuiltInNtp::SendRequests() {
  const auto poll_interval = FLAGS_builtin_ntp_poll_interval_ms;
  const auto now = MonoTime::Now();
  for (const auto& s : servers_) {
    if (now >= s->next_poll()) {
      RETURN_NOT_OK(SendPoll(s.get()));
      // The trivial IO loop of this implementation is better off with less
      // 'wavy' IO, especially if all NTP servers are at the same RTT distance.
      int delay = poll_interval + rng_.Uniform(poll_interval / 2);
      s->UpdateNextPoll(now + MonoDelta::FromMilliseconds(delay));
    }
  }
  return Status::OK();
}

Status BuiltInNtp::SendPoll(ServerState* s) {
  CHECK_NE(-1, socket_.GetFd());
  const Sockaddr& addr = s->cur_addr();
  sockaddr_in si_other = addr.addr();

  unique_ptr<PendingRequest> pr(new PendingRequest);
  pr->server = s;
  pr->request = CreateClientPacket();
  pr->addr = addr;

  // This is wrapped into a lambda for usage in RETRY_ON_EINTR() below.
  const auto sender = [&]() {
    // Yield before we send, so that we grab the time and send the packet at the
    // start of a fresh scheduler quantum. This reduces the likelihood of being
    // context-switched out in the next few lines.
    sched_yield();
    pr->send_time_mono_us = GetMonoTimeMicrosRaw();
    return sendto(socket_.GetFd(), &pr->request, sizeof(pr->request),
                  /*flags=*/0, reinterpret_cast<sockaddr*>(&si_other),
                  sizeof(si_other));
  };
  ssize_t rc = -1;
  RETRY_ON_EINTR(rc, sender());
  if (rc == -1) {
    int err = errno;
    if (err == EPIPE) {
      return Status::Aborted("shutdown");
    }
    return Status::NetworkError(
        Substitute("failed to send to NTP server $0", addr.ToString()),
        ErrnoToString(err));
  }
  pending_.emplace_back(std::move(pr));

  return Status::OK();
}

void BuiltInNtp::PollThread() {
  while (!is_shutdown()) {
    TimeOutRequests();
    Status s = SendRequests();
    if (s.IsAborted()) {
      return;
    }
    while (!is_shutdown() && TryReceivePacket()) {
      // Loop receiving packets until we have no more pending packets.
    }
  }
}

std::unique_ptr<BuiltInNtp::PendingRequest> BuiltInNtp::RemovePending(
    const Sockaddr& addr,
    const NtpPacket& response) {
  for (auto it = pending_.begin(); it != pending_.end(); ++it) {
    if ((*it)->addr == addr &&
        (*it)->request.transmit_time_f == response.orig_time_f &&
        (*it)->request.transmit_time_s == response.orig_time_s) {
      std::unique_ptr<PendingRequest> ret = std::move(*it);
      pending_.erase(it);
      return ret;
    }
  }
  return nullptr;
}

void BuiltInNtp::RecordResponse(ServerState* from_server,
                                const RecordedResponse& rr) {
  from_server->RecordPacket(rr);
  // TODO(KUDU-2939): as a part of robust clock selection algorithm, make the
  //                  dispersion of samples increasing as described in
  //                  https://tools.ietf.org/html/rfc5905 A.5.2. clock_filter()
  const auto s = CombineClocks();
  if (!s.ok()) {
    KLOG_EVERY_N_SECS(INFO, 60)
        << Substitute("combining reference clocks failed: $0", s.ToString());
  }
}

Status BuiltInNtp::FilterResponses(vector<RecordedResponse>* filtered) {
  vector<RecordedResponse> result;
  result.reserve(servers_.size());
  for (const auto& server : servers_) {
    RecordedResponse response;
    auto s = server->GetBestResponse(&response);
    if (s.IsNotFound()) {
      continue;
    }
    RETURN_NOT_OK(s);
    result.emplace_back(response);
  }
  *filtered = std::move(result);
  return Status::OK();
}

BuiltInNtp::NtpPacket BuiltInNtp::CreateClientPacket() {
  // Leap: no warning, version: NTPv3, mode: client.
  static const uint8_t cNoLeapClientMode = NtpPacket::generate_lvm(
      NtpPacket::kNoWarning, kNtpVersion, NtpPacket::kClient);

  NtpPacket p = {}; // zero-initialization
  p.lvm = cNoLeapClientMode;

  // The transmit_time_{s,f} fields are used as a nonce and they don't carry
  // the actual time.
  p.transmit_time_s = rng_.Next();
  p.transmit_time_f = rng_.Next();
  return p;
}

// In essence, the code below is a version of Marzullo's algorithm: search for
// intersection intervals among the correctness intervals built from clock
// readings received from configured NTP sources.
Status BuiltInNtp::CombineClocks() {
  // See https://www.eecis.udel.edu/~mills/ntp/html/select.html for details
  // on the official NTP reference implementation. Also, check
  // A.5.2. in https://tools.ietf.org/html/rfc5905 for reference clock_filter()
  // implementation: finding the best sample from a single source among
  // available ones -- this implementation stores up to
  // kResponsesToRememberPerServer recent samples from the same server, and
  // it's necessary to find best each time when combining clock measurements
  // from different servers.
  vector<RecordedResponse> responses;
  RETURN_NOT_OK(FilterResponses(&responses));

  const auto now = GetMonoTimeMicrosRaw();
  const Interval best_interval = FindIntersection(responses, now);
  VLOG(2) << Substitute("intersection interval: ($0, $1)",
                        best_interval.first, best_interval.second);
  if (best_interval == kIntervalNone) {
    return Status::Incomplete("no intersection of clock correctness intervals");
  }
  DCHECK_GT(best_interval.first, 0);
  DCHECK_GT(best_interval.second, 0);

  // From the reference NTP implementation:
  //   ... A candidate with a correctness interval that contains no points in
  //   the intersection interval is a "falseticker". A candidate with a
  //   correctness interval that contains points in the intersection interval is
  //   a "truechimer" and the best offset estimate is the midpoint of its
  //   correctness interval. On the other hand, the midpoint sample produced
  //   by the clock filter algorithm is the maximum likelihood estimate and thus
  //   best represents the truechimer time ...
  //
  //   ... The clock select algorithm again scans the correctness intervals. If
  //   the right endpoint of the correctness interval for a candidate is greater
  //   than the left endpoint of the intersection interval, or if the left
  //   endpoint of the correctness interval is less than the right endpoint of
  //   the intersection interval, the candidate is a truechimer; otherwise,
  //   it is a falseticker ...
  int falsetickers_num = 0;
  int all_sources_num = 0;
  for (const auto& r : responses) {
    ++all_sources_num;
    const auto& addr = r.addr.ToString();
    int64_t wall = now + r.offset_us;
    int64_t error = r.error_us + (now - r.monotime) * kSkewPpm / 1e6;
    DCHECK_GE(now, r.monotime);
    DCHECK_GE(error, 0);
    if (wall + error > best_interval.first ||
        wall - error < best_interval.second) {
      // This NTP source is marked as "truechimer".
      continue;
    }
    // This NTP source is marked as "falseticker".
    ++falsetickers_num;
    VLOG(2) << Substitute("($0, $1): correctness interval from falseticker $2",
                          wall - error, wall + error, addr);
  }
  if (falsetickers_num > all_sources_num / 2) {
    VLOG(1) << Substitute("majority of recent NTP samples ($0 out of $1) "
                          "found to be from falsetickers",
                          falsetickers_num, all_sources_num);
  }

  int64_t compute_wall = (best_interval.first + best_interval.second) / 2;
  int64_t compute_error = (best_interval.second - best_interval.first) / 2;
  {
    // Extra sanity check to make sure walltime doesn't go back.
    std::lock_guard<rw_spinlock> l(last_computed_lock_);
    if (last_computed_.wall > compute_wall) {
      auto msg = Substitute("walltime would move into past: "
                            "current walltime $0, last walltime $1, "
                            "current mono $2, last mono $3, "
                            "current error $4, last error $5",
                            compute_wall, last_computed_.wall,
                            now, last_computed_.mono,
                            compute_error, last_computed_.error);
      VLOG(1) << msg;
      return Status::IllegalState(msg);
    }
    last_computed_.is_synchronized = true;
    last_computed_.mono = now;
    last_computed_.wall = compute_wall;
    last_computed_.error = compute_error;
  }
  VLOG(2) << Substitute("combined clocks: $0 $1 $2",
                        now, compute_wall, compute_error);

  // We got a valid clock result, so wake up Init() that we are ready to be used.
  {
    MutexLock l(state_lock_);
    if (state_ == kStarting) {
      state_ = kStarted;
    }
  }

  return Status::OK();
}

} // namespace clock
} // namespace kudu
