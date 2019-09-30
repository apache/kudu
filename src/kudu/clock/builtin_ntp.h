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

#include <cstdint>
#include <list>
#include <memory>
#include <string>
#include <vector>

#include "kudu/clock/time_service.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/locks.h"
#include "kudu/util/mutex.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"

namespace kudu {

class HostPort;
class Sockaddr;
class Thread;

namespace clock {

namespace internal {
struct RecordedResponse;
} // namespace internal

// This time service is based on a simplified NTP client implementation.
// It's not RFC-compliant yet (RFC 5905). The most important missing pieces are:
//   * support of iburst/burst operation modes (see KUDU-2937)
//   * handling of KoD packets (see KUDU-2938)
//   * strict clock selection algorithm (see KUDU-2939)
//   * measuring and applying local clock skew (see KUDU-2940)
//   * support crypto authn for NTP packets (see KUDU-2941)
//
// The built-in NTP client keeps track of the true time information it receives
// from configured NTP servers and maintains walltime with error estimation.
// The client neither drives the node's wallclock nor relies on it in any way,
// it only uses raw monotonic clock to estimate the true time based on
// responses from NTP servers. With built-in client properly configured,
// there is no need to synchronize the system clock of nodes where Kudu
// masters and tablet servers are running with NTP servers.
//
// See http://www.ntp.org/ntpfaq/NTP-s-algo.htm on introduction to basic NTP
// concepts.
class BuiltInNtp : public TimeService {
 public:
  // Create an instance using the servers specified in --builtin_ntp_servers
  // as NTP sources.
  BuiltInNtp();

  // Create an instance using the specified servers as NTP sources. The set
  // of source NTP servers must not be empty.
  explicit BuiltInNtp(std::vector<HostPort> servers);

  ~BuiltInNtp() override;

  Status Init() override;

  Status WalltimeWithError(uint64_t* now_usec, uint64_t* error_usec) override;

  int64_t skew_ppm() const override {
    return kSkewPpm;
  }

  void DumpDiagnostics(std::vector<std::string>* log) const override;

 private:
  class ServerState;
  struct NtpPacket;
  struct PendingRequest;

  // Information on the computed walltime.
  struct WalltimeSnapshot {
    WalltimeSnapshot()
        : mono(0),
          wall(0),
          error(0),
          is_synchronized(false) {
    }
    int64_t mono;
    int64_t wall;
    int64_t error;
    bool is_synchronized;
  };

  // Upper estimate for a clock skew.
  static constexpr int kSkewPpm = 500;

  // Implementation of Init().
  Status InitImpl();

  // Populate run-time structures with the specified information on NTP servers.
  Status PopulateServers(std::vector<HostPort> servers);

  bool is_shutdown() const;
  void Shutdown();

  // Read data from client NTP socket and parse the contents, adding the result
  // into the set of responses per server if the data validation passes. This
  // method returns 'false' if there was a low-level error while reading data
  // from the socket, and 'true' otherwise (regardless the validity of the data).
  bool TryReceivePacket();

  // Iterate over all pending requests and remove all requests which have
  // already timed out.
  void TimeOutRequests();

  // Iterate over all scheduled NTP requests and send ones which are at or past
  // their scheduled time.
  Status SendRequests();

  // Send a request to the specified server.
  Status SendPoll(ServerState* s);

  // The IO loop thread: sending and receiving NTP packets to the configured
  // servers.
  void PollThread();

  // Find and return information on the corresponding request for the specified
  // response received from server with given address. This function returns
  // a smart pointer to non-null PendingRequest structure if a request is found,
  // and nullptr smartpointer wrapper otherwise (a response might be expired and
  // removed from the queue prior to call).
  std::unique_ptr<PendingRequest> RemovePending(const Sockaddr& addr,
                                                const NtpPacket& response);

  // Record and process response received from NTP server.
  void RecordResponse(ServerState* from_server,
                      const internal::RecordedResponse& rr);

  // Among all available responses, select the best ones to use in the clock
  // selection algorithm.
  Status FilterResponses(std::vector<internal::RecordedResponse>* filtered);

  // Create NTP packet to send to a server.
  NtpPacket CreateClientPacket();

  // Compute walltime and its estimated error from the true time
  // based on responses received so far from configured NTP servers.
  Status CombineClocks();

  Random rng_;
  Socket socket_;

  // Protects 'last_computed_'.
  mutable rw_spinlock last_computed_lock_;
  WalltimeSnapshot last_computed_;

  // Protects 'state_'.
  mutable Mutex state_lock_;
  enum State {
    kUninitialized,
    kStarting,
    kStarted,
    kShutdown
  };
  State state_ = kUninitialized;

  std::vector<std::unique_ptr<ServerState>> servers_;

  std::list<std::unique_ptr<PendingRequest>> pending_;

  // The polling thread. Responsible for sending/receiving NTP packets and
  // updating the maintained walltime based on the NTP responses received.
  scoped_refptr<Thread> thread_;

  DISALLOW_COPY_AND_ASSIGN(BuiltInNtp);
};

} // namespace clock
} // namespace kudu
