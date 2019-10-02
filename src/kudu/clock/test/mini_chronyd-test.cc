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

#include "kudu/clock/test/mini_chronyd.h"

#include <ctime>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/clock/test/mini_chronyd_test_util.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace clock {

class MiniChronydTest: public KuduTest {
};

// Run chronyd without any reference: neither local reference mode, nor
// reference NTP server present. Such server cannot be a good NTP source
// because its clock is unsynchronized.
TEST_F(MiniChronydTest, UnsynchronizedServer) {
  unique_ptr<MiniChronyd> chrony;
  {
    MiniChronydOptions options;
    options.local = false;
    ASSERT_OK(StartChronydAtAutoReservedPort(&chrony, &options));
  }

  // No client has talked to the NTP server yet.
  {
    MiniChronyd::ServerStats stats;
    ASSERT_OK(chrony->GetServerStats(&stats));
    ASSERT_EQ(0, stats.ntp_packets_received);
  }

  auto s = MiniChronyd::CheckNtpSource({ chrony->address() });
  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
                      "failed measure clock offset from reference NTP servers");

  // Make sure the client has communicated with the server.
  {
    MiniChronyd::ServerStats stats;
    ASSERT_OK(chrony->GetServerStats(&stats));
    ASSERT_LT(0, stats.ntp_packets_received);
  }
}

// This scenario verifies basic functionality of the mini-chronyd wrapper:
// start, stop, manually setting the reference time, etc.
TEST_F(MiniChronydTest, BasicSingleServerInstance) {
  // Start chronyd at the specified port, making sure it's serving requests.
  unique_ptr<MiniChronyd> chrony;
  ASSERT_OK(StartChronydAtAutoReservedPort(&chrony));

  // A chronyd that uses the system clock as a reference lock should present
  // itself as reliable NTP server.
  const HostPort ntp_endpoint(chrony->address());
  {
    // Make sure the server opens ports to listen and serve requests
    // from NTP clients.
    auto s = MiniChronyd::CheckNtpSource({ ntp_endpoint });
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  // Set time manually using chronyc and verify that chronyd tracks the time as
  // expected.
  ASSERT_OK(chrony->SetTime(time(nullptr) - 60));

  // Sanity check: make sure chronyd receives NTP packets which were sent
  // by chronyc and the chronyd running in client-only mode.
  MiniChronyd::ServerStats stats;
  ASSERT_OK(chrony->GetServerStats(&stats));
  ASSERT_LT(0, stats.ntp_packets_received);
  ASSERT_LT(0, stats.cmd_packets_received);
  const auto ntp_packets_received = stats.ntp_packets_received;

  {
    // After setting the clock manually to be simply offset from the reference,
    // chronyd should continue providing a good NTP source for its clients.
    auto s = MiniChronyd::CheckNtpSource({ ntp_endpoint });
    ASSERT_TRUE(s.ok()) << s.ToString();

    // The activity of checking for a reliable NTP source and fetching
    // information on the system clock synchronisation status should generate
    // additional NTP packets which should have been received by the NTP server.
    MiniChronyd::ServerStats stats;
    ASSERT_OK(chrony->GetServerStats(&stats));
    ASSERT_GT(stats.ntp_packets_received, ntp_packets_received);
  }
}

// This scenario runs multiple chronyd and verifies they can co-exist without
// conflicts w.r.t. resources such as port numbers and paths to their
// configuration files, command sockets, and other related files.
TEST_F(MiniChronydTest, BasicMultipleServerInstances) {
  vector<unique_ptr<MiniChronyd>> servers;
  vector<HostPort> ntp_endpoints;
  for (int idx = 0; idx < 5; ++idx) {
    MiniChronydOptions options;
    options.index = idx;
    unique_ptr<MiniChronyd> chrony;
    ASSERT_OK(StartChronydAtAutoReservedPort(&chrony, &options));
    ntp_endpoints.emplace_back(chrony->address());
    servers.emplace_back(std::move(chrony));
  }

  {
    // All chronyd servers that use the system clock as a reference lock should
    // present themselves as a set of NTP servers suitable for synchronisation.
    auto s = MiniChronyd::CheckNtpSource(ntp_endpoints, 10);
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  // On macOS the same loopback IP address is used for all the NTP servers:
  // they differ only by their port number. However, it seems chronyd
  // doesn't differentiate between them by IP addr + port, only by IP addr.
  // So, it means chronyd run as a client (chronyd -q/-Q) sees all those as
  // the same NTP server, and talks only with the first one (as listed
  // in the configuration file).

  // Sanity check: make sure every server received packets from the client
  // (see the note above regarding running chronyd at the same IP address
  //  but different NTP port number).
  for (auto& server : servers) {
    MiniChronyd::ServerStats stats;
    ASSERT_OK(server->GetServerStats(&stats));
#ifndef __APPLE__
    ASSERT_LT(0, stats.ntp_packets_received);
#endif
    ASSERT_LT(0, stats.cmd_packets_received);
  }

  // Offset the reference time at the servers, so they would have their
  // reference times far from each other. It doesn't matter from which point
  // the servers are offset, but it's important that they are 'far enough'
  // from each other. The idea is to make sure the client does _not_ see these
  // servers as a reliable source for time synchronisation via NTP.
  const auto ref_time = time(nullptr) - 50;
  for (auto i = 0; i < servers.size(); ++i) {
    ASSERT_OK(servers[i]->SetTime(ref_time + i * 10));
  }

#ifndef __APPLE__
  {
    // Now, with contradicting source NTP servers, it should be impossible
    // to synchronize the time.
    auto s = MiniChronyd::CheckNtpSource(ntp_endpoints, 10);
    ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "No suitable source for synchronisation");
  }
#endif
}

// This scenario runs multi-tier set of chronyd servers: few servers of
// stratum X and few more servers of stratum X+1, so the latter use the former
// as the source for synchronisation. Both set of servers should be a good clock
// source to serve NTP clients.
TEST_F(MiniChronydTest, MultiTierBasic) {
  vector<unique_ptr<MiniChronyd>> servers_0;
  vector<HostPort> ntp_endpoints_0;
  for (auto idx = 0; idx < 3; ++idx) {
    MiniChronydOptions options;
    options.index = idx;
    unique_ptr<MiniChronyd> chrony;
    ASSERT_OK(StartChronydAtAutoReservedPort(&chrony, &options));
    ntp_endpoints_0.emplace_back(chrony->address());
    servers_0.emplace_back(std::move(chrony));
  }

  vector<unique_ptr<MiniChronyd>> servers_1;
  vector<HostPort> ntp_endpoints_1;
  for (auto idx = 3; idx < 5; ++idx) {
    MiniChronydOptions options;
    options.index = idx;
    options.local = false;
    for (const auto& ref : servers_0) {
      const auto addr = ref->address();
      MiniChronydServerOptions server_options;
      server_options.address = addr.host();
      server_options.port = addr.port();
      options.servers.emplace_back(std::move(server_options));
    }
    unique_ptr<MiniChronyd> chrony;
    ASSERT_OK(StartChronydAtAutoReservedPort(&chrony, &options));
    ntp_endpoints_1.emplace_back(chrony->address());
    servers_1.emplace_back(std::move(chrony));
  }

  {
    // All chronyd servers that use the system clock as a reference lock should
    // present themselves as a set of NTP servers suitable for synchronisation.
    auto s = MiniChronyd::CheckNtpSource(ntp_endpoints_0);
    ASSERT_TRUE(s.ok()) << s.ToString();
  }
  {
    // All chronyd servers that use the above mentioned chronyd servers
    // as reference should be a set of good NTP sources as well.
    auto s = MiniChronyd::CheckNtpSource(ntp_endpoints_1);
    ASSERT_TRUE(s.ok()) << s.ToString();
  }
}

} // namespace clock
} // namespace kudu
