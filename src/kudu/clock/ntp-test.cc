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

#include <algorithm>
#include <cinttypes>
#include <cstdint>
#include <ctime>
#include <iterator>
#include <memory>
#include <string>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/clock/builtin_ntp.h"
#include "kudu/clock/test/mini_chronyd.h"
#include "kudu/clock/test/mini_chronyd_test_util.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int32(ntp_initial_sync_wait_secs);
DECLARE_string(builtin_ntp_servers);
DECLARE_uint32(builtin_ntp_poll_interval_ms);

using std::back_inserter;
using std::copy;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace clock {

#define WALLTIME_DIAG_FMT   "%" PRId64 " +/- %8" PRId64 " us"

// Test to verify functionality of the built-in NTP client by communicating
// with real NTP server (chronyd) in various scenarios.
//
// NOTE: In all these scenarios, the reference chronyd servers are not using
//       the default NTP port 123 because it's a privileged one and usually
//       requires super-user privileges to bind to.
//
// NOTE: Some scenarios and sub-scenarios are disabled on macOS because
//       of a bug in chronyd: it doesn't differentiate between configured NTP
//       sources by IP address + port, using only IP address as the key.
//       On macOS, the same IP address 127.0.0.1 (loopback) is used for all
//       the test NTP servers since multiple loopback addresses from
//       the 127.0.0.0/8 range are not available out of the box. So, on macOS,
//       the end-points of the test NTP servers differ only by their port
//       number. It means chronyd doesn't see multiple NTP servers and talks
//       only with the first one as listed in chrony.conf configuration file.
//       In essence, any scenario which involves multiple NTP servers as source
//       of true time for chrony doesn't run as expected on macOS.
//
// TODO(aserbin): fix the described chrony's bug, at least in thirdparty
class BuiltinNtpWithMiniChronydTest: public KuduTest {
 protected:
  void CheckInitUnsync(BuiltInNtp* ntp_client) {
    auto deadline = MonoTime::Now() +
        MonoDelta::FromSeconds(FLAGS_ntp_initial_sync_wait_secs);
    ASSERT_OK(ntp_client->Init());
    Status s;
    do {
      uint64_t now_us;
      uint64_t err_us;
      s = ntp_client->WalltimeWithError(&now_us, &err_us);
      if (!s.IsServiceUnavailable()) {
        break;
      }
    } while (MonoTime::Now() < deadline);
    ASSERT_TRUE(s.IsServiceUnavailable()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "no valid NTP responses yet");
  }

  void CheckWallTimeUnavailable(BuiltInNtp* ntp_client,
                                const string& expected_err_msg =
      "wallclock is not synchronized: no valid NTP responses yet") {
    uint64_t now;
    uint64_t err;
    auto s = ntp_client->WalltimeWithError(&now, &err);
    ASSERT_TRUE(s.IsServiceUnavailable()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), expected_err_msg);
  }

  void CheckNoNtpSource(const vector<HostPort>& servers, int timeout_sec = 3) {
    auto s = MiniChronyd::CheckNtpSource(servers, timeout_sec);
    ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "No suitable source for synchronisation");
  }
};

// This is a basic scenario to verify that built-in NTP client is able
// to synchronize with NTP servers.
TEST_F(BuiltinNtpWithMiniChronydTest, Basic) {
  vector<unique_ptr<MiniChronyd>> servers;
  vector<HostPort> servers_endpoints;
  for (auto idx = 0; idx < 3; ++idx) {
    MiniChronydOptions options;
    options.index = idx;
    unique_ptr<MiniChronyd> chrony;
    ASSERT_OK(StartChronydAtAutoReservedPort(&chrony, &options));
    servers_endpoints.emplace_back(options.bindaddress, options.port);
    servers.emplace_back(std::move(chrony));
  }

  // All chronyd servers that use the system clock as a reference lock should
  // present themselves as a set of NTP servers suitable for synchronisation.
  ASSERT_OK(MiniChronyd::CheckNtpSource(servers_endpoints));

  // chronyd supports very short polling intervals (down to 1/64 second).
  FLAGS_builtin_ntp_poll_interval_ms = 50;
  BuiltInNtp c(servers_endpoints);
  ASSERT_OK(c.Init());
  ASSERT_EVENTUALLY([&] {
    uint64_t now_us;
    uint64_t err_us;
    ASSERT_OK(c.WalltimeWithError(&now_us, &err_us));
  });

  // Make sure WalltimeWithError() works with the built-in NTP client once
  // it has initted/synchronized with the reference test NTP servers.
  for (auto i = 0; i < 5; ++i) {
    SleepFor(MonoDelta::FromMilliseconds(500));
    uint64_t now, error;
    ASSERT_OK(c.WalltimeWithError(&now, &error));
    LOG(INFO) << StringPrintf("built-in: " WALLTIME_DIAG_FMT, now, error);
  }
}

#ifndef __APPLE__
// Make sure the built-in client doesn't latch on NTP servers if they have their
// true time spaced far away from each other. Basically, when no intersection is
// registered between the true time reference intervals, the build-in NTP client
// should not synchronize its clock with any of the servers.
//
// NOTE: see class-wide notes for details on macOS-specific exclusion
TEST_F(BuiltinNtpWithMiniChronydTest, NoIntersectionIntervalAtStartup) {
  vector<HostPort> servers_endpoints;
  vector<unique_ptr<MiniChronyd>> servers;
  for (auto idx = 0; idx < 3; ++idx) {
    MiniChronydOptions options;
    options.index = idx;
    unique_ptr<MiniChronyd> chrony;
    ASSERT_OK(StartChronydAtAutoReservedPort(&chrony, &options));
    servers_endpoints.emplace_back(options.bindaddress, options.port);
    servers.emplace_back(std::move(chrony));
  }

  const auto time_now = time(nullptr);
  ASSERT_OK(servers[0]->SetTime(time_now - 60));
  ASSERT_OK(servers[1]->SetTime(time_now + 60));

  NO_FATALS(CheckNoNtpSource(servers_endpoints));

  // Don't wait for too long: we don't expect to synchronize anyways.
  FLAGS_ntp_initial_sync_wait_secs = 2;
  // chronyd supports very short polling intervals (down to 1/64 second).
  FLAGS_builtin_ntp_poll_interval_ms = 50;

  BuiltInNtp c(servers_endpoints);
  NO_FATALS(CheckInitUnsync(&c));
  NO_FATALS(CheckWallTimeUnavailable(&c));
}
#endif

// A scenario where the built-in NTP client is pointed to various combinations
// of synchronized and unsynchronized NTP servers. In the scenarios where the
// set of NTP servers is not a good clock source, the client shouldn't
// synchronize and report appropriate error upon calling WalltimeWithError().
TEST_F(BuiltinNtpWithMiniChronydTest, SyncAndUnsyncReferenceServers) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  vector<unique_ptr<MiniChronyd>> sync_servers;
  vector<HostPort> sync_servers_refs;
  for (auto idx = 0; idx < 2; ++idx) {
    MiniChronydOptions options;
    options.index = idx;
    unique_ptr<MiniChronyd> chrony;
    ASSERT_OK(StartChronydAtAutoReservedPort(&chrony, &options));
    sync_servers_refs.emplace_back(options.bindaddress, options.port);
    sync_servers.emplace_back(std::move(chrony));
  }

  // Introduce a huge offset between the true time tracked
  // by each of the servers above.
  const auto time_now = time(nullptr);
  ASSERT_OK(sync_servers[0]->SetTime(time_now - 60));
  ASSERT_OK(sync_servers[1]->SetTime(time_now + 60));

  // Verify that chronyd observes the expected conditions itself.
  ASSERT_OK(MiniChronyd::CheckNtpSource({ sync_servers_refs[0] }));
  ASSERT_OK(MiniChronyd::CheckNtpSource({ sync_servers_refs[1] }));
#ifndef __APPLE__
  // NOTE: see class-wide notes for details on macOS-specific exclusion
  NO_FATALS(CheckNoNtpSource(sync_servers_refs));
#endif

  vector<unique_ptr<MiniChronyd>> unsync_servers;
  vector<HostPort> unsync_servers_refs;

  // Start a server without any true time reference: it has nothing to
  // synchronize with and stays unsynchronized.
  {
    MiniChronydOptions options;
    options.index = 2;
    options.local = false;
    unique_ptr<MiniChronyd> chrony;
    ASSERT_OK(StartChronydAtAutoReservedPort(&chrony, &options));
    unsync_servers_refs.emplace_back(options.bindaddress, options.port);
    unsync_servers.emplace_back(std::move(chrony));
  }

  // Another NTP server which uses those two synchronized, but far from each
  // other NTP servers. As the result, the new NTP server runs unsynchonized.
  {
    MiniChronydOptions options;
    options.index = 3;
    options.local = false;
    for (const auto& server : sync_servers) {
      MiniChronydServerOptions server_options;
      server_options.address = server->options().bindaddress;
      server_options.port = server->options().port;
      options.servers.emplace_back(std::move(server_options));
    }

    unique_ptr<MiniChronyd> chrony;
    ASSERT_OK(StartChronydAtAutoReservedPort(&chrony, &options));
    unsync_servers_refs.emplace_back(options.bindaddress, options.port);
    unsync_servers.emplace_back(std::move(chrony));
  }

  // chronyd supports very short polling intervals (down to 1/64 second).
  FLAGS_builtin_ntp_poll_interval_ms = 50;
  // Don't wait for too long: we don't expect to synchronize anyways.
  FLAGS_ntp_initial_sync_wait_secs = 2;

  // A sub-scenario to verify that the built-in NTP client behaves as expected
  // when provided a set of servers which are not good NTP source:
  //   * Make sure that the reference NTP client doesn't consider the specified
  //     NTP servers as a good clock source.
  //   * Make sure the built-in NTP client doesn't "latch" with the specified
  //     not-so-good NTP source.
  auto check_not_a_good_clock_source = [&](const vector<HostPort>& refs) {
    // Verify chronyd's client itself does not accept the set of of NTP sources.
    NO_FATALS(CheckNoNtpSource(refs));

    BuiltInNtp c(refs);
    NO_FATALS(CheckInitUnsync(&c));
    NO_FATALS(CheckWallTimeUnavailable(&c));
  };

  // Use first unsynchronized server.
  NO_FATALS(check_not_a_good_clock_source({ unsync_servers_refs[0] }));

#ifndef __APPLE__
  // NOTE: see class-wide notes for details on macOS-specific exclusion

  // Use second unsynchronized server.
  check_not_a_good_clock_source({ unsync_servers_refs[1] });

  // Use both unsynchronized servers.
  check_not_a_good_clock_source(unsync_servers_refs);

  // Mix all available servers. The NTP client shouldn't be able to synchronize
  // because the only synchronized servers are too far from each other and
  // the rest are simply unsynchronized.
  {
    vector<HostPort> refs;
    copy(sync_servers_refs.begin(), sync_servers_refs.end(),
         back_inserter(refs));
    copy(unsync_servers_refs.begin(), unsync_servers_refs.end(),
         back_inserter(refs));

    check_not_a_good_clock_source(refs);
  }

  // If using just one synchronized server, mixing in unsynchronized servers
  // shouldn't stop the client from synchronizing and working: the client should
  // latch with the good clock source, ignoring the other one.
  for (const auto& ref : sync_servers_refs) {
    vector<HostPort> refs;
    refs.emplace_back(ref);
    copy(unsync_servers_refs.begin(), unsync_servers_refs.end(),
         back_inserter(refs));

    // Verify chronyd's client itself accepts the set of of NTP sources.
    ASSERT_OK(MiniChronyd::CheckNtpSource(refs));

    BuiltInNtp c(refs);
    ASSERT_OK(c.Init());
    ASSERT_EVENTUALLY([&] {
      uint64_t now_us;
      uint64_t err_us;
      ASSERT_OK(c.WalltimeWithError(&now_us, &err_us));
    });
  }
#endif // #ifndef __APPLE__
}

} // namespace clock
} // namespace kudu
