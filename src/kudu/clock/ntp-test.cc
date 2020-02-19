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

#if !defined(NO_CHRONY)
#include <algorithm>
#include <cinttypes>
#include <cstdint>
#include <ctime>
#endif
#include <iterator>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#if !defined(NO_CHRONY)
#include <glog/logging.h>
#endif
#include <gtest/gtest.h>

#include "kudu/clock/builtin_ntp-internal.h"
#if !defined(NO_CHRONY)
#include "kudu/clock/builtin_ntp.h"
#include "kudu/clock/test/mini_chronyd.h"
#include "kudu/clock/test/mini_chronyd_test_util.h"
#include "kudu/gutil/integral_types.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/cloud/instance_detector.h"
#include "kudu/util/cloud/instance_metadata.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#endif

DECLARE_int32(ntp_initial_sync_wait_secs);
DECLARE_string(builtin_ntp_servers);
DECLARE_uint32(builtin_ntp_poll_interval_ms);
DECLARE_uint32(cloud_metadata_server_request_timeout_ms);

METRIC_DECLARE_entity(server);

using kudu::clock::internal::FindIntersection;
using kudu::clock::internal::Interval;
using kudu::clock::internal::RecordedResponse;
using kudu::clock::internal::kIntervalNone;
using kudu::clock::kStandardNtpPort;
using kudu::cloud::InstanceDetector;
using std::back_inserter;
using std::copy;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace clock {

// Few scenarios for the intersection interval in case of a single NTP response.
// Being a trivial case with regard to the intersection logic itself, it's
// a good case to verify that the uncertainty of the intersection interval
// widens in accordance with the skew of the clock when current time drifts
// further away from the time when a sample of the reference clock was captured.
TEST(TimeIntervalsTest, SingleResponse) {
  // Zero clock skew: this is something theoretical, but anyways.
  // In case of zero clock skew, regardless of the difference between the
  // capture time and current time, the result error is not widening,
  // and the result interval is drifting along with current time.
  {
    // Zero error.
    {
      const vector<RecordedResponse> responses = {
        { 0, 1, 0, },
      };

      const auto res0 = FindIntersection(responses, 0, 0);
      ASSERT_EQ(1, res0.first);
      ASSERT_EQ(1, res0.second);

      const auto res1 = FindIntersection(responses, 100, 0);
      ASSERT_EQ(101, res1.first);
      ASSERT_EQ(101, res1.second);
    }

    // Non-zero error.
    {
      const vector<RecordedResponse> responses = {
        { 0, 10, 1, },
      };

      const auto res0 = FindIntersection(responses, 0, 0);
      ASSERT_EQ(9, res0.first);
      ASSERT_EQ(11, res0.second);

      const auto res1 = FindIntersection(responses, 100, 0);
      ASSERT_EQ(109, res1.first);
      ASSERT_EQ(111, res1.second);
    }
  }

  // Non-zero clock skew.
  // In case of non-zero clock skew, the intersection interval is widening when
  // current time drifts apart from the capture time. The error of a captured
  // sample adds constant delta to the error of result intersection interval.
  {
    // Zero error.
    {
      {
        const vector<RecordedResponse> responses = {
          { 0, 10, 0, },
        };

        const auto res0 = FindIntersection(responses, 0, 1000000);
        ASSERT_EQ(10, res0.first);
        ASSERT_EQ(10, res0.second);

        const auto res1 = FindIntersection(responses, 100, 1000000);
        ASSERT_EQ(10, res1.first);
        ASSERT_EQ(210, res1.second);

        const auto res2 = FindIntersection(responses, 100, 2000000);
        ASSERT_EQ(-90, res2.first);
        ASSERT_EQ(310, res2.second);

        const auto res3 = FindIntersection(responses, 200, 1000000);
        ASSERT_EQ(10, res3.first);
        ASSERT_EQ(410, res3.second);
      }
    }

    // Non-zero error.
    {
      {
        const vector<RecordedResponse> responses = {
          { 0, 10, 1, },
        };

        const auto res0 = FindIntersection(responses, 0, 1000000);
        ASSERT_EQ(9, res0.first);
        ASSERT_EQ(11, res0.second);

        const auto res1 = FindIntersection(responses, 100, 1000000);
        ASSERT_EQ(9, res1.first);
        ASSERT_EQ(211, res1.second);

        const auto res2 = FindIntersection(responses, 50, 2000000);
        ASSERT_EQ(-41, res2.first);
        ASSERT_EQ(161, res2.second);
      }
    }
  }
}

// A case where two samples of the reference clock were acquired.
TEST(TimeIntervalsTest, TwoResponses) {
  // The same interval: a single point at the time of sample capture.
  {
    const vector<RecordedResponse> responses = {
      { 0, 3, 0, },
      { 0, 3, 0, },
    };

    const auto res0 = FindIntersection(responses, 0, 1000000);
    ASSERT_EQ(3, res0.first);
    ASSERT_EQ(3, res0.second);

    const auto res1 = FindIntersection(responses, 1, 1000000);
    // [3, 5] & [3, 5]
    ASSERT_EQ(3, res1.first);
    ASSERT_EQ(5, res1.second);
  }

  // Single intersection point: the edge.
  {
    const vector<RecordedResponse> responses = {
      { 0, 2, 1, },
      { 0, 5, 2, },
    };

    const auto res0 = FindIntersection(responses, 0, 1000000);
    // [1, 3] & [3, 7]
    ASSERT_EQ(3, res0.first);
    ASSERT_EQ(3, res0.second);
  }

  // Embedded intervals with the same reported time but different errors.
  {
    const vector<RecordedResponse> responses = {
      { 0, 10, 1, },
      { 0, 10, 2, },
    };

    const auto res0 = FindIntersection(responses, 0, 1000000);
    // [9, 11] & [8, 12]
    ASSERT_EQ(9, res0.first);
    ASSERT_EQ(11, res0.second);

    const auto res1 = FindIntersection(responses, 5, 1000000);
    // [9, 21] & [8, 22]
    ASSERT_EQ(9, res1.first);
    ASSERT_EQ(21, res1.second);
  }

  // Embedded intervals with the same reported time but different errors.
  // The second interval represents a corner case of zero-error sample.
  {
    const vector<RecordedResponse> responses = {
      { 0, 10, 1, },
      { 0, 10, 0, },
    };

    const auto res0 = FindIntersection(responses, 0, 1000000);
    // [9, 11] & [10, 10]
    ASSERT_EQ(10, res0.first);
    ASSERT_EQ(10, res0.second);

    const auto res1 = FindIntersection(responses, 5, 1000000);
    // [9, 21] & [10, 20]
    ASSERT_EQ(10, res1.first);
    ASSERT_EQ(20, res1.second);
  }

  // Embedded intervals with different reported time.
  {
    const vector<RecordedResponse> responses = {
      { 0, 1, 1, },
      { 0, 2, 2, },
    };

    const auto res0 = FindIntersection(responses, 0, 1000000);
    // [0, 2] & [0, 4]
    ASSERT_EQ(0, res0.first);
    ASSERT_EQ(2, res0.second);

    const auto res1 = FindIntersection(responses, 10, 1000000);
    // [0, 22] & [0, 24]
    ASSERT_EQ(0, res1.first);
    ASSERT_EQ(22, res1.second);
  }

  // Non-intersecting (as of time of capture) intervals.
  {
    const vector<RecordedResponse> responses = {
      { 0, 1, 1, },
      { 0, 5, 1, },
    };

    const auto res0 = FindIntersection(responses, 0, 1000000);
    // [0, 2] & [4, 6]
    ASSERT_EQ(kIntervalNone, res0);

    const auto res1 = FindIntersection(responses, 5, 1000000);
    // [0, 12] & [4, 16]
    ASSERT_EQ(4, res1.first);
    ASSERT_EQ(12, res1.second);
  }
}

// A case where three samples of the reference clock were acquired.
TEST(TimeIntervalsTest, ThreeResponses) {
  // The same interval: a single point at the time of sample capture.
  {
    const vector<RecordedResponse> responses = {
      { 0, 3, 0, },
      { 0, 3, 0, },
      { 0, 3, 0, },
    };

    const auto res0 = FindIntersection(responses, 0, 1000000);
    ASSERT_EQ(3, res0.first);
    ASSERT_EQ(3, res0.second);

    const auto res1 = FindIntersection(responses, 1, 1000000);
    // [3, 5] & [3, 5] & [3, 5]
    ASSERT_EQ(3, res1.first);
    ASSERT_EQ(5, res1.second);
  }

  // Non-intersecting (as of time of capture) intervals.
  {
    const vector<RecordedResponse> responses = {
      { 0, 1, 1, },
      { 0, 4, 1, },
      { 0, 7, 1, },
    };

    const auto res0 = FindIntersection(responses, 0, 1000000);
    // [0, 2] & [3, 5] & [6, 8]
    ASSERT_EQ(kIntervalNone, res0);

    const auto res1 = FindIntersection(responses, 10, 1000000);
    // [0, 22] & [3, 25] & [6, 28]
    ASSERT_EQ(6, res1.first);
    ASSERT_EQ(22, res1.second);
  }

  // Embedded intervals with the same reported time but different errors.
  // The second interval represents a corner case of zero-error sample.
  {
    const vector<RecordedResponse> responses = {
      { 0, 10, 5, },
      { 0, 10, 1, },
      { 0, 10, 10, },
    };

    const auto res0 = FindIntersection(responses, 0, 1000000);
    // [5, 15] & [9, 11] & [0, 20]
    ASSERT_EQ(9, res0.first);
    ASSERT_EQ(11, res0.second);

    const auto res1 = FindIntersection(responses, 10, 1000000);
    // [5, 35] & [9, 31] & [0, 40]
    ASSERT_EQ(9, res1.first);
    ASSERT_EQ(31, res1.second);
  }

  // Three 'chained' intervals that have a single intersection point at the time
  // of samples capture.
  {
    const vector<RecordedResponse> responses = {
      { 0, 4, 2, },
      { 0, 6, 2, },
      { 0, 8, 2, },
    };

    const auto res0 = FindIntersection(responses, 0, 1000000);
    // [2, 6] & [4, 8] & [6, 10]
    ASSERT_EQ(6, res0.first);
    ASSERT_EQ(6, res0.second);

    const auto res1 = FindIntersection(responses, 1, 1000000);
    // [2, 8] & [4, 10] & [6, 12]
    ASSERT_EQ(6, res1.first);
    ASSERT_EQ(8, res1.second);
  }

  // Three 'chained' intervals without a single intersection point.
  // As of now, the first intersection interval (the earlier) is chosen.
  {
    const vector<RecordedResponse> responses = {
      { 0, 3, 2, },
      { 0, 6, 2, },
      { 0, 9, 2, },
    };

    const auto res0 = FindIntersection(responses, 0, 1000000);
    // [1, 5] & [4, 8] & [7, 11]
    ASSERT_EQ(4, res0.first);
    ASSERT_EQ(5, res0.second);
  }

  // Two intersections: first two points, then two intervals. As of now,
  // the first (the earlier) is chosen.
  {
    const vector<RecordedResponse> responses = {
      { 0, 3, 2, },
      { 0, 6, 2, },
      { 0, 9, 2, },
    };

    const auto res0 = FindIntersection(responses, 0, 1000000);
    // [1, 5] & [4, 8] & [7, 11]
    ASSERT_EQ(4, res0.first);
    ASSERT_EQ(5, res0.second);
  }

  // One interval contains two other (no boundary intesections), so the total
  // is two intervals. The first (the earliest) one is chosen as the result.
  {
    const vector<RecordedResponse> responses = {
      { 0, 6, 6, },
      { 0, 2, 1, },
      { 0, 5, 1, },
    };

    const auto res0 = FindIntersection(responses, 0, 1000000);
    // [0, 12] & [1, 3] & [4, 6]
    ASSERT_EQ(1, res0.first);
    ASSERT_EQ(3, res0.second);
  }

  // One interval contains two other, but with boundary intesection, there is
  // one common point among all three intervals.
  {
    const vector<RecordedResponse> responses = {
      { 0, 4, 4, },
      { 0, 2, 2, },
      { 0, 6, 2, },
    };

    const auto res0 = FindIntersection(responses, 0, 1000000);
    // [0, 8] & [0, 4] & [4, 8]
    ASSERT_EQ(4, res0.first);
    ASSERT_EQ(4, res0.second);
  }
}

#if !defined(NO_CHRONY)

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
 public:
  BuiltinNtpWithMiniChronydTest()
      : metric_entity_(METRIC_ENTITY_server.Instantiate(&metric_registry_,
                                                        "ntp-test")) {
  }
 protected:
  static void CheckInitUnsync(BuiltInNtp* ntp_client) {
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

  MetricRegistry metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
};

// This is a basic scenario to verify that built-in NTP client is able
// to synchronize with NTP servers.
TEST_F(BuiltinNtpWithMiniChronydTest, Basic) {
  vector<unique_ptr<MiniChronyd>> servers;
  vector<HostPort> servers_endpoints;
  for (auto idx = 0; idx < 3; ++idx) {
    unique_ptr<MiniChronyd> chrony;
    {
      MiniChronydOptions options;
      options.index = idx;
      ASSERT_OK(StartChronydAtAutoReservedPort(std::move(options), &chrony));
    }
    servers_endpoints.emplace_back(chrony->address());
    servers.emplace_back(std::move(chrony));
  }

  // All chronyd servers that use the system clock as a reference lock should
  // present themselves as a set of NTP servers suitable for synchronisation.
  ASSERT_OK(MiniChronyd::CheckNtpSource(servers_endpoints));

  // chronyd supports very short polling intervals (down to 1/64 second).
  FLAGS_builtin_ntp_poll_interval_ms = 50;
  BuiltInNtp c(servers_endpoints, metric_entity_);
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
    unique_ptr<MiniChronyd> chrony;
    {
      MiniChronydOptions options;
      options.index = idx;
      ASSERT_OK(StartChronydAtAutoReservedPort(std::move(options), &chrony));
    }
    servers_endpoints.emplace_back(chrony->address());
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

  BuiltInNtp c(servers_endpoints, metric_entity_);
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
    unique_ptr<MiniChronyd> chrony;
    {
      MiniChronydOptions options;
      options.index = idx;
      ASSERT_OK(StartChronydAtAutoReservedPort(std::move(options), &chrony));
    }
    sync_servers_refs.emplace_back(chrony->address());
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
    unique_ptr<MiniChronyd> chrony;
    {
      MiniChronydOptions options;
      options.index = 2;
      options.local = false;
      ASSERT_OK(StartChronydAtAutoReservedPort(std::move(options), &chrony));
    }
    unsync_servers_refs.emplace_back(chrony->address());
    unsync_servers.emplace_back(std::move(chrony));
  }

  // Another NTP server which uses those two synchronized, but far from each
  // other NTP servers. As the result, the new NTP server runs unsynchonized.
  {
    unique_ptr<MiniChronyd> chrony;
    {
      MiniChronydOptions options;
      options.index = 3;
      options.local = false;
      for (const auto& server : sync_servers) {
        const auto addr = server->address();
        MiniChronydServerOptions server_options;
        server_options.address = addr.host();
        server_options.port = addr.port();
        options.servers.emplace_back(std::move(server_options));
      }
      ASSERT_OK(StartChronydAtAutoReservedPort(std::move(options), &chrony));
    }
    unsync_servers_refs.emplace_back(chrony->address());
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

    BuiltInNtp c(refs, metric_entity_);
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

    BuiltInNtp c(refs, metric_entity_);
    ASSERT_OK(c.Init());
    ASSERT_EVENTUALLY([&] {
      uint64_t now_us;
      uint64_t err_us;
      ASSERT_OK(c.WalltimeWithError(&now_us, &err_us));
    });
  }
#endif // #ifndef __APPLE__
}

// This is a basic scenario to verify that the built-in NTP client is able
// to synchronize with NTP servers provided by supported cloud environments.
TEST_F(BuiltinNtpWithMiniChronydTest, CloudInstanceNtpServer) {
  SKIP_IF_SLOW_NOT_ALLOWED();

#ifdef THREAD_SANITIZER
  // In case of TSAN builds, it takes longer to spawn threads and, overall,
  // the sanitized version of libcurl works an order of magnitude slower
  // than the regular version.
  FLAGS_cloud_metadata_server_request_timeout_ms = 10000;
#endif
  InstanceDetector detector;
  unique_ptr<cloud::InstanceMetadata> md;
  string ntp_server;
  {
    auto s = detector.Detect(&md);
    if (!s.ok()) {
      ASSERT_TRUE(s.IsNotFound()) << s.ToString();
      LOG(WARNING) << "test is skipped: non-supported or non-cloud environment";
      return;
    }
  }
  {
    ASSERT_NE(nullptr, md.get());
    auto s = md->GetNtpServer(&ntp_server);
    if (!s.ok()) {
      // The only expected error in this case is Status::NotSupported().
      ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
      LOG(WARNING) << strings::Substitute(
          "test is skipped: $0 cloud instance doesn't provide NTP server",
          cloud::TypeToString(md->type()));
      return;
    }
  }

  const vector<HostPort> servers_endpoints = {{ ntp_server, kStandardNtpPort }};

  // All chronyd servers that use the system clock as a reference lock should
  // present themselves as a set of NTP servers suitable for synchronisation.
  ASSERT_OK(MiniChronyd::CheckNtpSource(servers_endpoints));

  FLAGS_builtin_ntp_poll_interval_ms = 500;
  BuiltInNtp c(servers_endpoints, metric_entity_);
  ASSERT_OK(c.Init());
  ASSERT_EVENTUALLY([&] {
    uint64_t now_us;
    uint64_t err_us;
    ASSERT_OK(c.WalltimeWithError(&now_us, &err_us));
  });

  // Make sure WalltimeWithError() works with the built-in NTP client once
  // it has initted/synchronized with the reference NTP server available
  // from within the cloud instance.
  for (auto i = 0; i < 5; ++i) {
    SleepFor(MonoDelta::FromMilliseconds(500));
    uint64_t now;
    uint64 error;
    ASSERT_OK(c.WalltimeWithError(&now, &error));
    LOG(INFO) << StringPrintf("built-in: " WALLTIME_DIAG_FMT, now, error);
  }
}

#endif // #if !defined(NO_CHRONY)

} // namespace clock
} // namespace kudu
