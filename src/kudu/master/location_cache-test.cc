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

#include "kudu/master/location_cache.h"

#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/metrics.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

METRIC_DECLARE_counter(location_mapping_cache_hits);
METRIC_DECLARE_counter(location_mapping_cache_queries);

using std::string;
using std::thread;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace master {

// Targeted test for LocationCache.
class LocationCacheTest : public KuduTest {
 protected:
  void SetUp() override {
    KuduTest::SetUp();
    metric_entity_ = METRIC_ENTITY_server.Instantiate(&metric_registry_,
                                                      "LocationCacheTest");
  }

  void CheckMetrics(int64_t expected_queries, int64_t expected_hits) {
    scoped_refptr<Counter> cache_queries(metric_entity_->FindOrCreateCounter(
        &METRIC_location_mapping_cache_queries));
    ASSERT_NE(nullptr, cache_queries.get());
    ASSERT_EQ(expected_queries, cache_queries->value());

    scoped_refptr<Counter> cache_hits(metric_entity_->FindOrCreateCounter(
        &METRIC_location_mapping_cache_hits));
    ASSERT_NE(nullptr, cache_hits.get());
    ASSERT_EQ(expected_hits, cache_hits->value());
  }

  MetricRegistry metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
};

TEST_F(LocationCacheTest, EmptyMappingCommand) {
  LocationCache cache(" ", metric_entity_.get());
  string location;
  auto s = cache.GetLocation("na", &location);
  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
  ASSERT_STR_CONTAINS(
      s.ToString(), "invalid empty location mapping command");
  NO_FATALS(CheckMetrics(1, 0));
}

TEST_F(LocationCacheTest, MappingCommandFailureExitStatus) {
  LocationCache cache("/sbin/nologin", metric_entity_.get());
  string location;
  auto s = cache.GetLocation("na", &location);
  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
  ASSERT_STR_CONTAINS(
      s.ToString(), "failed to run location mapping command: ");
  NO_FATALS(CheckMetrics(1, 0));
}

TEST_F(LocationCacheTest, MappingCommandEmptyOutput) {
  LocationCache cache("/bin/cat", metric_entity_.get());
  string location;
  auto s = cache.GetLocation("/dev/null", &location);
  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
  ASSERT_STR_CONTAINS(
      s.ToString(), "location mapping command returned invalid empty location");
  NO_FATALS(CheckMetrics(1, 0));
}

TEST_F(LocationCacheTest, MappingCommandReturnsInvalidLocation) {
  const string cmd_path = JoinPathSegments(GetTestExecutableDirectory(),
                                           "testdata/first_argument.sh");
  const string location_mapping_cmd = Substitute("$0 invalid.location",
                                                 cmd_path);
  LocationCache cache(location_mapping_cmd, metric_entity_.get());
  string location;
  auto s = cache.GetLocation("na", &location);
  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
  ASSERT_STR_CONTAINS(
      s.ToString(), "location mapping command returned invalid location");
  NO_FATALS(CheckMetrics(1, 0));
}

TEST_F(LocationCacheTest, HappyPath) {
  const string kRefLocation = "/ref_location";
  const string cmd_path = JoinPathSegments(GetTestExecutableDirectory(),
                                           "testdata/first_argument.sh");
  const string location_mapping_cmd = Substitute("$0 $1",
                                                 cmd_path, kRefLocation);
  LocationCache cache(location_mapping_cmd, metric_entity_.get());
  NO_FATALS(CheckMetrics(0, 0));

  string location;
  auto s = cache.GetLocation("key_0", &location);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(kRefLocation, location);
  NO_FATALS(CheckMetrics(1, 0));

  s = cache.GetLocation("key_1", &location);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(kRefLocation, location);
  NO_FATALS(CheckMetrics(2, 0));

  s = cache.GetLocation("key_1", &location);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(kRefLocation, location);
  NO_FATALS(CheckMetrics(3, 1));

  s = cache.GetLocation("key_0", &location);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(kRefLocation, location);
  NO_FATALS(CheckMetrics(4, 2));
}

TEST_F(LocationCacheTest, ConcurrentRequests) {
  static constexpr auto kNumThreads = 32;
  const string kRefLocation = "/ref_location";
  const string cmd_path = JoinPathSegments(GetTestExecutableDirectory(),
                                           "testdata/first_argument.sh");
  const string location_mapping_cmd = Substitute("$0 $1",
                                                 cmd_path, kRefLocation);
  LocationCache cache(location_mapping_cmd, metric_entity_.get());
  NO_FATALS(CheckMetrics(0, 0));

  for (auto iter = 0; iter < 2; ++iter) {
    vector<thread> threads;
    threads.reserve(kNumThreads);
    for (auto idx = 0; idx < kNumThreads; ++idx) {
      threads.emplace_back([&cache, &kRefLocation, idx]() {
        string location;
        auto s = cache.GetLocation(Substitute("key_$0", idx), &location);
        CHECK(s.ok()) << s.ToString();
        CHECK_EQ(kRefLocation, location);
      });
    }
    for (auto& t : threads) {
      t.join();
    }
    // Expecting to account for every query, and the follow-up iteration
    // should result in every query hitting the cache.
    NO_FATALS(CheckMetrics(kNumThreads * (iter + 1),
                           kNumThreads * iter));
  }
}

} // namespace master
} // namespace kudu
