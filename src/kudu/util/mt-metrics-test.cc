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

#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/debug/leakcheck_disabler.h"
#include "kudu/util/env.h"
#include "kudu/util/metrics.h"
#include "kudu/util/test_util.h"

DEFINE_int32(mt_metrics_test_num_threads, 4,
             "Number of threads to spawn in mt metrics tests");

METRIC_DEFINE_entity(test_entity);

namespace kudu {

using debug::ScopedLeakCheckDisabler;
using std::string;
using std::thread;
using std::vector;

class MultiThreadedMetricsTest : public KuduTest {
 public:
  static void RegisterCounters(const scoped_refptr<MetricEntity>& metric_entity,
                               const string& name_prefix, int num_counters);

  MetricRegistry registry_;
};

// Call increment on a Counter a bunch of times.
static void CountWithCounter(scoped_refptr<Counter> counter, int num_increments) {
  for (int i = 0; i < num_increments; i++) {
    counter->Increment();
  }
}

// Helper function that spawns and then joins a bunch of threads.
static void RunWithManyThreads(const std::function<void()>& f, int num_threads) {
  vector<thread> threads;
  threads.reserve(num_threads);
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back([f]() { f(); });
  }
  for (int i = 0; i < num_threads; i++) {
    threads[i].join();
  }
}

METRIC_DEFINE_counter(test_entity, test_counter, "Test Counter",
                      MetricUnit::kRequests, "Test counter",
                      kudu::MetricLevel::kInfo);

// Ensure that incrementing a counter is thread-safe.
TEST_F(MultiThreadedMetricsTest, CounterIncrementTest) {
  scoped_refptr<Counter> counter = new Counter(&METRIC_test_counter);
  int num_threads = FLAGS_mt_metrics_test_num_threads;
  int num_increments = 1000;
  RunWithManyThreads([=]() { CountWithCounter(counter, num_increments); },
                     num_threads);
  ASSERT_EQ(num_threads * num_increments, counter->value());
}

// Helper function to register a bunch of counters in a loop.
void MultiThreadedMetricsTest::RegisterCounters(
    const scoped_refptr<MetricEntity>& metric_entity,
    const string& name_prefix,
    int num_counters) {
  uint64_t tid = Env::Default()->gettid();
  for (int i = 0; i < num_counters; i++) {
    // This loop purposefully leaks metrics prototypes, because the metrics system
    // expects the prototypes and their names to live forever. This is the only
    // place we dynamically generate them for the purposes of a test, so it's easier
    // to just leak them than to figure out a way to manage lifecycle of objects that
    // are typically static.
    ScopedLeakCheckDisabler disabler;

    string name = strings::Substitute("$0-$1-$2", name_prefix, tid, i);
    auto proto = new CounterPrototype(MetricPrototype::CtorArgs(
        "test_entity", strdup(name.c_str()), "Test Counter",
        MetricUnit::kOperations, "test counter",
        MetricLevel::kInfo));
    proto->Instantiate(metric_entity)->Increment();
  }
}

// Ensure that adding a counter to a registry is thread-safe.
TEST_F(MultiThreadedMetricsTest, AddCounterToRegistryTest) {
  scoped_refptr<MetricEntity> entity = METRIC_ENTITY_test_entity.Instantiate(&registry_, "my-test");
  int num_threads = FLAGS_mt_metrics_test_num_threads;
  int num_counters = 1000;
  RunWithManyThreads([=]() { RegisterCounters(entity, "prefix", num_counters); },
                     num_threads);
  ASSERT_EQ(num_threads * num_counters, entity->UnsafeMetricsMapForTests().size());
}

} // namespace kudu
