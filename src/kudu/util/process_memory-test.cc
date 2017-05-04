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

#include <atomic>
#include <thread>
#include <vector>

#include "kudu/util/monotime.h"
#include "kudu/util/process_memory.h"
#include "kudu/util/test_util.h"

using std::atomic;
using std::thread;
using std::vector;

namespace kudu {

// Microbenchmark for our new/delete hooks which track process-wide
// memory consumption.
TEST(ProcessMemory, BenchmarkConsumptionTracking) {
  const int kNumThreads = 200;
  vector<thread> threads;
  atomic<bool> done(false);
  atomic<int64_t> total_count(0);

  // We start many threads, each of which performs 10:1 ratio of
  // new/delete pairs to consumption lookups. The high number
  // of threads highlights when there is contention on central
  // tcmalloc locks.
  for (int i = 0; i < kNumThreads; i++) {
    threads.emplace_back([&]() {
        int64_t local_count = 0;
        while (!done) {
          for (int a = 0; a < 10; a++) {
            // Mark 'x' volatile so that the compiler does not optimize out the
            // allocation.
            char* volatile x = new char[8000];
            delete[] x;
          }
          process_memory::CurrentConsumption();
          local_count++;
        }
        total_count += local_count;
      });
  }
  double secs = 3;
  SleepFor(MonoDelta::FromSeconds(secs));
  done = true;

  for (auto& t : threads) {
    t.join();
  }

  LOG(INFO) << "Performed " << total_count / secs << " iters/sec";
}

} // namespace kudu
