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
#include "kudu/tserver/tablet_server-test-base.h"

#include <thread>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/stopwatch.h"

DEFINE_int32(runtime_secs, 10,
             "Maximum number of seconds to run. If the threads have not completed "
             "inserting by this time, they will stop regardless. Set to 0 to disable "
             "the timeout.");
DEFINE_int32(num_inserter_threads, 8, "Number of inserter threads to run");
DEFINE_int32(num_inserts_per_thread, 100000000,
             "Number of inserts from each thread. If 'runtime_secs' is non-zero, threads will "
             "exit after that time out even if they have not inserted the desired number. The "
             "default is set high so that, typically, the 'runtime_secs' parameter determines "
             "how long this test will run.");
DECLARE_bool(enable_maintenance_manager);

METRIC_DEFINE_histogram(test, insert_latency,
                        "Insert Latency",
                        kudu::MetricUnit::kMicroseconds,
                        "TabletServer single threaded insert latency.",
                        10000000,
                        2);

namespace kudu {
namespace tserver {

class TSStressTest : public TabletServerTestBase {
 public:
  TSStressTest()
      : start_latch_(FLAGS_num_inserter_threads),
        stop_latch_(1) {

    OverrideFlagForSlowTests("runtime_secs", "60");

    // Re-enable the maintenance manager which is disabled by default
    // in TS tests. We want to stress the whole system including
    // flushes, etc.
    FLAGS_enable_maintenance_manager = true;
  }

  virtual void SetUp() OVERRIDE {
    TabletServerTestBase::SetUp();
    NO_FATALS(StartTabletServer());

    histogram_ = METRIC_insert_latency.Instantiate(ts_test_metric_entity_);
  }

  void StartThreads() {
    for (int i = 0; i < FLAGS_num_inserter_threads; i++) {
      scoped_refptr<kudu::Thread> new_thread;
      CHECK_OK(kudu::Thread::Create("test", strings::Substitute("test$0", i),
                                    &TSStressTest::InserterThread, this, i, &new_thread));
      threads_.push_back(new_thread);
    }
  }

  void JoinThreads() {
    for (scoped_refptr<kudu::Thread> thr : threads_) {
     CHECK_OK(ThreadJoiner(thr.get()).Join());
    }
  }

  void InserterThread(int thread_idx);

 protected:
  scoped_refptr<Histogram> histogram_;
  CountDownLatch start_latch_;
  CountDownLatch stop_latch_;
  std::vector<scoped_refptr<kudu::Thread> > threads_;
};

void TSStressTest::InserterThread(int thread_idx) {
  // Wait for all the threads to be ready before we start.
  start_latch_.CountDown();
  start_latch_.Wait();
  LOG(INFO) << "Starting inserter thread " << thread_idx << " complete";

  uint64_t max_rows = FLAGS_num_inserts_per_thread;
  int start_row = thread_idx * max_rows;
  for (int i = start_row; i < start_row + max_rows && stop_latch_.count() > 0; i++) {
    MonoTime before = MonoTime::Now();
    InsertTestRowsRemote(thread_idx, i, 1);
    MonoTime after = MonoTime::Now();
    MonoDelta delta = after - before;
    histogram_->Increment(delta.ToMicroseconds());
  }
  LOG(INFO) << "Inserter thread " << thread_idx << " complete";
}

TEST_F(TSStressTest, TestMTInserts) {
  std::thread timeout_thread;
  StartThreads();
  Stopwatch s(Stopwatch::ALL_THREADS);
  s.start();

  // Start a thread to fire 'stop_latch_' after the prescribed number of seconds.
  if (FLAGS_runtime_secs > 0) {
    timeout_thread = std::thread([&]() {
      stop_latch_.WaitFor(MonoDelta::FromSeconds(FLAGS_runtime_secs));
      stop_latch_.CountDown();
    });
  }
  JoinThreads();
  s.stop();

  int num_rows = histogram_->TotalCount();
  LOG(INFO) << "Inserted " << num_rows << " rows in " << s.elapsed().wall_millis() << " ms";
  LOG(INFO) << "Throughput: " << (num_rows * 1000 / s.elapsed().wall_millis()) << " rows/sec";
  LOG(INFO) << "CPU efficiency: " << (num_rows / s.elapsed().user_cpu_seconds()) << " rows/cpusec";

  // Generate the JSON.
  std::stringstream out;
  JsonWriter writer(&out, JsonWriter::PRETTY);
  ASSERT_OK(histogram_->WriteAsJson(&writer, MetricJsonOptions()));

  LOG(INFO) << out.str();

  // Ensure the timeout thread is stopped before exiting.
  stop_latch_.CountDown();
  if (timeout_thread.joinable()) timeout_thread.join();
}

} // namespace tserver
} // namespace kudu
