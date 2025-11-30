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

#include "kudu/util/maintenance_manager.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/locks.h"
#include "kudu/util/maintenance_manager.pb.h"
#include "kudu/util/maintenance_manager_metrics.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

using std::list;
using std::shared_ptr;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

METRIC_DEFINE_entity(test);
METRIC_DEFINE_gauge_uint32(test, maintenance_ops_running,
                           "Number of Maintenance Operations Running",
                           kudu::MetricUnit::kMaintenanceOperations,
                           "The number of background maintenance operations currently running.",
                           kudu::MetricLevel::kInfo);
METRIC_DEFINE_histogram(test, maintenance_op_duration,
                        "Maintenance Operation Duration",
                        kudu::MetricUnit::kSeconds, "",
                        kudu::MetricLevel::kInfo,
                        60000000LU, 2);

DECLARE_bool(enable_maintenance_manager);
DECLARE_int64(log_target_replay_size_mb);
DECLARE_double(maintenance_op_multiplier);
DECLARE_int32(max_priority_range);
DECLARE_double(run_non_memory_ops_prob);
DECLARE_double(data_gc_prioritization_prob);
DECLARE_int32(memory_pressure_percentage);
DECLARE_int32(memory_limit_soft_percentage);
namespace kudu {

// Set this a bit bigger so that the manager could keep track of all possible completed ops.
static const int kHistorySize = 10;
static const char kFakeUuid[] = "12345";

class TestMaintenanceOp : public MaintenanceOp {
 public:
  TestMaintenanceOp(const std::string& name,
                    IOUsage io_usage,
                    int32_t priority = 0,
                    CountDownLatch* start_stats_latch = nullptr,
                    CountDownLatch* continue_stats_latch = nullptr)
    : MaintenanceOp(name, io_usage),
      start_stats_latch_(start_stats_latch),
      continue_stats_latch_(continue_stats_latch),
      ram_anchored_(500),
      data_retained_bytes_(0),
      logs_retained_bytes_(0),
      perf_improvement_(0),
      metric_entity_(METRIC_ENTITY_test.Instantiate(&metric_registry_, "test")),
      maintenance_op_duration_(METRIC_maintenance_op_duration.Instantiate(metric_entity_)),
      maintenance_ops_running_(METRIC_maintenance_ops_running.Instantiate(metric_entity_, 0)),
      remaining_runs_(1),
      prepared_runs_(0),
      sleep_time_(MonoDelta::FromSeconds(0)),
      update_stats_time_(MonoDelta::FromSeconds(0)),
      priority_(priority),
      workload_score_(0),
      update_stats_count_(0),
      update_time_(MonoTime::Now()),
      queue_time_(MonoDelta::FromSeconds(0)),
      run_count_(0),
      updated_(false),
      register_self_(false) {
  }

  ~TestMaintenanceOp() override = default;

  bool Prepare() override {
    queue_time_ += (MonoTime::Now() - update_time_);
    std::lock_guard guard(lock_);
    if (remaining_runs_ == 0) {
      return false;
    }
    remaining_runs_--;
    prepared_runs_++;
    DLOG(INFO) << "Prepared op " << name();
    return true;
  }

  void Perform() override {
    {
      std::lock_guard guard(lock_);
      DLOG(INFO) << "Performing op " << name();

      // Ensure that we don't call Perform() more times than we returned
      // success from Prepare().
      CHECK_GE(prepared_runs_, 1);
      prepared_runs_--;
    }
    SleepFor(sleep_time_);
    {
      std::lock_guard guard(lock_);
      run_count_++;
      updated_ = false;
      completed_at_ = MonoTime::Now();
    }
    if (register_self_) {
      scoped_refptr<kudu::Thread> thread;
      // Re-register itself after 50ms.
      CHECK_OK(kudu::Thread::Create("maintenance-test",
                                    "self-register",
                                    [this]() {
                                      this->set_remaining_runs(1);
                                    }, &thread));
    }
  }

  void UpdateStats(MaintenanceOpStats* stats) override {
    if (start_stats_latch_) {
      start_stats_latch_->CountDown();
      DCHECK_NOTNULL(continue_stats_latch_)->Wait();
    }

    if (update_stats_time_.ToNanoseconds() > 0) {
      const auto run_until = MonoTime::Now() + update_stats_time_;
      volatile size_t cnt = 0;
      while (MonoTime::Now() < run_until) {
        ++cnt;
      }
    }

    std::lock_guard guard(lock_);
    stats->set_runnable(remaining_runs_ > 0);
    stats->set_ram_anchored(ram_anchored_);
    stats->set_logs_retained_bytes(logs_retained_bytes_);
    stats->set_data_retained_bytes(data_retained_bytes_);
    stats->set_perf_improvement(perf_improvement_);
    stats->set_workload_score(workload_score_);
    if (remaining_runs_ > 0 && !updated_) {
      update_time_ = MonoTime::Now();
      updated_ = true;
    }
    ++update_stats_count_;
  }

  void set_remaining_runs(int runs) {
    std::lock_guard guard(lock_);
    remaining_runs_ = runs;
  }

  void set_sleep_time(MonoDelta time) {
    std::lock_guard guard(lock_);
    sleep_time_ = time;
  }

  void set_update_stats_time(MonoDelta time) {
    std::lock_guard guard(lock_);
    update_stats_time_ = time;
  }

  void set_ram_anchored(uint64_t ram_anchored) {
    std::lock_guard guard(lock_);
    ram_anchored_ = ram_anchored;
  }

  void set_logs_retained_bytes(uint64_t logs_retained_bytes) {
    std::lock_guard guard(lock_);
    logs_retained_bytes_ = logs_retained_bytes;
  }

  void set_data_retained_bytes(uint64_t data_retained_bytes) {
    std::lock_guard guard(lock_);
    data_retained_bytes_ = data_retained_bytes;
  }

  void set_perf_improvement(uint64_t perf_improvement) {
    std::lock_guard guard(lock_);
    perf_improvement_ = perf_improvement;
  }

  void set_workload_score(uint64_t workload_score) {
    std::lock_guard guard(lock_);
    workload_score_ = workload_score;
  }

  void set_register_self(bool register_self) {
    std::lock_guard guard(lock_);
    register_self_ = register_self;
  }

  scoped_refptr<Histogram> DurationHistogram() const override {
    return maintenance_op_duration_;
  }

  scoped_refptr<AtomicGauge<uint32_t> > RunningGauge() const override {
    return maintenance_ops_running_;
  }

  int32_t priority() const override {
    return priority_;
  }

  int remaining_runs() const {
    std::lock_guard guard(lock_);
    return remaining_runs_;
  }

  uint64_t update_stats_count() const {
    std::lock_guard guard(lock_);
    return update_stats_count_;
  }

  MonoTime completed_at() const {
    std::lock_guard guard(lock_);
    return completed_at_;
  }

  int64_t run_count() const {
    return run_count_;
  }

  MonoDelta queue_time() const {
    return queue_time_;
  }

 private:
  mutable simple_spinlock lock_;

  // Latch used to help other threads wait for us to begin updating stats for
  // this op. Another thread may wait for this latch, and once the countdown is
  // complete, the maintenance manager lock will be locked while computing
  // stats, at which point the scheduler thread will wait for
  // 'continue_stats_latch_' to be counted down.
  CountDownLatch* start_stats_latch_;
  CountDownLatch* continue_stats_latch_;

  uint64_t ram_anchored_;
  uint64_t data_retained_bytes_;
  uint64_t logs_retained_bytes_;
  uint64_t perf_improvement_;
  MetricRegistry metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
  scoped_refptr<Histogram> maintenance_op_duration_;
  scoped_refptr<AtomicGauge<uint32_t>> maintenance_ops_running_;

  // The number of remaining times this operation will run before disabling
  // itself.
  int remaining_runs_;
  // The number of Prepared() operations which have not yet been Perform()ed.
  int prepared_runs_;

  // The amount of time each op invocation will sleep.
  MonoDelta sleep_time_;

  // The amount of time each UpdateStats will sleep.
  MonoDelta update_stats_time_;

  // Maintenance priority.
  int32_t priority_;

  double workload_score_;

  // Number of times the 'UpdateStats()' method is called on this instance.
  uint64_t update_stats_count_;

  // Timestamp of the monotonous clock when the operation was completed.
  MonoTime completed_at_;

  // Timestamp of updating status in FindBestOp.
  MonoTime update_time_;
  // Sum of scheduled time, which indicates how long this op has been
  // scheduled after it could be run.
  MonoDelta queue_time_;
  // How many times the operation has been run.
  int64_t run_count_;
  // If the operation had been updated but has not been performed.
  bool updated_;
  // Whether re-register itself after performing.
  bool register_self_;
};

class MaintenanceManagerTest : public KuduTest {
 public:
  MaintenanceManagerTest()
      : metric_entity_(METRIC_ENTITY_server.Instantiate(
                       &metric_registry_, "test_entity")) {
  }

  void SetUp() override {
    StartManager(2);
  }

  void TearDown() override {
    StopManager();
  }

  void StartManager(int32_t num_threads) {
    MaintenanceManager::Options options;
    options.num_threads = num_threads;
    options.polling_interval_ms = 1;
    options.history_size = kHistorySize;
    manager_.reset(new MaintenanceManager(options, kFakeUuid, metric_entity_));
    // Same logic with MaintenanceManager#ProceedWithFlush, but the memory usage is
    // simulated.
    manager_->set_memory_pressure_func_for_tests(
        [&](double* /* consumption */) {
          static const double pressure_threshold = FLAGS_memory_pressure_percentage;
          static const double soft_limit = FLAGS_memory_limit_soft_percentage;
          static const double pressure_diff = soft_limit - pressure_threshold;
          if (memory_pressure_pct_.load() < pressure_threshold) {
            return false;
          }
          const double used_diff = soft_limit - memory_pressure_pct_.load();
          return pressure_diff <= 0 || used_diff <= 0 ||
              Random(GetRandomSeed32()).NextDoubleFraction() * pressure_diff >=
                  FLAGS_run_non_memory_ops_prob * used_diff;
        });
    ASSERT_OK(manager_->Start());
  }

  void StopManager() {
    manager_->Shutdown();
  }

  void WaitForSchedulerThreadRunning(const string& op_name) {
    // Register an op whose sole purpose is to make sure the MM scheduler
    // thread is running.
    TestMaintenanceOp canary_op(op_name, MaintenanceOp::HIGH_IO_USAGE, 0);
    canary_op.set_perf_improvement(1);
    manager_->RegisterOp(&canary_op);
    // Unregister the 'canary_op' operation if it goes out of scope to avoid
    // de-referencing invalid pointers.
    SCOPED_CLEANUP({
      manager_->UnregisterOp(&canary_op);
    });
    ASSERT_EVENTUALLY([&]() {
      ASSERT_EQ(0, canary_op.remaining_runs());
    });
  }

 protected:
  MetricRegistry metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;

  shared_ptr<MaintenanceManager> manager_;
  std::atomic<double> memory_pressure_pct_ { 0.0 };
};

// Just create the MaintenanceManager and then shut it down, to make sure
// there are no race conditions there.
TEST_F(MaintenanceManagerTest, TestCreateAndShutdown) {
}

// Create an op and wait for it to start running.  Unregister it while it is
// running and verify that UnregisterOp waits for it to finish before
// proceeding.
TEST_F(MaintenanceManagerTest, TestRegisterUnregister) {
  TestMaintenanceOp op1("1", MaintenanceOp::HIGH_IO_USAGE);
  op1.set_perf_improvement(10);
  // Register initially with no remaining runs. We'll later enable it once it's
  // already registered.
  op1.set_remaining_runs(0);
  manager_->RegisterOp(&op1);
  thread thread([&op1]() { op1.set_remaining_runs(1); });
  SCOPED_CLEANUP({ thread.join(); });
  ASSERT_EVENTUALLY([&]() {
    ASSERT_EQ(op1.DurationHistogram()->TotalCount(), 1);
  });
  manager_->UnregisterOp(&op1);
}

TEST_F(MaintenanceManagerTest, TestRegisterUnregisterWithContention) {
  CountDownLatch start_latch(1);
  CountDownLatch continue_latch(1);
  TestMaintenanceOp op1("1", MaintenanceOp::HIGH_IO_USAGE, 0, &start_latch, &continue_latch);
  manager_->RegisterOp(&op1);
  // Wait for the maintenance manager to start updating stats for this op.
  // This will effectively block the maintenance manager lock until
  // 'continue_latch' counts down.
  start_latch.Wait();

  // Register another op while the maintenance manager lock is held.
  TestMaintenanceOp op2("2", MaintenanceOp::HIGH_IO_USAGE);
  manager_->RegisterOp(&op2);
  TestMaintenanceOp op3("3", MaintenanceOp::HIGH_IO_USAGE);
  manager_->RegisterOp(&op3);

  // Allow UpdateStats() to complete and release the maintenance manager lock.
  continue_latch.CountDown();

  // We should be able to unregister an op even though, because of the forced
  // lock contention, it may have been added to the list of ops pending
  // registration instead of "fully registered" ops.
  manager_->UnregisterOp(&op3);

  // Even though we blocked registration, when we dump, we'll take the
  // maintenance manager and merge ops that were pending registration.
  MaintenanceManagerStatusPB status_pb;
  manager_->GetMaintenanceManagerStatusDump(&status_pb);
  ASSERT_EQ(2, status_pb.registered_operations_size());
  manager_->UnregisterOp(&op1);
  manager_->UnregisterOp(&op2);
}

// Regression test for KUDU-1495: when an operation is being unregistered,
// new instances of that operation should not be scheduled.
TEST_F(MaintenanceManagerTest, TestNewOpsDontGetScheduledDuringUnregister) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  // Set the op to run up to 10 times, and each time it sleeps for some time.
  // The sleep time is set quite high to avoid flakiness due to scheduler
  // anomalies when running this scenario on busy machines, especially if the
  // binaries are instrumented by ASAN/TSAN. When all the avaiable maintenance
  // worker threads are busy with running their first iteration of 'op1',
  // if the main thread is scheduled off the CPU for a long time before
  // unregistering the operation, the sleep time should provide enough margin
  // for the main thread becoming active again and unregistering 'op1'.
  TestMaintenanceOp op1("1", MaintenanceOp::HIGH_IO_USAGE);
  op1.set_perf_improvement(10);
  op1.set_remaining_runs(10);
  op1.set_sleep_time(MonoDelta::FromSeconds(5));
  manager_->RegisterOp(&op1);

  // Wait until two instances of the ops start running, since we have two
  // maintenance worker threads.
  ASSERT_EVENTUALLY([&]() {
    ASSERT_EQ(op1.RunningGauge()->value(), 2);
  });

  // Trigger Unregister while they are running. This should wait for the currently-
  // running operations to complete, but no new operations should be scheduled.
  manager_->UnregisterOp(&op1);

  // Hence, we should have run only the original two that we saw above.
  ASSERT_LE(op1.DurationHistogram()->TotalCount(), 2);
}

// Test that we'll run an operation that doesn't improve performance when memory
// pressure gets high.
TEST_F(MaintenanceManagerTest, TestMemoryPressurePrioritizesMemory) {
  TestMaintenanceOp op("op", MaintenanceOp::HIGH_IO_USAGE);
  op.set_ram_anchored(100);
  manager_->RegisterOp(&op);

  // At first, we don't want to run this, since there is no perf_improvement.
  SleepFor(MonoDelta::FromMilliseconds(20));
  ASSERT_EQ(0, op.DurationHistogram()->TotalCount());

  // Fake that the server is under memory pressure.
  memory_pressure_pct_ = 70;

  ASSERT_EVENTUALLY([&]() {
      ASSERT_EQ(op.DurationHistogram()->TotalCount(), 1);
    });
  manager_->UnregisterOp(&op);
}

// Test that when under memory pressure, we'll run an op that doesn't improve
// memory pressure if there's nothing else to do.
TEST_F(MaintenanceManagerTest, TestMemoryPressurePerformsNoMemoryOp) {
  TestMaintenanceOp op("op", MaintenanceOp::HIGH_IO_USAGE);
  op.set_ram_anchored(0);
  manager_->RegisterOp(&op);

  // At first, we don't want to run this, since there is no perf_improvement.
  SleepFor(MonoDelta::FromMilliseconds(20));
  ASSERT_EQ(0, op.DurationHistogram()->TotalCount());

  // Now fake that the server is under memory pressure and make our op runnable
  // by giving it a perf score.
  memory_pressure_pct_ = 70;
  op.set_perf_improvement(1);

  // Even though we're under memory pressure, and even though our op doesn't
  // have any ram anchored, there's nothing else to do, so we run our op.
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(op.DurationHistogram()->TotalCount(), 1);
  });
  manager_->UnregisterOp(&op);
}

// Test that ops are prioritized correctly when we add log retention.
TEST_F(MaintenanceManagerTest, TestLogRetentionPrioritization) {
  const int64_t kMB = 1024 * 1024;

  StopManager();

  TestMaintenanceOp op1("op1", MaintenanceOp::LOW_IO_USAGE);
  op1.set_ram_anchored(0);
  op1.set_logs_retained_bytes(100 * kMB);

  TestMaintenanceOp op2("op2", MaintenanceOp::HIGH_IO_USAGE);
  op2.set_ram_anchored(100);
  op2.set_logs_retained_bytes(100 * kMB);

  TestMaintenanceOp op3("op3", MaintenanceOp::HIGH_IO_USAGE);
  op3.set_ram_anchored(200);
  op3.set_logs_retained_bytes(100 * kMB);

  manager_->RegisterOp(&op1);
  manager_->RegisterOp(&op2);
  manager_->RegisterOp(&op3);

  // We want to do the low IO op first since it clears up some log retention.
  auto op_and_why = manager_->FindBestOp();
  ASSERT_EQ(&op1, op_and_why.first);
  EXPECT_EQ(op_and_why.second, "free 104857600 bytes of WAL");

  manager_->UnregisterOp(&op1);

  // Low IO is taken care of, now we find the op that clears the most log retention and ram.
  // However, with the default settings, we won't bother running any of these operations
  // which only retain 100MB of logs.
  op_and_why = manager_->FindBestOp();
  ASSERT_EQ(nullptr, op_and_why.first);
  EXPECT_EQ(op_and_why.second, "no ops with positive improvement");

  // If we change the target WAL size, we will select these ops.
  FLAGS_log_target_replay_size_mb = 50;
  op_and_why = manager_->FindBestOp();
  ASSERT_EQ(&op3, op_and_why.first);
  EXPECT_EQ(op_and_why.second, "104857600 bytes log retention, and flush 200 bytes memory");

  manager_->UnregisterOp(&op3);

  op_and_why = manager_->FindBestOp();
  ASSERT_EQ(&op2, op_and_why.first);
  EXPECT_EQ(op_and_why.second, "104857600 bytes log retention, and flush 100 bytes memory");

  manager_->UnregisterOp(&op2);
}

// Test that ops are prioritized correctly when under memory pressure.
TEST_F(MaintenanceManagerTest, TestPrioritizeLogRetentionUnderMemoryPressure) {
  StopManager();

  // We should perform these in the order of WAL bytes retained, followed by
  // amount of memory anchored.
  TestMaintenanceOp op1("op1", MaintenanceOp::HIGH_IO_USAGE);
  op1.set_logs_retained_bytes(100);
  op1.set_ram_anchored(100);

  TestMaintenanceOp op2("op2", MaintenanceOp::HIGH_IO_USAGE);
  op2.set_logs_retained_bytes(100);
  op2.set_ram_anchored(99);

  TestMaintenanceOp op3("op3", MaintenanceOp::HIGH_IO_USAGE);
  op3.set_logs_retained_bytes(99);
  op3.set_ram_anchored(101);

  memory_pressure_pct_ = 70;
  manager_->RegisterOp(&op1);
  manager_->RegisterOp(&op2);
  manager_->RegisterOp(&op3);

  auto op_and_why = manager_->FindBestOp();
  ASSERT_EQ(&op1, op_and_why.first);
  EXPECT_STR_CONTAINS(
      op_and_why.second, "100 bytes log retention, and flush 100 bytes memory");
  manager_->UnregisterOp(&op1);

  op_and_why = manager_->FindBestOp();
  ASSERT_EQ(&op2, op_and_why.first);
  EXPECT_STR_CONTAINS(
      op_and_why.second, "100 bytes log retention, and flush 99 bytes memory");
  manager_->UnregisterOp(&op2);

  op_and_why = manager_->FindBestOp();
  ASSERT_EQ(&op3, op_and_why.first);
  EXPECT_STR_CONTAINS(
      op_and_why.second, "99 bytes log retention, and flush 101 bytes memory");
  manager_->UnregisterOp(&op3);
}

// Test retrieving a list of an op's running instances
TEST_F(MaintenanceManagerTest, RunningInstances) {
  TestMaintenanceOp op("op", MaintenanceOp::HIGH_IO_USAGE);
  op.set_perf_improvement(10);
  op.set_remaining_runs(2);
  op.set_sleep_time(MonoDelta::FromSeconds(1));

  manager_->RegisterOp(&op);
  auto unregistrant = MakeScopedCleanup([&] {
    manager_->UnregisterOp(&op);
  });

  // Check that running instances are added to the maintenance manager's collection,
  // and fields are populated.
  MaintenanceManagerStatusPB status_pb;
  std::array<const MaintenanceManagerStatusPB_OpInstancePB*, 2> instances{ nullptr, nullptr };
  ASSERT_EVENTUALLY([&]() {
    // Clear 'status_pb' if ASSERT_EVENTUALLY() retries:
    // MaintenanceManager::GetMaintenanceManagerStatusDump() doesn't clear
    // its output parameter, adding up more data instead.
    status_pb.Clear();
    manager_->GetMaintenanceManagerStatusDump(&status_pb);
    const auto num_running = status_pb.running_operations_size();
    ASSERT_GE(num_running, 0);
    const auto num_completed = status_pb.completed_operations_size();
    ASSERT_GE(num_completed, 0);

    // Due to scheduler anomalies and other uncontrollable factors, the main
    // thread might be put off CPU for some time, so one or even two operations
    // may be completed already.
    ASSERT_EQ(2, num_running + num_completed);
    if (num_running == 2) {
      instances[0] = &status_pb.running_operations(0);
      instances[1] = &status_pb.running_operations(1);
    } else if (num_completed == 2) {
      instances[0] = &status_pb.completed_operations(0);
      instances[1] = &status_pb.completed_operations(1);
    } else {
      instances[0] = &status_pb.running_operations(0);
      instances[1] = &status_pb.completed_operations(0);
    }
  });
  ASSERT_NE(nullptr, instances[0]);
  ASSERT_NE(nullptr, instances[1]);
  ASSERT_EQ(op.name(), instances[0]->name());
  ASSERT_EQ(op.name(), instances[1]->name());
  ASSERT_NE(instances[0]->thread_id(), instances[1]->thread_id());

  // When unregistering operations, the maintenance manager waits for them
  // to complete.
  unregistrant.run();

  // Check that running instances aren't present in the collection
  // when the ops are no longer running.
  {
    MaintenanceManagerStatusPB status_pb;
    manager_->GetMaintenanceManagerStatusDump(&status_pb);
    ASSERT_EQ(0, status_pb.running_operations_size());
  }

  // The information on the completed tasks appears after waiting for each
  // of them to wrap up and waking up the scheduler. So, there might be a race
  // between this test thread which calls GetMaintenanceManagerStatusDump()
  // and the MM worker threads that update the container with the information
  // on the completed tasks. The ASSERT_EVENTUALLY() macro below helps
  // to avoid flakiness in case of scheduler anomalies or when running this
  // test scenario on a busy machine.
  ASSERT_EVENTUALLY([&]() {
    MaintenanceManagerStatusPB status_pb;
    manager_->GetMaintenanceManagerStatusDump(&status_pb);
    ASSERT_EQ(2, status_pb.completed_operations_size());
    ASSERT_NE(status_pb.completed_operations(0).thread_id(),
              status_pb.completed_operations(1).thread_id());
  });
}

// Test adding operations and make sure that the history of recently completed
// operations is correct in that it wraps around and doesn't grow.
TEST_F(MaintenanceManagerTest, TestCompletedOpsHistory) {
  for (int i = 0; i < kHistorySize + 1; i++) {
    const auto name = Substitute("op$0", i);
    TestMaintenanceOp op(name, MaintenanceOp::HIGH_IO_USAGE);
    op.set_perf_improvement(1);
    op.set_ram_anchored(100);
    manager_->RegisterOp(&op);

    ASSERT_EVENTUALLY([&]() {
      ASSERT_EQ(op.DurationHistogram()->TotalCount(), 1);
    });
    manager_->UnregisterOp(&op);

    // There might be a race between updating the internal container with the
    // operations completed so far and serving the request to the embedded
    // web server: the latter might acquire the lock guarding the container
    // first and output the information before the operation marked 'complete'.
    // ASSERT_EVENTUALLY() addresses the transient condition.
    ASSERT_EVENTUALLY([&]() {
      MaintenanceManagerStatusPB status_pb;
      manager_->GetMaintenanceManagerStatusDump(&status_pb);
      // The size should equal to the current completed OP size,
      // and should be at most the kHistorySize.
      ASSERT_EQ(std::min(kHistorySize, i + 1),
                status_pb.completed_operations_size());
      // The most recently completed op should always be first, even if we wrap
      // around.
      ASSERT_EQ(name, status_pb.completed_operations(0).name());
    });
  }
}

// Test maintenance OP factors.
// The OPs on different priority levels have different OP score multipliers.
TEST_F(MaintenanceManagerTest, TestOpFactors) {
  StopManager();

  ASSERT_GE(FLAGS_max_priority_range, 1);
  TestMaintenanceOp op1("op1", MaintenanceOp::HIGH_IO_USAGE, -FLAGS_max_priority_range - 1);
  TestMaintenanceOp op2("op2", MaintenanceOp::HIGH_IO_USAGE, -1);
  TestMaintenanceOp op3("op3", MaintenanceOp::HIGH_IO_USAGE, 0);
  TestMaintenanceOp op4("op4", MaintenanceOp::HIGH_IO_USAGE, 1);
  TestMaintenanceOp op5("op5", MaintenanceOp::HIGH_IO_USAGE, FLAGS_max_priority_range + 1);

  ASSERT_DOUBLE_EQ(pow(FLAGS_maintenance_op_multiplier, -FLAGS_max_priority_range),
                   manager_->AdjustedPerfScore(1, 0, op1.priority()));
  ASSERT_DOUBLE_EQ(pow(FLAGS_maintenance_op_multiplier, -1),
                   manager_->AdjustedPerfScore(1, 0, op2.priority()));
  ASSERT_DOUBLE_EQ(1, manager_->AdjustedPerfScore(1, 0, op3.priority()));
  ASSERT_DOUBLE_EQ(FLAGS_maintenance_op_multiplier,
                   manager_->AdjustedPerfScore(1, 0, op4.priority()));
  ASSERT_DOUBLE_EQ(pow(FLAGS_maintenance_op_multiplier, FLAGS_max_priority_range),
                   manager_->AdjustedPerfScore(1, 0, op5.priority()));
}

// Test priority OP launching.
TEST_F(MaintenanceManagerTest, TestPriorityOpLaunch) {
  StopManager();
  StartManager(1);

  NO_FATALS(WaitForSchedulerThreadRunning("canary"));

  // The MM scheduler thread is now running. It is now safe to use
  // FLAGS_enable_maintenance_manager to temporarily disable the MM, thus
  // allowing us to register a group of ops "atomically" and ensuring the op
  // execution order that this test wants to see.
  //
  // Without the WaitForSchedulerThreadRunning() above, there's a small chance
  // that the MM scheduler thread will not have run at all at the time of
  // FLAGS_enable_maintenance_manager = false, which would cause the thread
  // to exit entirely instead of sleeping.

  // Ops are listed here in final perf score order, which is a function of the
  // op's raw perf improvement, workload score and its priority.
  // The 'op0' would never launch because it has a raw perf improvement 0, even if
  // it has a high workload_score and a high priority.
  TestMaintenanceOp op0("op0", MaintenanceOp::HIGH_IO_USAGE, FLAGS_max_priority_range + 1);
  op0.set_perf_improvement(0);
  op0.set_workload_score(10);
  op0.set_remaining_runs(1);
  op0.set_sleep_time(MonoDelta::FromMilliseconds(1));

  TestMaintenanceOp op1("op1", MaintenanceOp::HIGH_IO_USAGE, -FLAGS_max_priority_range - 1);
  op1.set_perf_improvement(10);
  op1.set_remaining_runs(1);
  op1.set_sleep_time(MonoDelta::FromMilliseconds(1));

  TestMaintenanceOp op2("op2", MaintenanceOp::HIGH_IO_USAGE, -1);
  op2.set_perf_improvement(10);
  op2.set_remaining_runs(1);
  op2.set_sleep_time(MonoDelta::FromMilliseconds(1));

  TestMaintenanceOp op3("op3", MaintenanceOp::HIGH_IO_USAGE, 0);
  op3.set_perf_improvement(10);
  op3.set_remaining_runs(1);
  op3.set_sleep_time(MonoDelta::FromMilliseconds(1));

  TestMaintenanceOp op4("op4", MaintenanceOp::HIGH_IO_USAGE, 1);
  op4.set_perf_improvement(10);
  op4.set_remaining_runs(1);
  op4.set_sleep_time(MonoDelta::FromMilliseconds(1));

  TestMaintenanceOp op5("op5", MaintenanceOp::HIGH_IO_USAGE, 0);
  op5.set_perf_improvement(12);
  op5.set_remaining_runs(1);
  op5.set_sleep_time(MonoDelta::FromMilliseconds(1));

  TestMaintenanceOp op6("op6", MaintenanceOp::HIGH_IO_USAGE, FLAGS_max_priority_range + 1);
  op6.set_perf_improvement(10);
  op6.set_remaining_runs(1);
  op6.set_sleep_time(MonoDelta::FromMilliseconds(1));

  TestMaintenanceOp op7("op7", MaintenanceOp::HIGH_IO_USAGE, 0);
  op7.set_perf_improvement(9);
  op7.set_workload_score(10);
  op7.set_remaining_runs(1);
  op7.set_sleep_time(MonoDelta::FromMilliseconds(1));

  FLAGS_enable_maintenance_manager = false;
  manager_->RegisterOp(&op0);
  manager_->RegisterOp(&op1);
  manager_->RegisterOp(&op2);
  manager_->RegisterOp(&op3);
  manager_->RegisterOp(&op4);
  manager_->RegisterOp(&op5);
  manager_->RegisterOp(&op6);
  manager_->RegisterOp(&op7);
  FLAGS_enable_maintenance_manager = true;

  // From this point forward if an ASSERT fires, we'll hit a CHECK failure if
  // we don't unregister an op before it goes out of scope.
  SCOPED_CLEANUP({
    manager_->UnregisterOp(&op0);
    manager_->UnregisterOp(&op1);
    manager_->UnregisterOp(&op2);
    manager_->UnregisterOp(&op3);
    manager_->UnregisterOp(&op4);
    manager_->UnregisterOp(&op5);
    manager_->UnregisterOp(&op6);
    manager_->UnregisterOp(&op7);
  });

  ASSERT_EVENTUALLY([&]() {
    MaintenanceManagerStatusPB status_pb;
    manager_->GetMaintenanceManagerStatusDump(&status_pb);
    ASSERT_EQ(8, status_pb.completed_operations_size());
  });

  // Wait for instances to complete by shutting down the maintenance manager.
  // We can still call GetMaintenanceManagerStatusDump though.
  StopManager();

  // Check that running instances are removed from collection after completion.
  MaintenanceManagerStatusPB status_pb;
  manager_->GetMaintenanceManagerStatusDump(&status_pb);
  ASSERT_EQ(0, status_pb.running_operations_size());

  // Check that ops were executed in perf improvement order (from greatest to
  // least improvement). Note that completed ops are listed in _reverse_ execution order.
  list<string> ordered_ops({"op1",
                            "op2",
                            "op3",
                            "op4",
                            "op5",
                            "op6",
                            "op7",
                            "canary"});
  ASSERT_EQ(ordered_ops.size(), status_pb.completed_operations().size());
  for (const auto& instance : status_pb.completed_operations()) {
    ASSERT_EQ(ordered_ops.front(), instance.name());
    ordered_ops.pop_front();
  }
}

// Check for MaintenanceManager metrics.
TEST_F(MaintenanceManagerTest, VerifyMetrics) {
  // Nothing has failed so far.
  ASSERT_EQ(0, manager_->metrics_.op_prepare_failed->value());

  // The oppf's Prepare() returns 'false', meaning the operation failed to
  // prepare. However, the scores for this operation is set higher than the
  // other two to make sure the scheduler starts working on this task before
  // the other two.
  class PrepareFailedMaintenanceOp : public TestMaintenanceOp {
   public:
    PrepareFailedMaintenanceOp()
        : TestMaintenanceOp("oppf", MaintenanceOp::HIGH_IO_USAGE) {
    }

    bool Prepare() override {
      set_remaining_runs(0);
      return false;
    }
  } oppf;
  oppf.set_perf_improvement(3);
  oppf.set_workload_score(3);

  TestMaintenanceOp op0("op0", MaintenanceOp::HIGH_IO_USAGE);
  op0.set_perf_improvement(2);
  op0.set_workload_score(2);
  op0.set_update_stats_time(MonoDelta::FromMicroseconds(10000));

  TestMaintenanceOp op1("op1", MaintenanceOp::HIGH_IO_USAGE);
  op1.set_perf_improvement(1);
  op1.set_workload_score(1);

  manager_->RegisterOp(&oppf);
  manager_->RegisterOp(&op0);
  manager_->RegisterOp(&op1);
  SCOPED_CLEANUP({
    manager_->UnregisterOp(&op1);
    manager_->UnregisterOp(&op0);
    manager_->UnregisterOp(&oppf);
  });

  ASSERT_EVENTUALLY([&]() {
    MaintenanceManagerStatusPB status_pb;
    manager_->GetMaintenanceManagerStatusDump(&status_pb);
    // Only 2 operations should successfully complete so far: op0, op1.
    // Operation oppf should fail during Prepare().
    ASSERT_EQ(2, status_pb.completed_operations_size());
  });

  {
    // A sanity check: no operations should be running.
    MaintenanceManagerStatusPB status_pb;
    manager_->GetMaintenanceManagerStatusDump(&status_pb);
    ASSERT_EQ(0, status_pb.running_operations_size());

    ASSERT_EQ(1, op0.DurationHistogram()->TotalCount());
    ASSERT_EQ(1, op1.DurationHistogram()->TotalCount());
    ASSERT_EQ(0, oppf.DurationHistogram()->TotalCount());
  }

  // Exactly one operation has failed prepare: oppf.
  ASSERT_EQ(1, manager_->metrics_.op_prepare_failed->value());

  // There should be at least 3 runs of the FindBestOp() method by this time
  // becase 3 operations have been scheduled: oppf, op0, op1,
  // but it might be many more since the scheduler is still active.
  ASSERT_LE(3, manager_->metrics_.op_find_duration->TotalCount());

  // Max time taken by FindBestOp() should be at least 10 msec since that's
  // the mininum duration of op0's UpdateStats().
  ASSERT_GE(manager_->metrics_.op_find_duration->MaxValueForTests(), 10000);
}

// This test scenario verifies that maintenance manager is able to process
// operations with high enough level of concurrency, even if their UpdateStats()
// method is computationally heavy.
TEST_F(MaintenanceManagerTest, ManyOperationsHeavyUpdateStats) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  StopManager();
  StartManager(4);

  NO_FATALS(WaitForSchedulerThreadRunning("canary"));

  constexpr auto kOpsNum = 1000;
  vector<unique_ptr<TestMaintenanceOp>> ops;
  ops.reserve(kOpsNum);
  for (auto i = 0; i < kOpsNum; ++i) {
    unique_ptr<TestMaintenanceOp> op(new TestMaintenanceOp(
        std::to_string(i), MaintenanceOp::HIGH_IO_USAGE));
    op->set_perf_improvement(i + 1);
    op->set_workload_score(i + 1);
    op->set_remaining_runs(1);
    op->set_sleep_time(MonoDelta::FromMilliseconds(i % 8 + 1));
    op->set_update_stats_time(MonoDelta::FromMicroseconds(5));
    ops.emplace_back(std::move(op));
  }

  // For cleaner timings, disable processing of the registered operations,
  // and re-enable that after registering all operations.
  FLAGS_enable_maintenance_manager = false;
  for (auto& op : ops) {
    manager_->RegisterOp(op.get());
  }
  FLAGS_enable_maintenance_manager = true;
  MonoTime time_started = MonoTime::Now();

  SCOPED_CLEANUP({
    for (auto& op : ops) {
      manager_->UnregisterOp(op.get());
    }
  });

  // Given the performance improvement scores and workload scores assigned
  // to the maintenance operations, the operation scheduled first should
  // be processed last. Once it's done, no other operations should be running.
  AssertEventually([&] {
    ASSERT_EQ(1, ops.front()->DurationHistogram()->TotalCount());
  }, MonoDelta::FromSeconds(60));

  // A sanity check: verify no operations are left running, but all are still
  // registered.
  {
    MaintenanceManagerStatusPB status_pb;
    manager_->GetMaintenanceManagerStatusDump(&status_pb);
    ASSERT_EQ(0, status_pb.running_operations_size());
    ASSERT_EQ(kOpsNum, status_pb.registered_operations_size());
  }

  MonoTime time_completed = time_started;
  for (const auto& op : ops) {
    const auto op_time_completed = op->completed_at();
    if (op_time_completed > time_completed) {
      time_completed = op_time_completed;
    }
  }
  const auto time_spent = time_completed - time_started;
  LOG(INFO) << Substitute("spent $0 milliseconds to process $1 operations",
                          time_spent.ToMilliseconds(), kOpsNum);
  LOG(INFO) << Substitute("number of UpdateStats() calls per operation: $0",
                          ops.front()->update_stats_count());
}

// Regression test for KUDU-3268, where the unregistering and destruction of an
// op may race with the scheduling of that op, resulting in a segfault.
TEST_F(MaintenanceManagerTest, TestUnregisterWhileScheduling) {
  TestMaintenanceOp op1("1", MaintenanceOp::HIGH_IO_USAGE);
  op1.set_perf_improvement(10);
  // Set a bunch of runs so we continually schedule the op.
  op1.set_remaining_runs(1000000);
  manager_->RegisterOp(&op1);
  ASSERT_EVENTUALLY([&]() {
    ASSERT_GE(op1.DurationHistogram()->TotalCount(), 1);
  });
  op1.Unregister();
}

// Show which operation the @MaintenanceManager will pick in various workloads and
// policies. This test would not assert anything since it tests the probability flags.
TEST_F(MaintenanceManagerTest, ComprehensiveTest) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  // Select policies here.
  memory_pressure_pct_ = 60;
  FLAGS_run_non_memory_ops_prob = 0.2;
  FLAGS_data_gc_prioritization_prob = 0.5;

  StopManager();

  TestMaintenanceOp op1("perf_op", MaintenanceOp::HIGH_IO_USAGE);
  op1.set_perf_improvement(10);
  op1.set_remaining_runs(1);
  op1.set_sleep_time(MonoDelta::FromMilliseconds(5));
  op1.set_register_self(true);

  TestMaintenanceOp op2("memory_op", MaintenanceOp::HIGH_IO_USAGE);
  op2.set_ram_anchored(2000);
  op2.set_remaining_runs(1);
  op2.set_sleep_time(MonoDelta::FromMilliseconds(5));
  op2.set_register_self(true);

  TestMaintenanceOp op3("data_gc_op", MaintenanceOp::HIGH_IO_USAGE);
  op3.set_data_retained_bytes(1024 * 1024);
  op3.set_remaining_runs(1);
  op3.set_sleep_time(MonoDelta::FromMilliseconds(5));
  op3.set_register_self(true);

  // Set maintenance manager num to 1 to get a clean vision of scheduling.
  NO_FATALS(StartManager(1));
  FLAGS_enable_maintenance_manager = false;
  manager_->RegisterOp(&op1);
  manager_->RegisterOp(&op2);
  manager_->RegisterOp(&op3);
  FLAGS_enable_maintenance_manager = true;
  // Wait for the memory_op to run over 1000 times and then check the running times
  // of other operations.
  AssertEventually([&]() {
    ASSERT_LE(1000, op2.DurationHistogram()->TotalCount());
  }, MonoDelta::FromSeconds(60));
  op1.set_register_self(false);
  op2.set_register_self(false);
  op3.set_register_self(false);
  // Wait until all the operations are done.
  SleepFor(MonoDelta::FromMilliseconds(100));

  manager_->UnregisterOp(&op1);
  manager_->UnregisterOp(&op2);
  manager_->UnregisterOp(&op3);

  // The expected running times of operations should be like:
  //    other_ops_running_times = memory_op_running_times * probability_not_to_flush
  // Since the memory usage is 60%, probability_not_to_flush is exactly
  // FLAGS_run_non_memory_ops_prob. And also, the applying and registering might
  // take time, so the other_ops_running_times might be greater than expected.
  // Add some extra margin (about x3) for probability_not_to_flush
  // to avoid flakiness.
  const int64_t memory_op_running_times = op2.run_count();
  const int64_t other_ops_running_times = op1.run_count() + op3.run_count();
  ASSERT_LT(memory_op_running_times * 0.1, other_ops_running_times);
  ASSERT_GT(memory_op_running_times * 0.6, other_ops_running_times);

  LOG(INFO) << Substitute("op1: $0 perform count: $1 average schedule time: $2",
                          op1.name(), op1.run_count(), op1.queue_time().ToMilliseconds()
                          / op1.run_count());
  LOG(INFO) << Substitute("op2: $0 perform count: $1 average schedule time: $2",
                          op2.name(), op2.run_count(), op2.queue_time().ToMilliseconds()
                          / op2.run_count());
  LOG(INFO) << Substitute("op3: $0 perform count: $1 average schedule time: $2",
                          op3.name(), op3.run_count(), op3.queue_time().ToMilliseconds()
                          / op3.run_count());
}
} // namespace kudu
