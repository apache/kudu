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

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <memory>
#include <mutex>
#include <vector>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/util/maintenance_manager.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

using std::shared_ptr;
using std::vector;
using strings::Substitute;

METRIC_DEFINE_entity(test);
METRIC_DEFINE_gauge_uint32(test, maintenance_ops_running,
                           "Number of Maintenance Operations Running",
                           kudu::MetricUnit::kMaintenanceOperations,
                           "The number of background maintenance operations currently running.");
METRIC_DEFINE_histogram(test, maintenance_op_duration,
                        "Maintenance Operation Duration",
                        kudu::MetricUnit::kSeconds, "", 60000000LU, 2);

namespace kudu {

const int kHistorySize = 4;

class MaintenanceManagerTest : public KuduTest {
 public:
  MaintenanceManagerTest() {
    test_tracker_ = MemTracker::CreateTracker(1000, "test");
    MaintenanceManager::Options options;
    options.num_threads = 2;
    options.polling_interval_ms = 1;
    options.history_size = kHistorySize;
    options.parent_mem_tracker = test_tracker_;
    manager_.reset(new MaintenanceManager(options));
    manager_->Init();
  }
  ~MaintenanceManagerTest() {
    manager_->Shutdown();
  }

 protected:
  shared_ptr<MemTracker> test_tracker_;
  shared_ptr<MaintenanceManager> manager_;
};

// Just create the MaintenanceManager and then shut it down, to make sure
// there are no race conditions there.
TEST_F(MaintenanceManagerTest, TestCreateAndShutdown) {
}

class TestMaintenanceOp : public MaintenanceOp {
 public:
  TestMaintenanceOp(const std::string& name,
                    IOUsage io_usage,
                    const shared_ptr<MemTracker>& tracker)
    : MaintenanceOp(name, io_usage),
      consumption_(tracker, 500),
      logs_retained_bytes_(0),
      perf_improvement_(0),
      metric_entity_(METRIC_ENTITY_test.Instantiate(&metric_registry_, "test")),
      maintenance_op_duration_(METRIC_maintenance_op_duration.Instantiate(metric_entity_)),
      maintenance_ops_running_(METRIC_maintenance_ops_running.Instantiate(metric_entity_, 0)),
      remaining_runs_(1),
      prepared_runs_(0),
      sleep_time_(MonoDelta::FromSeconds(0)) {
  }

  virtual ~TestMaintenanceOp() {}

  virtual bool Prepare() OVERRIDE {
    std::lock_guard<Mutex> guard(lock_);
    if (remaining_runs_ == 0) {
      return false;
    }
    remaining_runs_--;
    prepared_runs_++;
    DLOG(INFO) << "Prepared op " << name();
    return true;
  }

  virtual void Perform() OVERRIDE {
    {
      std::lock_guard<Mutex> guard(lock_);
      DLOG(INFO) << "Performing op " << name();

      // Ensure that we don't call Perform() more times than we returned
      // success from Prepare().
      CHECK_GE(prepared_runs_, 1);
      prepared_runs_--;
    }

    SleepFor(sleep_time_);
  }

  virtual void UpdateStats(MaintenanceOpStats* stats) OVERRIDE {
    std::lock_guard<Mutex> guard(lock_);
    stats->set_runnable(remaining_runs_ > 0);
    stats->set_ram_anchored(consumption_.consumption());
    stats->set_logs_retained_bytes(logs_retained_bytes_);
    stats->set_perf_improvement(perf_improvement_);
  }

  void set_remaining_runs(int runs) {
    std::lock_guard<Mutex> guard(lock_);
    remaining_runs_ = runs;
  }

  void set_sleep_time(MonoDelta time) {
    std::lock_guard<Mutex> guard(lock_);
    sleep_time_ = time;
  }

  void set_ram_anchored(uint64_t ram_anchored) {
    std::lock_guard<Mutex> guard(lock_);
    consumption_.Reset(ram_anchored);
  }

  void set_logs_retained_bytes(uint64_t logs_retained_bytes) {
    std::lock_guard<Mutex> guard(lock_);
    logs_retained_bytes_ = logs_retained_bytes;
  }

  void set_perf_improvement(uint64_t perf_improvement) {
    std::lock_guard<Mutex> guard(lock_);
    perf_improvement_ = perf_improvement;
  }

  virtual scoped_refptr<Histogram> DurationHistogram() const OVERRIDE {
    return maintenance_op_duration_;
  }

  virtual scoped_refptr<AtomicGauge<uint32_t> > RunningGauge() const OVERRIDE {
    return maintenance_ops_running_;
  }

 private:
  Mutex lock_;

  ScopedTrackedConsumption consumption_;
  uint64_t logs_retained_bytes_;
  uint64_t perf_improvement_;
  MetricRegistry metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
  scoped_refptr<Histogram> maintenance_op_duration_;
  scoped_refptr<AtomicGauge<uint32_t> > maintenance_ops_running_;

  // The number of remaining times this operation will run before disabling
  // itself.
  int remaining_runs_;
  // The number of Prepared() operations which have not yet been Perform()ed.
  int prepared_runs_;

  // The amount of time each op invocation will sleep.
  MonoDelta sleep_time_;
};

// Create an op and wait for it to start running.  Unregister it while it is
// running and verify that UnregisterOp waits for it to finish before
// proceeding.
TEST_F(MaintenanceManagerTest, TestRegisterUnregister) {
  TestMaintenanceOp op1("1", MaintenanceOp::HIGH_IO_USAGE, test_tracker_);
  op1.set_ram_anchored(1001);
  // Register initially with no remaining runs. We'll later enable it once it's
  // already registered.
  op1.set_remaining_runs(0);
  manager_->RegisterOp(&op1);
  scoped_refptr<kudu::Thread> thread;
  CHECK_OK(Thread::Create(
      "TestThread", "TestRegisterUnregister",
      boost::bind(&TestMaintenanceOp::set_remaining_runs, &op1, 1), &thread));
  AssertEventually([&]() {
      ASSERT_EQ(op1.DurationHistogram()->TotalCount(), 1);
    });
  manager_->UnregisterOp(&op1);
  ThreadJoiner(thread.get()).Join();
}

// Regression test for KUDU-1495: when an operation is being unregistered,
// new instances of that operation should not be scheduled.
TEST_F(MaintenanceManagerTest, TestNewOpsDontGetScheduledDuringUnregister) {
  TestMaintenanceOp op1("1", MaintenanceOp::HIGH_IO_USAGE, test_tracker_);
  op1.set_ram_anchored(1001);

  // Set the op to run up to 10 times, and each time should sleep for a second.
  op1.set_remaining_runs(10);
  op1.set_sleep_time(MonoDelta::FromSeconds(1));
  manager_->RegisterOp(&op1);

  // Wait until two instances of the ops start running, since we have two MM threads.
  AssertEventually([&]() {
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
TEST_F(MaintenanceManagerTest, TestMemoryPressure) {
  TestMaintenanceOp op("op", MaintenanceOp::HIGH_IO_USAGE, test_tracker_);
  op.set_ram_anchored(100);
  manager_->RegisterOp(&op);

  // At first, we don't want to run this, since there is no perf_improvement.
  SleepFor(MonoDelta::FromMilliseconds(20));
  ASSERT_EQ(0, op.DurationHistogram()->TotalCount());

  // set the ram_anchored by the high mem op so high that we'll have to run it.
  scoped_refptr<kudu::Thread> thread;
  ASSERT_OK(Thread::Create("TestThread", "MaintenanceManagerTest",
      boost::bind(&TestMaintenanceOp::set_ram_anchored, &op, 1100), &thread));

  AssertEventually([&]() {
      ASSERT_EQ(op.DurationHistogram()->TotalCount(), 1);
    });
  manager_->UnregisterOp(&op);
  ThreadJoiner(thread.get()).Join();
}

// Test that ops are prioritized correctly when we add log retention.
TEST_F(MaintenanceManagerTest, TestLogRetentionPrioritization) {
  manager_->Shutdown();

  TestMaintenanceOp op1("op1", MaintenanceOp::LOW_IO_USAGE, test_tracker_);
  op1.set_ram_anchored(0);
  op1.set_logs_retained_bytes(100);

  TestMaintenanceOp op2("op2", MaintenanceOp::HIGH_IO_USAGE, test_tracker_);
  op2.set_ram_anchored(100);
  op2.set_logs_retained_bytes(100);

  TestMaintenanceOp op3("op3", MaintenanceOp::HIGH_IO_USAGE, test_tracker_);
  op3.set_ram_anchored(200);
  op3.set_logs_retained_bytes(100);

  manager_->RegisterOp(&op1);
  manager_->RegisterOp(&op2);
  manager_->RegisterOp(&op3);

  // We want to do the low IO op first since it clears up some log retention.
  ASSERT_EQ(&op1, manager_->FindBestOp());

  manager_->UnregisterOp(&op1);

  // Low IO is taken care of, now we find the op clears the most log retention and ram.
  ASSERT_EQ(&op3, manager_->FindBestOp());

  manager_->UnregisterOp(&op3);

  ASSERT_EQ(&op2, manager_->FindBestOp());

  manager_->UnregisterOp(&op2);
}

// Test adding operations and make sure that the history of recently completed operations
// is correct in that it wraps around and doesn't grow.
TEST_F(MaintenanceManagerTest, TestCompletedOpsHistory) {
  for (int i = 0; i < 5; i++) {
    string name = Substitute("op$0", i);
    TestMaintenanceOp op(name, MaintenanceOp::HIGH_IO_USAGE, test_tracker_);
    op.set_perf_improvement(1);
    op.set_ram_anchored(100);
    manager_->RegisterOp(&op);

    AssertEventually([&]() {
        ASSERT_EQ(op.DurationHistogram()->TotalCount(), 1);
      });
    manager_->UnregisterOp(&op);

    MaintenanceManagerStatusPB status_pb;
    manager_->GetMaintenanceManagerStatusDump(&status_pb);
    // The size should be at most the history_size.
    ASSERT_GE(kHistorySize, status_pb.completed_operations_size());
    // The most recently completed op should always be first, even if we wrap
    // around.
    ASSERT_EQ(name, status_pb.completed_operations(0).name());
  }
}

} // namespace kudu
