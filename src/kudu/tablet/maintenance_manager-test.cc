// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <tr1/memory>
#include <vector>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/maintenance_manager.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/util/metrics.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

using std::tr1::shared_ptr;
using std::vector;
using strings::Substitute;
using kudu::tablet::MaintenanceManagerStatusPB;

METRIC_DEFINE_gauge_uint32(running_gauge, kudu::MetricUnit::kMaintenanceOperations, "");

METRIC_DEFINE_histogram(duration_histogram, kudu::MetricUnit::kSeconds, "", 60000000LU, 2);

namespace kudu {

const int kHistorySize = 4;

class MaintenanceManagerTest : public KuduTest {
 public:
  MaintenanceManagerTest() {
    MaintenanceManager::Options options;
    options.num_threads = 2;
    options.polling_interval_ms = 1;
    options.memory_limit = 1000;
    options.history_size = kHistorySize;
    manager_.reset(new MaintenanceManager(options));
    manager_->Init();
  }
  ~MaintenanceManagerTest() {
    manager_->Shutdown();
  }

 protected:
  shared_ptr<MaintenanceManager> manager_;
};

// Just create the MaintenanceManager and then shut it down, to make sure
// there are no race conditions there.
TEST_F(MaintenanceManagerTest, TestCreateAndShutdown) {
}

enum TestMaintenanceOpState {
  OP_DISABLED,
  OP_RUNNABLE,
  OP_RUNNING,
  OP_FINISHED,
};

class TestMaintenanceOp : public MaintenanceOp {
 public:
  TestMaintenanceOp(const std::string& name,
                    TestMaintenanceOpState state)
    : MaintenanceOp(name),
      state_change_cond_(&lock_),
      state_(state),
      ram_anchored_(500),
      logs_retained_bytes_(0),
      perf_improvement_(0),
      metric_ctx_(&metric_registry_, "test"),
      duration_histogram_(METRIC_duration_histogram.Instantiate(metric_ctx_)),
      running_gauge_(AtomicGauge<uint32_t>::Instantiate(METRIC_running_gauge, metric_ctx_)) { }

  virtual ~TestMaintenanceOp() {
  }

  virtual bool Prepare() OVERRIDE {
    lock_guard<Mutex> guard(&lock_);
    if (state_ != OP_RUNNABLE) {
      return false;
    }
    state_ = OP_RUNNING;
    state_change_cond_.Broadcast();
    DLOG(INFO) << "Prepared op " << name();
    return true;
  }

  virtual void Perform() OVERRIDE {
    DLOG(INFO) << "Performing op " << name();
    lock_guard<Mutex> guard(&lock_);
    CHECK_EQ(OP_RUNNING, state_);
    state_ = OP_FINISHED;
    state_change_cond_.Broadcast();
  }

  virtual void UpdateStats(MaintenanceOpStats* stats) OVERRIDE {
    lock_guard<Mutex> guard(&lock_);
    stats->runnable = (state_ == OP_RUNNABLE);
    stats->ram_anchored = ram_anchored_;
    stats->logs_retained_bytes = logs_retained_bytes_;
    stats->perf_improvement = perf_improvement_;
  }

  void Enable() {
    lock_guard<Mutex> guard(&lock_);
    DCHECK((state_ == OP_DISABLED) || (state_ == OP_FINISHED));
    state_ = OP_RUNNABLE;
    state_change_cond_.Broadcast();
  }

  void WaitForState(TestMaintenanceOpState state) {
    lock_guard<Mutex> guard(&lock_);
    while (true) {
      if (state_ == state) {
        return;
      }
      state_change_cond_.Wait();
    }
  }

  bool WaitForStateWithTimeout(TestMaintenanceOpState state, int ms) {
    MonoDelta to_wait = MonoDelta::FromMilliseconds(ms);
    lock_guard<Mutex> guard(&lock_);
    while (true) {
      if (state_ == state) {
        return true;
      }
      if (!state_change_cond_.TimedWait(to_wait)) {
        return false;
      }
    }
  }

  void set_ram_anchored(uint64_t ram_anchored) {
    lock_guard<Mutex> guard(&lock_);
    ram_anchored_ = ram_anchored;
  }

  void set_logs_retained_bytes(uint64_t logs_retained_bytes) {
    lock_guard<Mutex> guard(&lock_);
    logs_retained_bytes_ = logs_retained_bytes;
  }

  void set_perf_improvement(uint64_t perf_improvement) {
    lock_guard<Mutex> guard(&lock_);
    perf_improvement_ = perf_improvement;
  }

  virtual scoped_refptr<Histogram> DurationHistogram() OVERRIDE {
    return duration_histogram_;
  }

  virtual scoped_refptr<AtomicGauge<uint32_t> > RunningGauge() OVERRIDE {
    return running_gauge_;
  }

 private:
  Mutex lock_;
  ConditionVariable state_change_cond_;
  enum TestMaintenanceOpState state_;
  uint64_t ram_anchored_;
  uint64_t logs_retained_bytes_;
  uint64_t perf_improvement_;
  MetricRegistry metric_registry_;
  MetricContext metric_ctx_;
  scoped_refptr<Histogram> duration_histogram_;
  scoped_refptr<AtomicGauge<uint32_t> > running_gauge_;
};

// Create an op and wait for it to start running.  Unregister it while it is
// running and verify that UnregisterOp waits for it to finish before
// proceeding.
TEST_F(MaintenanceManagerTest, TestRegisterUnregister) {
  TestMaintenanceOp op1("1", OP_DISABLED);
  op1.set_ram_anchored(1001);
  manager_->RegisterOp(&op1);
  scoped_refptr<kudu::Thread> thread;
  CHECK_OK(Thread::Create("TestThread", "TestRegisterUnregister",
        boost::bind(&TestMaintenanceOp::Enable, &op1), &thread));
  op1.WaitForState(OP_FINISHED);
  manager_->UnregisterOp(&op1);
  ThreadJoiner(thread.get()).Join();
}

// Test that we'll run an operation that doesn't improve performance when memory
// pressure gets high.
TEST_F(MaintenanceManagerTest, TestMemoryPressure) {
  TestMaintenanceOp op("op", OP_RUNNABLE);
  op.set_perf_improvement(0);
  op.set_ram_anchored(100);
  manager_->RegisterOp(&op);

  // At first, we don't want to run this, since there is no perf_improvement.
  CHECK_EQ(false, op.WaitForStateWithTimeout(OP_FINISHED, 20));

  // set the ram_anchored by the high mem op so high that we'll have to run it.
  scoped_refptr<kudu::Thread> thread;
  CHECK_OK(Thread::Create("TestThread", "MaintenanceManagerTest",
      boost::bind(&TestMaintenanceOp::set_ram_anchored, &op, 1100), &thread));
  op.WaitForState(OP_FINISHED);
  manager_->UnregisterOp(&op);
  ThreadJoiner(thread.get()).Join();
}

// Test that ops are prioritized correctly when we add log retention.
TEST_F(MaintenanceManagerTest, TestLogRetentionPrioritization) {
  manager_->Shutdown();

  TestMaintenanceOp op1("op1", OP_RUNNABLE);
  op1.set_perf_improvement(0);
  op1.set_ram_anchored(100);
  op1.set_logs_retained_bytes(100);
  manager_->RegisterOp(&op1);

  MaintenanceOp* best_op = manager_->FindBestOp();
  ASSERT_EQ(&op1, best_op);

  TestMaintenanceOp op2("op2", OP_RUNNABLE);
  op2.set_perf_improvement(0);
  op2.set_ram_anchored(200);
  op2.set_logs_retained_bytes(100);
  manager_->RegisterOp(&op2);

  best_op = manager_->FindBestOp();
  ASSERT_EQ(&op2, best_op);

  manager_->UnregisterOp(&op2);

  best_op = manager_->FindBestOp();
  ASSERT_EQ(&op1, best_op);

  manager_->UnregisterOp(&op1);
}

// Test adding operations and make sure that the history of recently completed operations
// is correct in that it wraps around and doesn't grow.
TEST_F(MaintenanceManagerTest, TestCompletedOpsHistory) {
  for (int i = 0; i < 5; i++) {
    string name = Substitute("op$0", i);
    TestMaintenanceOp op(name, OP_RUNNABLE);
    op.set_perf_improvement(1);
    op.set_ram_anchored(100);
    manager_->RegisterOp(&op);

    CHECK_EQ(true, op.WaitForStateWithTimeout(OP_FINISHED, 200));
    manager_->UnregisterOp(&op);

    MaintenanceManagerStatusPB status_pb;
    manager_->GetMaintenanceManagerStatusDump(&status_pb);
    // The size should be at most the history_size.
    ASSERT_GE(kHistorySize, status_pb.completed_operations_size());
    // See that we have the right name, even if we wrap around.
    ASSERT_EQ(name, status_pb.completed_operations(i % 4).name());
  }
}

} // namespace kudu
