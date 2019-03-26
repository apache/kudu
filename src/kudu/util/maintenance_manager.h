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

#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest_prod.h>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/atomic.h"
#include "kudu/util/condition_variable.h"
#include "kudu/util/monotime.h"
#include "kudu/util/mutex.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"

namespace kudu {

template<class T>
class AtomicGauge;
class Histogram;
class MaintenanceManager;
class MaintenanceManagerStatusPB;
class MaintenanceManagerStatusPB_OpInstancePB;
class Thread;
class ThreadPool;

class MaintenanceOpStats {
 public:
  MaintenanceOpStats();

  // Zero all stats. They are invalid until the first setter is called.
  void Clear();

  bool runnable() const {
    DCHECK(valid_);
    return runnable_;
  }

  void set_runnable(bool runnable) {
    UpdateLastModified();
    runnable_ = runnable;
  }

  int64_t ram_anchored() const {
    DCHECK(valid_);
    return ram_anchored_;
  }

  void set_ram_anchored(int64_t ram_anchored) {
    UpdateLastModified();
    ram_anchored_ = ram_anchored;
  }

  int64_t logs_retained_bytes() const {
    DCHECK(valid_);
    return logs_retained_bytes_;
  }

  void set_logs_retained_bytes(int64_t logs_retained_bytes) {
    UpdateLastModified();
    logs_retained_bytes_ = logs_retained_bytes;
  }

  int64_t data_retained_bytes() const {
    DCHECK(valid_);
    return data_retained_bytes_;
  }

  void set_data_retained_bytes(int64_t data_retained_bytes) {
    UpdateLastModified();
    data_retained_bytes_ = data_retained_bytes;
  }

  double perf_improvement() const {
    DCHECK(valid_);
    return perf_improvement_;
  }

  void set_perf_improvement(double perf_improvement) {
    UpdateLastModified();
    perf_improvement_ = perf_improvement;
  }

  const MonoTime& last_modified() const {
    DCHECK(valid_);
    return last_modified_;
  }

  bool valid() const {
    return valid_;
  }

 private:
  void UpdateLastModified() {
    valid_ = true;
    last_modified_ = MonoTime::Now();
  }

  // Important: Update Clear() when adding fields to this class.

  // True if these stats are valid.
  bool valid_;

  // True if this op can be run now.
  bool runnable_;

  // The approximate amount of memory that not doing this operation keeps
  // around. This number is used to decide when to start freeing memory, so it
  // should be fairly accurate. May be 0.
  int64_t ram_anchored_;

  // Approximate amount of disk space in WAL files that would be freed if this
  // operation ran. May be 0.
  int64_t logs_retained_bytes_;

  // Approximate amount of disk space in data blocks that would be freed if
  // this operation ran. May be 0.
  int64_t data_retained_bytes_;

  // The estimated performance improvement-- how good it is to do this on some
  // absolute scale (yet TBD).
  double perf_improvement_;

  // The last time that the stats were modified.
  MonoTime last_modified_;
};

// Represents an instance of a maintenance operation.
struct OpInstance {
  // Id of thread the instance ran on.
  int64_t thread_id;
  // Name of operation.
  std::string name;
  // Time the operation took to run. Value is unitialized if instance is still running.
  MonoDelta duration;
  // The time at which the operation was launched.
  MonoTime start_mono_time;

  MaintenanceManagerStatusPB_OpInstancePB DumpToPB() const;
};

// MaintenanceOp objects represent background operations that the
// MaintenanceManager can schedule. Once a MaintenanceOp is registered, the
// manager will periodically poll it for statistics. The registrant is
// responsible for managing the memory associated with the MaintenanceOp object.
// Op objects should be unregistered before being de-allocated.
class MaintenanceOp {
 public:
  friend class MaintenanceManager;

  // General indicator of how much IO the Op will use.
  enum IOUsage {
    LOW_IO_USAGE, // Low impact operations like removing a file, updating metadata.
    HIGH_IO_USAGE // Everything else.
  };

  explicit MaintenanceOp(std::string name, IOUsage io_usage);
  virtual ~MaintenanceOp();

  // Unregister this op, if it is currently registered.
  void Unregister();

  // Update the op statistics. This will be called every scheduling period
  // (about a few times a second), so it should not be too expensive.  It's
  // possible for the returned statistics to be invalid; the caller should
  // call MaintenanceOpStats::valid() before using them. This will be run
  // under the MaintenanceManager lock.
  virtual void UpdateStats(MaintenanceOpStats* stats) = 0;

  // Prepare to perform the operation. This will be run without holding the
  // maintenance manager lock. It should be short, since it is run from the
  // context of the maintenance op scheduler thread rather than a worker thread.
  // If this returns false, we will abort the operation.
  virtual bool Prepare() = 0;

  // Perform the operation. This will be run without holding the maintenance
  // manager lock, and may take a long time.
  virtual void Perform() = 0;

  // Returns the histogram for this op that tracks duration. Cannot be NULL.
  virtual scoped_refptr<Histogram> DurationHistogram() const = 0;

  // Returns the gauge for this op that tracks when this op is running. Cannot be NULL.
  virtual scoped_refptr<AtomicGauge<uint32_t>> RunningGauge() const = 0;

  uint32_t running() { return running_; }

  const std::string& name() const { return name_; }

  IOUsage io_usage() const { return io_usage_; }

  // Return true if the operation has been cancelled due to a pending Unregister().
  bool cancelled() const {
    return cancel_.Load();
  }

  // Cancel this operation, which prevents new instances of it from being scheduled
  // regardless of whether the statistics indicate it is runnable. Instances may also
  // optionally poll 'cancelled()' on a periodic basis to know if they should abort a
  // lengthy operation in the middle of Perform().
  void CancelAndDisable() {
    cancel_.Store(true);
  }

 protected:
  virtual const std::string& table_id() const = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(MaintenanceOp);

  // The name of the operation.  Op names must be unique.
  const std::string name_;

  // The number of instances of this op that are currently running.
  uint32_t running_;

  // Set when we are trying to unregister the maintenance operation.
  // Ongoing operations could read this boolean and cancel themselves.
  // New operations will not be scheduled when this boolean is set.
  AtomicBool cancel_;

  // Condition variable which the UnregisterOp function can wait on.
  //
  // Note: 'cond_' is used with the MaintenanceManager's mutex. As such,
  // it only exists when the op is registered.
  std::unique_ptr<ConditionVariable> cond_;

  // The MaintenanceManager with which this op is registered, or null
  // if it is not registered.
  std::shared_ptr<MaintenanceManager> manager_;

  IOUsage io_usage_;
};

struct MaintenanceOpComparator {
  bool operator() (const MaintenanceOp* lhs,
                   const MaintenanceOp* rhs) const {
    return lhs->name().compare(rhs->name()) < 0;
  }
};

// The MaintenanceManager manages the scheduling of background operations such
// as flushes or compactions. It runs these operations in the background on a
// thread pool. It uses information provided in MaintenanceOpStats objects to
// decide which operations, if any, to run.
class MaintenanceManager : public std::enable_shared_from_this<MaintenanceManager> {
 public:
  struct Options {
    int32_t num_threads;
    int32_t polling_interval_ms;
    uint32_t history_size;
  };

  MaintenanceManager(const Options& options, std::string server_uuid);
  ~MaintenanceManager();

  // Start running the maintenance manager.
  // Must be called at most once.
  Status Start();
  void Shutdown();

  // Register an op with the manager.
  void RegisterOp(MaintenanceOp* op);

  // Unregister an op with the manager.
  // If the Op is currently running, it will not be interrupted.  However, this
  // function will block until the Op is finished.
  void UnregisterOp(MaintenanceOp* op);

  void GetMaintenanceManagerStatusDump(MaintenanceManagerStatusPB* out_pb);

  void set_memory_pressure_func_for_tests(std::function<bool(double*)> f) {
    std::lock_guard<Mutex> guard(lock_);
    memory_pressure_func_ = std::move(f);
  }

  static const Options kDefaultOptions;

 private:
  FRIEND_TEST(MaintenanceManagerTest, TestLogRetentionPrioritization);
  FRIEND_TEST(MaintenanceManagerTest, TestOpFactors);

  typedef std::map<MaintenanceOp*, MaintenanceOpStats,
          MaintenanceOpComparator> OpMapTy;
  typedef std::unordered_map<std::string, int32_t> TablePriorities;

  // Return true if tests have currently disabled the maintenance
  // manager by way of changing the gflags at runtime.
  bool disabled_for_tests() const;

  void RunSchedulerThread();

  bool FindAndLaunchOp(std::unique_lock<Mutex>* guard);

  // Find the best op, or null if there is nothing we want to run.
  //
  // Returns the op, as well as a string explanation of why that op was chosen,
  // suitable for logging.
  std::pair<MaintenanceOp*, std::string> FindBestOp();

  double PerfImprovement(double perf_improvement,
                         const std::string& table_id) const;

  void LaunchOp(MaintenanceOp* op);

  std::string LogPrefix() const;

  bool HasFreeThreads();

  bool CouldNotLaunchNewOp(bool prev_iter_found_no_work);

  void UpdateTablePriorities();

  void IncreaseOpCount(MaintenanceOp *op);
  void DecreaseOpCount(MaintenanceOp *op);

  const std::string server_uuid_;
  TablePriorities table_priorities_;
  const int32_t num_threads_;
  OpMapTy ops_; // Registered operations.
  Mutex lock_;
  scoped_refptr<kudu::Thread> monitor_thread_;
  gscoped_ptr<ThreadPool> thread_pool_;
  ConditionVariable cond_;
  bool shutdown_;
  int32_t polling_interval_ms_;
  int32_t running_ops_;
  // Vector used as a circular buffer for recently completed ops. Elements need to be added at
  // the completed_ops_count_ % the vector's size and then the count needs to be incremented.
  std::vector<OpInstance> completed_ops_;
  int64_t completed_ops_count_;
  Random rand_;

  // Function which should return true if the server is under global memory pressure.
  // This is indirected for testing purposes.
  std::function<bool(double*)> memory_pressure_func_;

  // Running instances lock.
  //
  // This is separate from lock_ so that worker threads don't need to take the
  // global MM lock when beginning operations. When taking both
  // running_instances_lock_ and lock_, lock_ must be acquired first.
  Mutex running_instances_lock_;

  // Maps thread ids to instances of an op that they're running. Instances should be added
  // right before MaintenanceOp::Perform() is called, and should be removed right after
  // MaintenanceOp::Perform() completes. Any thread that adds an instance to this map
  // owns that instance, and the instance should exist until the same thread removes it.
  //
  // Protected by running_instances_lock_;
  std::unordered_map<int64_t, OpInstance*> running_instances_;

  DISALLOW_COPY_AND_ASSIGN(MaintenanceManager);
};

} // namespace kudu
