// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TSERVER_MAINTENANCE_MANAGER_H
#define KUDU_TSERVER_MAINTENANCE_MANAGER_H

#include "gutil/macros.h"
#include "tablet/mvcc.h"
#include "util/countdown_latch.h"
#include "util/thread.h"
#include "util/threadpool.h"

#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>
#include <map>
#include <set>
#include <stdint.h>
#include <string>
#include <tr1/memory>

namespace kudu {

class MaintenanceManager;

struct MaintenanceOpStats {
  // True if this op can be run now.
  bool runnable;

  // The approximate amount of memory that not doing this operation keeps
  // around.  This number is used to decide when to start freeing memory, so it
  // should be fairly accurate.  May be 0.
  uint64_t ram_anchored;

  // The age of the oldest transaction id (in milliseconds) that not doing this
  // operation keeps around.  May be 0.
  int32_t ts_anchored_secs;

  // The estimated performance improvement-- how good it is to do this on some
  // absolute scale from 0 to 2*64.
  uint64_t perf_improvement;

  // Zero all stats.
  void Clear();
};

// MaintenanceOp objects represent background operations that the
// MaintenanceManager can schedule.  Once a MaintenanceOp is registered, the
// manager will periodically poll it for statistics.  The registrant is
// responsible for managing the memory associated with the MaintenanceOp object.
// Op objects should be unregistered before being de-allocated.
class MaintenanceOp {
 public:
  friend class MaintenanceManager;

  explicit MaintenanceOp(const std::string& name);
  virtual ~MaintenanceOp();

  // Update the op statistics.  This will be called every scheduling period
  // (about a few times a second), so it should not be too expensive.
  // This will be run under the MaintenanceManager lock.
  virtual void UpdateStats(MaintenanceOpStats* stats) = 0;

  // Prepare to perform the operation.  This will be run without holding any
  // locks.  It should be short, since it is run from the context of the
  // maintenance op scheduler thread rather than a worker thread.
  // If this returns false, we will abort the operation.
  virtual bool Prepare() = 0;

  // Perform the operation.  This will be run without holding any locks, and may
  // take a long time.
  virtual void Perform() = 0;

  uint32_t running() { return running_; }

  std::string name() const;

 private:
  DISALLOW_COPY_AND_ASSIGN(MaintenanceOp);

  // The name of the operation.  Op names must be unique.
  const std::string name_;

  // The number of times that this op is currently running.
  uint32_t running_;

  // Condition variable which the UnregisterOp function can wait on.
  boost::condition_variable cond_;

  // The MaintenanceManager with which this op is registered, or null
  // if it is not registered.
  std::tr1::shared_ptr<MaintenanceManager> manager_;
};

struct MaintenanceOpComparator {
  bool operator() (const MaintenanceOp* lhs,
                   const MaintenanceOp* rhs) const {
    return lhs->name().compare(rhs->name()) < 0;
  }
};

// The MaintenanceManager manages the scheduling of background operations such
// as flushes or compactions.  It runs these operations in the background, in a
// thread pool.  It uses information provided in MaintenanceOpStats objects to
// decide which operations, if any, to run.
class MaintenanceManager
    : public std::tr1::enable_shared_from_this<MaintenanceManager> {
 public:
  struct Options {
    int32_t num_threads;
    int32_t polling_interval_ms;
    int64_t memory_limit;
    int32_t max_ts_anchored_secs;
  };

  static const Options DEFAULT_OPTIONS;

  static void Disable();
  explicit MaintenanceManager(const Options& options);
  ~MaintenanceManager();
  Status Init();
  void Shutdown();

  // Register an op with the manager.
  void RegisterOp(MaintenanceOp* op);

  // Unregister an op with the manager.
  // If the Op is currently running, it will not be interrupted.  However, this
  // function will block until the Op is finished.
  void UnregisterOp(MaintenanceOp* op);

 private:
  static Atomic32 disabled_;

  typedef std::map<MaintenanceOp*, MaintenanceOpStats,
          MaintenanceOpComparator> OpMapTy;

  DISALLOW_COPY_AND_ASSIGN(MaintenanceManager);

  void RunSchedulerThread();

  // find the best op, or null if there is nothing we want to run
  MaintenanceOp* FindBestOp();

  void LaunchOp(MaintenanceOp* op);
  Status CalculateMemTarget(uint64_t* mem_target);
  static Status CalculateMemTotal(uint64_t* total);

  const int32_t num_threads_;
  OpMapTy ops_; // registered operations
  boost::mutex lock_;
  scoped_refptr<kudu::Thread> monitor_thread_;
  kudu::ThreadPool thread_pool_;
  boost::system_time next_schedule_time_;
  boost::condition_variable cond_;
  bool shutdown_;
  uint64_t mem_target_;
  uint64_t running_ops_;
  int32_t polling_interval_ms_;
  int64_t memory_limit_;
  int32_t max_ts_anchored_secs_;
};

} // namespace kudu

#endif
