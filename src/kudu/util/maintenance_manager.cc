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

#include <cinttypes>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>

#include <boost/bind.hpp>
#include <gflags/gflags.h>

#include "kudu/gutil/dynamic_annotations.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/debug/trace_logging.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/maintenance_manager.pb.h"
#include "kudu/util/metrics.h"
#include "kudu/util/process_memory.h"
#include "kudu/util/random_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/thread.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/trace.h"

using std::pair;
using std::string;
using strings::Substitute;

DEFINE_int32(maintenance_manager_num_threads, 1,
             "Size of the maintenance manager thread pool. "
             "For spinning disks, the number of threads should "
             "not be above the number of devices.");
TAG_FLAG(maintenance_manager_num_threads, stable);

DEFINE_int32(maintenance_manager_polling_interval_ms, 250,
       "Polling interval for the maintenance manager scheduler, "
       "in milliseconds.");
TAG_FLAG(maintenance_manager_polling_interval_ms, hidden);

DEFINE_int32(maintenance_manager_history_size, 8,
       "Number of completed operations the manager is keeping track of.");
TAG_FLAG(maintenance_manager_history_size, hidden);

DEFINE_bool(enable_maintenance_manager, true,
       "Enable the maintenance manager, runs compaction and tablet cleaning tasks.");
TAG_FLAG(enable_maintenance_manager, unsafe);

DEFINE_int64(log_target_replay_size_mb, 1024,
             "The target maximum size of logs to be replayed at startup. If a tablet "
             "has in-memory operations that are causing more than this size of logs "
             "to be retained, then the maintenance manager will prioritize flushing "
             "these operations to disk.");
TAG_FLAG(log_target_replay_size_mb, experimental);

DEFINE_int64(data_gc_min_size_mb, 0,
             "The (exclusive) minimum number of megabytes of ancient data on "
             "disk, per tablet, needed to prioritize deletion of that data.");
TAG_FLAG(data_gc_min_size_mb, experimental);

DEFINE_double(data_gc_prioritization_prob, 0.5,
             "The probability that we will prioritize data GC over performance "
             "improvement operations. If set to 1.0, we will always prefer to "
             "delete old data before running performance improvement operations "
             "such as delta compaction.");
TAG_FLAG(data_gc_prioritization_prob, experimental);

namespace kudu {

MaintenanceOpStats::MaintenanceOpStats() {
  Clear();
}

void MaintenanceOpStats::Clear() {
  valid_ = false;
  runnable_ = false;
  ram_anchored_ = 0;
  logs_retained_bytes_ = 0;
  data_retained_bytes_ = 0;
  perf_improvement_ = 0;
  last_modified_ = MonoTime();
}

MaintenanceOp::MaintenanceOp(std::string name, IOUsage io_usage)
    : name_(std::move(name)),
      running_(0),
      cancel_(false),
      io_usage_(io_usage) {
}

MaintenanceOp::~MaintenanceOp() {
  CHECK(!manager_.get()) << "You must unregister the " << name_
         << " Op before destroying it.";
}

void MaintenanceOp::Unregister() {
  CHECK(manager_.get()) << "Op " << name_ << " was never registered.";
  manager_->UnregisterOp(this);
}

MaintenanceManagerStatusPB_OpInstancePB OpInstance::DumpToPB() const {
  MaintenanceManagerStatusPB_OpInstancePB pb;
  pb.set_thread_id(thread_id);
  pb.set_name(name);
  if (duration.Initialized()) {
    pb.set_duration_millis(duration.ToMilliseconds());
  }
  MonoDelta delta(MonoTime::Now() - start_mono_time);
  pb.set_millis_since_start(delta.ToMilliseconds());
  return pb;
}

const MaintenanceManager::Options MaintenanceManager::kDefaultOptions = {
  .num_threads = 0,
  .polling_interval_ms = 0,
  .history_size = 0,
};

MaintenanceManager::MaintenanceManager(const Options& options)
  : num_threads_(options.num_threads <= 0 ?
      FLAGS_maintenance_manager_num_threads : options.num_threads),
    cond_(&lock_),
    shutdown_(false),
    polling_interval_ms_(options.polling_interval_ms <= 0 ?
          FLAGS_maintenance_manager_polling_interval_ms :
          options.polling_interval_ms),
    running_ops_(0),
    completed_ops_count_(0),
    rand_(GetRandomSeed32()),
    memory_pressure_func_(&process_memory::UnderMemoryPressure) {
  CHECK_OK(ThreadPoolBuilder("MaintenanceMgr").set_min_threads(num_threads_)
               .set_max_threads(num_threads_).Build(&thread_pool_));
  uint32_t history_size = options.history_size == 0 ?
                          FLAGS_maintenance_manager_history_size :
                          options.history_size;
  completed_ops_.resize(history_size);
}

MaintenanceManager::~MaintenanceManager() {
  Shutdown();
}

Status MaintenanceManager::Init(std::string server_uuid) {
  server_uuid_ = std::move(server_uuid);
  RETURN_NOT_OK(Thread::Create("maintenance", "maintenance_scheduler",
      boost::bind(&MaintenanceManager::RunSchedulerThread, this),
      &monitor_thread_));
  return Status::OK();
}

void MaintenanceManager::Shutdown() {
  {
    std::lock_guard<Mutex> guard(lock_);
    if (shutdown_) {
      return;
    }
    shutdown_ = true;
    cond_.Broadcast();
  }
  if (monitor_thread_.get()) {
    CHECK_OK(ThreadJoiner(monitor_thread_.get()).Join());
    monitor_thread_.reset();
    // Wait for all the running and queued tasks before shutting down. Otherwise,
    // Shutdown() can remove a queued task silently. We count on eventually running the
    // queued tasks to decrement their "running" count, which is incremented at the time
    // they are enqueued.
    thread_pool_->Wait();
    thread_pool_->Shutdown();
  }
}

void MaintenanceManager::RegisterOp(MaintenanceOp* op) {
  CHECK(op);
  std::lock_guard<Mutex> guard(lock_);
  CHECK(!op->manager_) << "Tried to register " << op->name()
          << ", but it was already registered.";
  pair<OpMapTy::iterator, bool> val
    (ops_.insert(OpMapTy::value_type(op, MaintenanceOpStats())));
  CHECK(val.second)
      << "Tried to register " << op->name()
      << ", but it already exists in ops_.";
  op->manager_ = shared_from_this();
  op->cond_.reset(new ConditionVariable(&lock_));
  VLOG_AND_TRACE("maintenance", 1) << LogPrefix() << "Registered " << op->name();
}

void MaintenanceManager::UnregisterOp(MaintenanceOp* op) {
  {
    std::lock_guard<Mutex> guard(lock_);
    CHECK(op->manager_.get() == this) << "Tried to unregister " << op->name()
          << ", but it is not currently registered with this maintenance manager.";
    auto iter = ops_.find(op);
    CHECK(iter != ops_.end()) << "Tried to unregister " << op->name()
        << ", but it was never registered";
    // While the op is running, wait for it to be finished.
    if (iter->first->running_ > 0) {
      VLOG_AND_TRACE("maintenance", 1) << LogPrefix() << "Waiting for op " << op->name()
                                       << " to finish so we can unregister it.";
    }
    op->CancelAndDisable();
    while (iter->first->running_ > 0) {
      op->cond_->Wait();
      iter = ops_.find(op);
      CHECK(iter != ops_.end()) << "Tried to unregister " << op->name()
          << ", but another thread unregistered it while we were "
          << "waiting for it to complete";
    }
    ops_.erase(iter);
  }
  LOG_WITH_PREFIX(INFO) << "Unregistered op " << op->name();
  op->cond_.reset();
  // Remove the op's shared_ptr reference to us.  This might 'delete this'.
  op->manager_.reset();
}

bool MaintenanceManager::disabled_for_tests() const {
  return !ANNOTATE_UNPROTECTED_READ(FLAGS_enable_maintenance_manager);
}

void MaintenanceManager::RunSchedulerThread() {
  if (!FLAGS_enable_maintenance_manager) {
    LOG(INFO) << "Maintenance manager is disabled. Stopping thread.";
    return;
  }

  MonoDelta polling_interval = MonoDelta::FromMilliseconds(polling_interval_ms_);

  std::unique_lock<Mutex> guard(lock_);

  // Set to true if the scheduler runs and finds that there is no work to do.
  bool prev_iter_found_no_work = false;

  while (true) {
    // We'll keep sleeping if:
    //    1) there are no free threads available to perform a maintenance op.
    // or 2) we just tried to schedule an op but found nothing to run.
    // However, if it's time to shut down, we want to do so immediately.
    while ((running_ops_ >= num_threads_ || prev_iter_found_no_work || disabled_for_tests()) &&
           !shutdown_) {
      cond_.WaitFor(polling_interval);
      prev_iter_found_no_work = false;
    }
    if (shutdown_) {
      VLOG_AND_TRACE("maintenance", 1) << LogPrefix() << "Shutting down maintenance manager.";
      return;
    }

    // Find the best op.
    pair<MaintenanceOp*, string> op_and_note = FindBestOp();
    auto* op = op_and_note.first;
    const auto& note = op_and_note.second;

    // If we found no work to do, then we should sleep before trying again to schedule.
    // Otherwise, we can go right into trying to find the next op.
    prev_iter_found_no_work = (op == nullptr);
    if (!op) {
      VLOG_AND_TRACE("maintenance", 2) << LogPrefix()
                                       << "No maintenance operations look worth doing.";
      continue;
    }

    // Prepare the maintenance operation.
    op->running_++;
    running_ops_++;
    guard.unlock();
    bool ready = op->Prepare();
    guard.lock();
    if (!ready) {
      LOG_WITH_PREFIX(INFO) << "Prepare failed for " << op->name()
                            << ".  Re-running scheduler.";
      op->running_--;
      running_ops_--;
      op->cond_->Signal();
      continue;
    }

    LOG_AND_TRACE("maintenance", INFO) << LogPrefix() << "Scheduling "
                                       << op->name() << ": " << note;
    // Run the maintenance operation.
    Status s = thread_pool_->SubmitFunc(boost::bind(
        &MaintenanceManager::LaunchOp, this, op));
    CHECK(s.ok());
  }
}

// Finding the best operation goes through four filters:
// - If there's an Op that we can run quickly that frees log retention, we run it.
// - If we've hit the overall process memory limit (note: this includes memory that the Ops cannot
//   free), we run the Op with the highest RAM usage.
// - If there are Ops that are retaining logs past our target replay size, we run the one that has
//   the highest retention (and if many qualify, then we run the one that also frees up the
//   most RAM).
// - Finally, if there's nothing else that we really need to do, we run the Op that will improve
//   performance the most.
//
// The reason it's done this way is that we want to prioritize limiting the amount of resources we
// hold on to. Low IO Ops go first since we can quickly run them, then we can look at memory usage.
// Reversing those can starve the low IO Ops when the system is under intense memory pressure.
//
// In the third priority we're at a point where nothing's urgent and there's nothing we can run
// quickly.
// TODO We currently optimize for freeing log retention but we could consider having some sort of
// sliding priority between log retention and RAM usage. For example, is an Op that frees
// 128MB of log retention and 12MB of RAM always better than an op that frees 12MB of log retention
// and 128MB of RAM? Maybe a more holistic approach would be better.
pair<MaintenanceOp*, string> MaintenanceManager::FindBestOp() {
  TRACE_EVENT0("maintenance", "MaintenanceManager::FindBestOp");

  size_t free_threads = num_threads_ - running_ops_;
  if (free_threads == 0) {
    return {nullptr, "no free threads"};
  }

  int64_t low_io_most_logs_retained_bytes = 0;
  MaintenanceOp* low_io_most_logs_retained_bytes_op = nullptr;

  uint64_t most_mem_anchored = 0;
  MaintenanceOp* most_mem_anchored_op = nullptr;

  int64_t most_logs_retained_bytes = 0;
  int64_t most_logs_retained_bytes_ram_anchored = 0;
  MaintenanceOp* most_logs_retained_bytes_op = nullptr;

  int64_t most_data_retained_bytes = 0;
  MaintenanceOp* most_data_retained_bytes_op = nullptr;

  double best_perf_improvement = 0;
  MaintenanceOp* best_perf_improvement_op = nullptr;
  for (OpMapTy::value_type &val : ops_) {
    MaintenanceOp* op(val.first);
    MaintenanceOpStats& stats(val.second);
    VLOG_WITH_PREFIX(3) << "Considering MM op " << op->name();
    // Update op stats.
    stats.Clear();
    op->UpdateStats(&stats);
    if (op->cancelled() || !stats.valid() || !stats.runnable()) {
      continue;
    }
    if (stats.logs_retained_bytes() > low_io_most_logs_retained_bytes &&
        op->io_usage() == MaintenanceOp::LOW_IO_USAGE) {
      low_io_most_logs_retained_bytes_op = op;
      low_io_most_logs_retained_bytes = stats.logs_retained_bytes();
      VLOG_AND_TRACE("maintenance", 2) << LogPrefix() << "Op " << op->name() << " can free "
                                       << stats.logs_retained_bytes() << " bytes of logs";
    }

    if (stats.ram_anchored() > most_mem_anchored) {
      most_mem_anchored_op = op;
      most_mem_anchored = stats.ram_anchored();
    }
    // We prioritize ops that can free more logs, but when it's the same we pick the one that
    // also frees up the most memory.
    if (stats.logs_retained_bytes() > 0 &&
        (stats.logs_retained_bytes() > most_logs_retained_bytes ||
            (stats.logs_retained_bytes() == most_logs_retained_bytes &&
                stats.ram_anchored() > most_logs_retained_bytes_ram_anchored))) {
      most_logs_retained_bytes_op = op;
      most_logs_retained_bytes = stats.logs_retained_bytes();
      most_logs_retained_bytes_ram_anchored = stats.ram_anchored();
    }

    if (stats.data_retained_bytes() > most_data_retained_bytes) {
      most_data_retained_bytes_op = op;
      most_data_retained_bytes = stats.data_retained_bytes();
      VLOG_AND_TRACE("maintenance", 2) << LogPrefix() << "Op " << op->name() << " can free "
                                       << stats.data_retained_bytes() << " bytes of data";
    }

    if ((!best_perf_improvement_op) ||
        (stats.perf_improvement() > best_perf_improvement)) {
      best_perf_improvement_op = op;
      best_perf_improvement = stats.perf_improvement();
    }
  }

  // Look at ops that we can run quickly that free up log retention.
  if (low_io_most_logs_retained_bytes_op) {
    if (low_io_most_logs_retained_bytes > 0) {
      string notes = Substitute("free $0 bytes of WAL", low_io_most_logs_retained_bytes);
      return {low_io_most_logs_retained_bytes_op, std::move(notes)};
    }
  }

  // Look at free memory. If it is dangerously low, we must select something
  // that frees memory-- the op with the most anchored memory.
  double capacity_pct;
  if (memory_pressure_func_(&capacity_pct)) {
    if (!most_mem_anchored_op) {
      std::string msg = StringPrintf("System under memory pressure "
          "(%.2f%% of limit used). However, there are no ops currently "
          "runnable which would free memory.", capacity_pct);
      LOG_WITH_PREFIX(INFO) << msg;
      return {nullptr, msg};
    }
    string note = StringPrintf("under memory pressure (%.2f%% used, "
                               "can flush %" PRIu64 " bytes)",
                               capacity_pct, most_mem_anchored);
    return {most_mem_anchored_op, std::move(note)};
  }

  if (most_logs_retained_bytes_op &&
      most_logs_retained_bytes / 1024 / 1024 >= FLAGS_log_target_replay_size_mb) {
    string note = Substitute("$0 bytes log retention", most_logs_retained_bytes);
    return {most_logs_retained_bytes_op, std::move(note)};
  }

  // Look at ops that we can run quickly that free up data on disk.
  if (most_data_retained_bytes_op &&
      most_data_retained_bytes > FLAGS_data_gc_min_size_mb * 1024 * 1024) {
    if (!best_perf_improvement_op || best_perf_improvement <= 0 ||
        rand_.NextDoubleFraction() <= FLAGS_data_gc_prioritization_prob) {
      string note = Substitute("$0 bytes on disk", most_data_retained_bytes);
      return {most_data_retained_bytes_op, std::move(note)};
    }
    VLOG(1) << "Skipping data GC due to prioritizing perf improvement";
  }

  if (best_perf_improvement_op && best_perf_improvement > 0) {
    string note = StringPrintf("perf score=%.6f", best_perf_improvement);
    return {best_perf_improvement_op, std::move(note)};
  }
  return {nullptr, "no ops with positive improvement"};
}

void MaintenanceManager::LaunchOp(MaintenanceOp* op) {
  int64_t thread_id = Thread::CurrentThreadId();
  OpInstance op_instance;
  op_instance.thread_id = thread_id;
  op_instance.name = op->name();
  op_instance.start_mono_time = MonoTime::Now();
  op->RunningGauge()->Increment();
  {
    std::lock_guard<Mutex> lock(running_instances_lock_);
    InsertOrDie(&running_instances_, thread_id, &op_instance);
  }

  SCOPED_CLEANUP({
    op->RunningGauge()->Decrement();

    std::lock_guard<Mutex> l(lock_);
    {
      std::lock_guard<Mutex> lock(running_instances_lock_);
      running_instances_.erase(thread_id);
    }
    op_instance.duration = MonoTime::Now() - op_instance.start_mono_time;
    completed_ops_[completed_ops_count_ % completed_ops_.size()] = op_instance;
    completed_ops_count_++;

    op->DurationHistogram()->Increment(op_instance.duration.ToMilliseconds());

    running_ops_--;
    op->running_--;
    op->cond_->Signal();
    cond_.Signal(); // wake up scheduler
  });

  scoped_refptr<Trace> trace(new Trace);
  Stopwatch sw;
  sw.start();
  {
    ADOPT_TRACE(trace.get());
    TRACE_EVENT1("maintenance", "MaintenanceManager::LaunchOp",
                 "name", op->name());
    op->Perform();
    sw.stop();
  }
  LOG_WITH_PREFIX(INFO) << op->name() << " complete. "
                        << "Timing: " << sw.elapsed().ToString()
                        << " Metrics: " << trace->MetricsAsJSON();
}

void MaintenanceManager::GetMaintenanceManagerStatusDump(MaintenanceManagerStatusPB* out_pb) {
  DCHECK(out_pb != nullptr);
  std::lock_guard<Mutex> guard(lock_);
  pair<MaintenanceOp*, string> best_op_and_why = FindBestOp();
  auto* best_op = best_op_and_why.first;

  for (MaintenanceManager::OpMapTy::value_type& val : ops_) {
    MaintenanceManagerStatusPB_MaintenanceOpPB* op_pb = out_pb->add_registered_operations();
    MaintenanceOp* op(val.first);
    MaintenanceOpStats& stat(val.second);
    op_pb->set_name(op->name());
    op_pb->set_running(op->running());
    if (stat.valid()) {
      op_pb->set_runnable(stat.runnable());
      op_pb->set_ram_anchored_bytes(stat.ram_anchored());
      op_pb->set_logs_retained_bytes(stat.logs_retained_bytes());
      op_pb->set_perf_improvement(stat.perf_improvement());
    } else {
      op_pb->set_runnable(false);
      op_pb->set_ram_anchored_bytes(0);
      op_pb->set_logs_retained_bytes(0);
      op_pb->set_perf_improvement(0.0);
    }

    if (best_op == op) {
      out_pb->mutable_best_op()->CopyFrom(*op_pb);
    }
  }

  {
    std::lock_guard<Mutex> lock(running_instances_lock_);
    for (const auto& running_instance : running_instances_) {
      *out_pb->add_running_operations() = running_instance.second->DumpToPB();
    }
  }

  for (int n = 1; n <= completed_ops_.size(); n++) {
    int i = completed_ops_count_ - n;
    if (i < 0) break;
    const auto& completed_op = completed_ops_[i % completed_ops_.size()];

    if (!completed_op.name.empty()) {
      *out_pb->add_completed_operations() = completed_op.DumpToPB();
    }
  }
}

std::string MaintenanceManager::LogPrefix() const {
  return Substitute("P $0: ", server_uuid_);
}

} // namespace kudu
