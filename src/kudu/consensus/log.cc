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

// **************   NOTICE  *******************************************
// Facebook 2019 - Notice of Changes
// This file has been modified to extract only the Raft implementation
// out of Kudu into a fork known as kuduraft.
// ********************************************************************

#include "kudu/consensus/log.h"

#include <cerrno>
#include <cstdint>
#include <memory>
#include <mutex>
#include <ostream>
#include <utility>

#include <boost/range/adaptor/reversed.hpp>
#include <gflags/gflags.h>

#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/log_index.h"
#include "kudu/consensus/log_metrics.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/log_util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/dynamic_annotations.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/async_util.h"
#include "kudu/util/compression/compression.pb.h"
#include "kudu/util/compression/compression_codec.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/kernel_stack_watchdog.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/trace.h"

// Log retention configuration.
// -----------------------------
DEFINE_int32(log_min_segments_to_retain, 1,
             "The minimum number of past log segments to keep at all times,"
             " regardless of what is required for durability. "
             "Must be at least 1.");
TAG_FLAG(log_min_segments_to_retain, runtime);
TAG_FLAG(log_min_segments_to_retain, advanced);

DEFINE_int32(log_max_segments_to_retain, 80,
             "The maximum number of past log segments to keep at all times for "
             "the purposes of catching up other peers.");
TAG_FLAG(log_max_segments_to_retain, runtime);
TAG_FLAG(log_max_segments_to_retain, advanced);
TAG_FLAG(log_max_segments_to_retain, experimental);


// Group commit configuration.
// -----------------------------
DEFINE_int32(group_commit_queue_size_bytes, 4 * 1024 * 1024,
             "Maximum size of the group commit queue in bytes");
TAG_FLAG(group_commit_queue_size_bytes, advanced);


DEFINE_int32(log_thread_idle_threshold_ms, 1000,
             "Number of milliseconds after which the log append thread decides that a "
             "log is idle, and considers shutting down. Used by tests.");
TAG_FLAG(log_thread_idle_threshold_ms, experimental);
TAG_FLAG(log_thread_idle_threshold_ms, hidden);

// Compression configuration.
// -----------------------------
DEFINE_string(log_compression_codec, "LZ4",
              "Codec to use for compressing WAL segments.");
TAG_FLAG(log_compression_codec, experimental);

// Fault/latency injection flags.
// -----------------------------
DEFINE_bool(log_inject_latency, false,
            "If true, injects artificial latency in log sync operations. "
            "Advanced option. Use at your own risk -- has a negative effect "
            "on performance for obvious reasons!");

DEFINE_bool(skip_remove_old_recovery_dir, false,
            "Skip removing WAL recovery dir after startup. (useful for debugging)");
TAG_FLAG(skip_remove_old_recovery_dir, hidden);

TAG_FLAG(log_inject_latency, unsafe);
TAG_FLAG(log_inject_latency, runtime);

DEFINE_int32(log_inject_latency_ms_mean, 100,
             "The number of milliseconds of latency to inject, on average. "
             "Only takes effect if --log_inject_latency is true");
TAG_FLAG(log_inject_latency_ms_mean, unsafe);
TAG_FLAG(log_inject_latency_ms_mean, runtime);

DEFINE_int32(log_inject_latency_ms_stddev, 100,
             "The standard deviation of latency to inject in the log. "
             "Only takes effect if --log_inject_latency is true");
TAG_FLAG(log_inject_latency_ms_stddev, unsafe);
TAG_FLAG(log_inject_latency_ms_stddev, runtime);

DEFINE_int32(log_inject_thread_lifecycle_latency_ms, 0,
             "Injection point for random latency during key thread lifecycle transition "
             "points.");
TAG_FLAG(log_inject_thread_lifecycle_latency_ms, unsafe);
TAG_FLAG(log_inject_thread_lifecycle_latency_ms, runtime);

DEFINE_double(fault_crash_before_append_commit, 0.0,
              "Fraction of the time when the server will crash just before appending a "
              "COMMIT message to the log. (For testing only!)");
TAG_FLAG(fault_crash_before_append_commit, unsafe);
TAG_FLAG(fault_crash_before_append_commit, runtime);

DEFINE_double(log_inject_io_error_on_append_fraction, 0.0,
              "Fraction of the time when the log will fail to append and return an IOError. "
              "(For testing only!)");
TAG_FLAG(log_inject_io_error_on_append_fraction, unsafe);
TAG_FLAG(log_inject_io_error_on_append_fraction, runtime);

DEFINE_double(log_inject_io_error_on_preallocate_fraction, 0.0,
              "Fraction of the time when the log will fail to preallocate and return an IOError. "
              "(For testing only!)");
TAG_FLAG(log_inject_io_error_on_preallocate_fraction, unsafe);
TAG_FLAG(log_inject_io_error_on_preallocate_fraction, runtime);

DEFINE_int64(fs_wal_dir_reserved_bytes, -1,
             "Number of bytes to reserve on the log directory filesystem for "
             "non-Kudu usage. The default, which is represented by -1, is that "
             "1% of the disk space on each disk will be reserved. Any other "
             "value specified represents the number of bytes reserved and must "
             "be greater than or equal to 0. Explicit percentages to reserve "
             "are not currently supported");
DEFINE_validator(fs_wal_dir_reserved_bytes, [](const char* /*n*/, int64_t v) { return v >= -1; });
TAG_FLAG(fs_wal_dir_reserved_bytes, runtime);
TAG_FLAG(fs_wal_dir_reserved_bytes, evolving);

DECLARE_bool(raft_derived_log_mode);

// Validate that log_min_segments_to_retain >= 1
static bool ValidateLogsToRetain(const char* flagname, int value) {
  if (value >= 1) {
    return true;
  }
  LOG(ERROR) << strings::Substitute("$0 must be at least 1, value $1 is invalid",
                                    flagname, value);
  return false;
}
static bool dummy = gflags::RegisterFlagValidator(
    &FLAGS_log_min_segments_to_retain, &ValidateLogsToRetain);

namespace kudu {
namespace log {

using consensus::CommitMsg;
using consensus::OpId;
using consensus::ReplicateRefPtr;
using env_util::OpenFileForRandom;
using std::shared_ptr;
using std::string;
using std::vector;
using std::unique_ptr;
using strings::Substitute;

// Manages the thread which drains groups of batches from the log's queue and
// appends them to the underlying log instance.
//
// Rather than being a long-running thread, this instead uses a threadpool with
// size 1 to automatically start and stop a thread on demand. When the log
// is idle for some amount of time, no task will be on the thread pool, and thus
// the underlying thread may exit.
//
// The design of submitting tasks to the threadpool is slightly tricky in order
// to achieve group commit and not have to submit one task per appended batch.
// Instead, a generic 'DoWork()' task is used which loops collecting work until
// it finds that it has been idle for a while, at which point the task finishes.
//
// The trick, then, lies in two areas:
//
// 1) after appending a batch, we need to ensure that a task is already running,
//    and if not, start one. This is done in Wake().
//
// 2) when the task finds no more work to do and wants to go idle, it needs to
//    ensure that it doesn't miss a concurrent wake-up. This is done in GoIdle().
//
// See the implementation comments in Wake() and GoIdle() for details.
class Log::AppendThread {
 public:
  explicit AppendThread(Log* log);

  // Initializes the objects and starts the thread pool.
  Status Init();

  // Waits until the last enqueued elements are processed, sets the
  // Appender thread to closing state. If any entries are added to the
  // queue during the process, invoke their callbacks' 'OnFailure()'
  // method.
  void Shutdown();

  // Wake up the appender task, if it is not already running.
  // This should be called after each time that a new entry is
  // appended to the log's queue.
  void Wake();

  bool active() const {
    return base::subtle::NoBarrier_Load(&worker_state_) == WORKER_ACTIVE;
  }

 private:
  // The task submitted to the threadpool which collects batches from the queue
  // and appends them, until it determines that the queue is idle.
  void DoWork();

  // Tries to transition back to WORKER_STOPPED state. If successful, returns true.
  //
  // Otherwise, returns false to indicate that the task should keep running because
  // a new task was enqueued just as we were trying to go idle.
  bool GoIdle();

  // Handle the actual appending of a group of entries. Responsible for deleting the
  // LogEntryBatch* pointers.
  void HandleGroup(vector<LogEntryBatch*> entry_batches);

  string LogPrefix() const;

  Log* const log_;

  // Atomic state machine for whether there is any worker task currently
  // queued or running on append_pool_. See Wake() and GoIdle() for more details.
  enum WorkerState {
    // No worker task is queued or running.
    WORKER_STOPPED,
    // A worker task is queued or running.
    WORKER_ACTIVE
  };
  Atomic32 worker_state_ = WORKER_STOPPED;

  // Pool with a single thread, which handles shutting down the thread
  // when idle.
  gscoped_ptr<ThreadPool> append_pool_;
};


Log::AppendThread::AppendThread(Log *log)
  : log_(log) {
}

Status Log::AppendThread::Init() {
  DCHECK(!append_pool_) << "Already initialized";
  VLOG_WITH_PREFIX(1) << "Starting log append thread";
  RETURN_NOT_OK(ThreadPoolBuilder("wal-append")
                .set_min_threads(0)
                // Only need one thread since we'll only schedule one
                // task at a time.
                .set_max_threads(1)
                // No need for keeping idle threads, since the task itself
                // handles waiting for work while idle.
                .set_idle_timeout(MonoDelta::FromSeconds(0))
                .Build(&append_pool_));
  return Status::OK();
}

void Log::AppendThread::Wake() {
  DCHECK(append_pool_);
  auto old_status = base::subtle::NoBarrier_CompareAndSwap(
      &worker_state_, WORKER_STOPPED, WORKER_ACTIVE);
  if (old_status == WORKER_STOPPED) {
    CHECK_OK(append_pool_->SubmitClosure(Bind(&Log::AppendThread::DoWork, Unretained(this))));
  }
}

bool Log::AppendThread::GoIdle() {
  // Inject latency at key points in this function for the purposes of tests.
  MAYBE_INJECT_RANDOM_LATENCY(FLAGS_log_inject_thread_lifecycle_latency_ms);

  // Stopping is a bit tricky. We have to consider the following race:
  //
  // T1                         AppendThread
  // ------------               -------------
  //                            - state is TRIGGERED
  //                            - BlockingDrainTo returns TimedOut()
  // - queue.Put()
  // - Wake() no-op because
  //   it's already triggered

  // So, we first transition back to STOPPED state, and then re-check to see
  // if there has been something enqueued in the meantime.
  auto old_state = base::subtle::NoBarrier_AtomicExchange(&worker_state_, WORKER_STOPPED);
  DCHECK_EQ(old_state, WORKER_ACTIVE);
  if (log_->entry_queue()->empty()) {
    // Nothing got enqueued, which means there must not have been any missed wakeup.
    // We are now in WORKER_STOPPED state.
    return true;
  }

  MAYBE_INJECT_RANDOM_LATENCY(FLAGS_log_inject_thread_lifecycle_latency_ms);
  // Someone enqueued something. We don't know whether their wakeup was successful
  // or not, but we can just try to transition back to ACTIVE mode here.
  if (base::subtle::NoBarrier_CompareAndSwap(&worker_state_, WORKER_STOPPED, WORKER_ACTIVE)
      == WORKER_STOPPED) {
    // Their wake-up was lost, but we've now marked ourselves as running.
    MAYBE_INJECT_RANDOM_LATENCY(FLAGS_log_inject_thread_lifecycle_latency_ms);
    return false;
  }

  // Their wake-up was successful, meaning that there is another task on the
  // queue behind us now, so we can exit this one.
  MAYBE_INJECT_RANDOM_LATENCY(FLAGS_log_inject_thread_lifecycle_latency_ms);
  return true;
}

void Log::AppendThread::DoWork() {
  DCHECK_EQ(ANNOTATE_UNPROTECTED_READ(worker_state_), WORKER_ACTIVE);
  VLOG_WITH_PREFIX(2) << "WAL Appender going active";
  while (true) {
    CHECK(!FLAGS_raft_derived_log_mode);
    MonoTime deadline = MonoTime::Now() +
        MonoDelta::FromMilliseconds(FLAGS_log_thread_idle_threshold_ms);
    vector<LogEntryBatch*> entry_batches;
    Status s = log_->entry_queue()->BlockingDrainTo(&entry_batches, deadline);
    if (PREDICT_FALSE(s.IsAborted())) {
      break;
    } else if (PREDICT_FALSE(s.IsTimedOut())) {
      if (GoIdle()) break;
      continue;
    }
    HandleGroup(std::move(entry_batches));
  }
  VLOG_WITH_PREFIX(2) << "WAL Appender going idle";
}

void Log::AppendThread::HandleGroup(vector<LogEntryBatch*> entry_batches) {
  CHECK(!FLAGS_raft_derived_log_mode);
  if (log_->metrics_) {
    log_->metrics_->entry_batches_per_group->Increment(entry_batches.size());
  }
  TRACE_EVENT1("log", "batch", "batch_size", entry_batches.size());

  SCOPED_LATENCY_METRIC(log_->metrics_, group_commit_latency);

  bool is_all_commits = true;
  for (LogEntryBatch* entry_batch : entry_batches) {
    TRACE_EVENT_FLOW_END0("log", "Batch", entry_batch);
    Status s = log_->DoAppend(entry_batch);
    if (PREDICT_FALSE(!s.ok())) {
      LOG_WITH_PREFIX(ERROR) << "Error appending to the log: " << s.ToString();
      // TODO(af): If a single transaction fails to append, should we
      // abort all subsequent transactions in this batch or allow
      // them to be appended? What about transactions in future
      // batches?
      if (!entry_batch->callback().is_null()) {
        entry_batch->callback().Run(s);
        entry_batch->callback_.Reset();
      }
    }
    if (is_all_commits && entry_batch->type_ != COMMIT) {
      is_all_commits = false;
    }
  }

  Status s;
  if (!is_all_commits) {
    s = log_->Sync();
  }
  if (PREDICT_FALSE(!s.ok())) {
    LOG_WITH_PREFIX(ERROR) << "Error syncing log: " << s.ToString();
    for (LogEntryBatch* entry_batch : entry_batches) {
      if (!entry_batch->callback().is_null()) {
        entry_batch->callback().Run(s);
      }
      delete entry_batch;
    }
  } else {
    TRACE_EVENT0("log", "Callbacks");
    VLOG_WITH_PREFIX(2) << "Synchronized " << entry_batches.size() << " entry batches";
    SCOPED_WATCH_STACK(100);
    for (LogEntryBatch* entry_batch : entry_batches) {
      if (PREDICT_TRUE(!entry_batch->callback().is_null())) {
        entry_batch->callback().Run(Status::OK());
      }
      // It's important to delete each batch as we see it, because
      // deleting it may free up memory from memory trackers, and the
      // callback of a later batch may want to use that memory.
      delete entry_batch;
    }
  }
}

void Log::AppendThread::Shutdown() {
  log_->entry_queue()->Shutdown();
  if (append_pool_) {
    append_pool_->Wait();
    append_pool_->Shutdown();
  }
}

string Log::AppendThread::LogPrefix() const {
  return log_->LogPrefix();
}

// Return true if the append thread is currently active.
bool Log::append_thread_active_for_tests() const {
  return append_thread_->active();
}


// This task is submitted to allocation_pool_ in order to
// asynchronously pre-allocate new log segments.
void Log::SegmentAllocationTask() {
  CHECK(!FLAGS_raft_derived_log_mode);
  allocation_status_.Set(PreAllocateNewSegment());
}

const Status Log::kLogShutdownStatus(
    Status::ServiceUnavailable("WAL is shutting down", "", ESHUTDOWN));

const uint64_t Log::kInitialLogSegmentSequenceNumber = 0L;

Status Log::Open(const LogOptions &options,
                 FsManager *fs_manager,
                 const std::string& tablet_id,
#ifdef FB_DO_NOT_REMOVE
                 const Schema& schema,
                 uint32_t schema_version,
#endif
                 const scoped_refptr<MetricEntity>& metric_entity,
                 scoped_refptr<Log>* log) {

  string tablet_wal_path = fs_manager->GetTabletWalDir(tablet_id);
  RETURN_NOT_OK(env_util::CreateDirIfMissing(
      fs_manager->env(), tablet_wal_path));

  scoped_refptr<Log> new_log;
  if (options.log_factory) {
    RETURN_NOT_OK(options.log_factory->createLog(options,
        fs_manager, tablet_wal_path,
        tablet_id, metric_entity, &new_log));
  } else {
    new_log = new Log(options, fs_manager,
                     tablet_wal_path,
                     tablet_id,
#ifdef FB_DO_NOT_REMOVE
                     schema,
                     schema_version,
#endif
                     metric_entity);
  }
  RETURN_NOT_OK(new_log->Init());
  log->swap(new_log);
  return Status::OK();
}

Log::Log(LogOptions options, FsManager* fs_manager, string log_path,
         string tablet_id,
#ifdef FB_DO_NOT_REMOVE
         const Schema& schema, uint32_t schema_version,
#endif
         scoped_refptr<MetricEntity> metric_entity)
    : options_(options),
      fs_manager_(fs_manager),
      log_dir_(std::move(log_path)),
      tablet_id_(std::move(tablet_id)),
#ifdef FB_DO_NOT_REMOVE
      schema_(schema),
      schema_version_(schema_version),
#endif
      active_segment_sequence_number_(0),
      log_state_(kLogInitialized),
      max_segment_size_(options_.segment_size_mb * 1024 * 1024),
      entry_batch_queue_(FLAGS_group_commit_queue_size_bytes),
      append_thread_(new AppendThread(this)),
      force_sync_all_(options_.force_fsync_all),
      sync_disabled_(false),
      allocation_state_(kAllocationNotStarted),
      codec_(nullptr),
      metric_entity_(std::move(metric_entity)),
      on_disk_size_(0) {
  CHECK_OK(ThreadPoolBuilder("log-alloc").set_max_threads(1).Build(&allocation_pool_));
  if (metric_entity_) {
    metrics_.reset(new LogMetrics(metric_entity_));
  }
}

Status LogFactory::createLog(LogOptions options,
    FsManager* fs_manager, std::string log_path,
    std::string tablet_id,
    scoped_refptr<MetricEntity> metric_entity,
    scoped_refptr<Log> *new_log) {
  *new_log = new Log(options, fs_manager, log_path,
      tablet_id, metric_entity);
  return Status::OK();
}

Status Log::Init() {
  std::lock_guard<percpu_rwlock> write_lock(state_lock_);
  CHECK_EQ(kLogInitialized, log_state_);
  CHECK(!FLAGS_raft_derived_log_mode);

  // Init the compression codec.
  if (!FLAGS_log_compression_codec.empty()) {
    auto codec_type = GetCompressionCodecType(FLAGS_log_compression_codec);
    if (codec_type != NO_COMPRESSION) {
      RETURN_NOT_OK_PREPEND(GetCompressionCodec(codec_type, &codec_),
                            "could not instantiate compression codec");
    }
  }

  // Init the index
  log_index_.reset(new LogIndex(log_dir_));

  // Reader for previous segments.
  RETURN_NOT_OK(LogReader::Open(fs_manager_,
                                log_index_,
                                tablet_id_,
                                metric_entity_.get(),
                                &reader_));

  // The case where we are continuing an existing log.
  // We must pick up where the previous WAL left off in terms of
  // sequence numbers.
  if (reader_->num_segments() != 0) {
    VLOG_WITH_PREFIX(1) << "Using existing " << reader_->num_segments()
                        << " segments from path: " << fs_manager_->GetWalsRootDir();

    vector<scoped_refptr<ReadableLogSegment> > segments;
    RETURN_NOT_OK(reader_->GetSegmentsSnapshot(&segments));
    active_segment_sequence_number_ = segments.back()->header().sequence_number();
  }

  if (force_sync_all_) {
    KLOG_FIRST_N(INFO, 1) << LogPrefix() << "Log is configured to fsync() on all Append() calls";
  } else {
    KLOG_FIRST_N(INFO, 1) << LogPrefix()
                          << "Log is configured to *not* fsync() on all Append() calls";
  }

  // We always create a new segment when the log starts.
  RETURN_NOT_OK(AsyncAllocateSegment());
  RETURN_NOT_OK(allocation_status_.Get());
  RETURN_NOT_OK(SwitchToAllocatedSegment());

  RETURN_NOT_OK(append_thread_->Init());
  log_state_ = kLogWriting;
  return Status::OK();
}

Status Log::AsyncAllocateSegment() {
  CHECK(!FLAGS_raft_derived_log_mode);
  std::lock_guard<RWMutex> l(allocation_lock_);
  CHECK_EQ(allocation_state_, kAllocationNotStarted);
  allocation_status_.Reset();
  allocation_state_ = kAllocationInProgress;
  RETURN_NOT_OK(allocation_pool_->SubmitClosure(
                  Bind(&Log::SegmentAllocationTask, Unretained(this))));
  return Status::OK();
}

Status Log::CloseCurrentSegment() {
  CHECK(!FLAGS_raft_derived_log_mode);
  if (!footer_builder_.has_min_replicate_index()) {
    VLOG_WITH_PREFIX(1) << "Writing a segment without any REPLICATE message. Segment: "
                        << active_segment_->path();
  }
  VLOG_WITH_PREFIX(2) << "Segment footer for " << active_segment_->path()
                      << ": " << pb_util::SecureShortDebugString(footer_builder_);

  footer_builder_.set_close_timestamp_micros(GetCurrentTimeMicros());
  RETURN_NOT_OK(active_segment_->WriteFooterAndClose(footer_builder_));

  return Status::OK();
}

Status Log::RollOver() {
  CHECK(!FLAGS_raft_derived_log_mode);
  SCOPED_LATENCY_METRIC(metrics_, roll_latency);

  // Check if any errors have occurred during allocation
  RETURN_NOT_OK(allocation_status_.Get());

  DCHECK_EQ(allocation_state(), kAllocationFinished);

  RETURN_NOT_OK(Sync());
  RETURN_NOT_OK(CloseCurrentSegment());

  RETURN_NOT_OK(SwitchToAllocatedSegment());

  LOG_WITH_PREFIX(INFO) << "Rolled over to a new log segment at " << active_segment_->path();
  return Status::OK();
}

Status Log::CreateBatchFromPB(LogEntryTypePB type,
                              unique_ptr<LogEntryBatchPB> entry_batch_pb,
                              unique_ptr<LogEntryBatch>* entry_batch) {
  CHECK(!FLAGS_raft_derived_log_mode);
  int num_ops = entry_batch_pb->entry_size();
  unique_ptr<LogEntryBatch> new_entry_batch(new LogEntryBatch(
      type, std::move(entry_batch_pb), num_ops));
  new_entry_batch->Serialize();
  TRACE("Serialized $0 byte log entry", new_entry_batch->total_size_bytes());

  *entry_batch = std::move(new_entry_batch);
  return Status::OK();
}

Status Log::AsyncAppend(unique_ptr<LogEntryBatch> entry_batch, const StatusCallback& callback) {
  CHECK(!FLAGS_raft_derived_log_mode);
  TRACE_EVENT0("log", "Log::AsyncAppend");

  entry_batch->set_callback(callback);
  TRACE_EVENT_FLOW_BEGIN0("log", "Batch", entry_batch.get());
  if (PREDICT_FALSE(!entry_batch_queue_.BlockingPut(entry_batch.get()))) {
    TRACE_EVENT_FLOW_END0("log", "Batch", entry_batch.get());
    return kLogShutdownStatus;
  }
  append_thread_->Wake();
  entry_batch.release();
  return Status::OK();
}

Status Log::AsyncAppendReplicates(const vector<ReplicateRefPtr>& replicates,
                                  const StatusCallback& callback) {
  CHECK(!FLAGS_raft_derived_log_mode);
  unique_ptr<LogEntryBatchPB> batch_pb = CreateBatchFromAllocatedOperations(replicates);

  unique_ptr<LogEntryBatch> batch;
  RETURN_NOT_OK(CreateBatchFromPB(REPLICATE, std::move(batch_pb), &batch));
  batch->SetReplicates(replicates);
  return AsyncAppend(std::move(batch), callback);
}

Status Log::AsyncAppendCommit(gscoped_ptr<consensus::CommitMsg> commit_msg,
                              const StatusCallback& callback) {
  CHECK(!FLAGS_raft_derived_log_mode);
  MAYBE_FAULT(FLAGS_fault_crash_before_append_commit);

  unique_ptr<LogEntryBatchPB> batch_pb(new LogEntryBatchPB);
  LogEntryPB* entry = batch_pb->add_entry();
  entry->set_type(COMMIT);
  entry->set_allocated_commit(commit_msg.release());

  unique_ptr<LogEntryBatch> entry_batch;
  RETURN_NOT_OK(CreateBatchFromPB(COMMIT, std::move(batch_pb), &entry_batch));
  AsyncAppend(std::move(entry_batch), callback);
  return Status::OK();
}

Status Log::DoAppend(LogEntryBatch* entry_batch) {
  CHECK(!FLAGS_raft_derived_log_mode);
  size_t num_entries = entry_batch->count();
  DCHECK_GT(num_entries, 0) << "Cannot call DoAppend() with zero entries reserved";

  MAYBE_RETURN_FAILURE(FLAGS_log_inject_io_error_on_append_fraction,
                       Status::IOError("Injected IOError in Log::DoAppend()"));

  Slice entry_batch_data = entry_batch->data();
  uint32_t entry_batch_bytes = entry_batch->total_size_bytes();
  // If there is no data to write return OK.
  if (PREDICT_FALSE(entry_batch_bytes == 0)) {
    return Status::OK();
  }

  // if the size of this entry overflows the current segment, get a new one
  if (allocation_state() == kAllocationNotStarted) {
    if ((active_segment_->Size() + entry_batch_bytes + 4) > max_segment_size_) {
      LOG_WITH_PREFIX(INFO) << "Max segment size reached. Starting new segment allocation";
      RETURN_NOT_OK(AsyncAllocateSegment());
      if (!options_.async_preallocate_segments) {
        LOG_SLOW_EXECUTION(WARNING, 50, Substitute("$0Log roll took a long time", LogPrefix())) {
          RETURN_NOT_OK(RollOver());
        }
      }
    }
  } else if (allocation_state() == kAllocationFinished) {
    LOG_SLOW_EXECUTION(WARNING, 50, Substitute("$0Log roll took a long time", LogPrefix())) {
      RETURN_NOT_OK(RollOver());
    }
  } else {
    VLOG_WITH_PREFIX(1) << "Segment allocation already in progress...";
  }

  int64_t start_offset = active_segment_->written_offset();

  LOG_SLOW_EXECUTION(WARNING, 50, Substitute("$0Append to log took a long time", LogPrefix())) {
    SCOPED_LATENCY_METRIC(metrics_, append_latency);
    SCOPED_WATCH_STACK(500);

    RETURN_NOT_OK(active_segment_->WriteEntryBatch(entry_batch_data, codec_));

    // Update the reader on how far it can read the active segment.
    reader_->UpdateLastSegmentOffset(active_segment_->written_offset());

    if (log_hooks_) {
      RETURN_NOT_OK_PREPEND(log_hooks_->PostAppend(), "PostAppend hook failed");
    }
  }

  if (metrics_) {
    metrics_->bytes_logged->IncrementBy(entry_batch_bytes);
  }

  CHECK_OK(UpdateIndexForBatch(*entry_batch, start_offset));
  UpdateFooterForBatch(entry_batch);

  return Status::OK();
}

Status Log::UpdateIndexForBatch(const LogEntryBatch& batch,
                                int64_t start_offset) {
  if (batch.type_ != REPLICATE) {
    return Status::OK();
  }

  for (const LogEntryPB& entry_pb : batch.entry_batch_pb_->entry()) {
    LogIndexEntry index_entry;

    index_entry.op_id = entry_pb.replicate().id();
    index_entry.segment_sequence_number = active_segment_sequence_number_;
    index_entry.offset_in_segment = start_offset;
    RETURN_NOT_OK(log_index_->AddEntry(index_entry));
  }
  return Status::OK();
}

void Log::UpdateFooterForBatch(LogEntryBatch* batch) {
  CHECK(!FLAGS_raft_derived_log_mode);
  footer_builder_.set_num_entries(footer_builder_.num_entries() + batch->count());

  // We keep track of the last-written OpId here.
  // This is needed to initialize Consensus on startup.
  // We also retrieve the opid of the first operation in the batch so that, if
  // we roll over to a new segment, we set the first operation in the footer
  // immediately.
  if (batch->type_ == REPLICATE) {
    // Update the index bounds for the current segment.
    for (const LogEntryPB& entry_pb : batch->entry_batch_pb_->entry()) {
      UpdateFooterForReplicateEntry(entry_pb, &footer_builder_);
    }
  }
}

Status Log::AllocateSegmentAndRollOver() {
  CHECK(!FLAGS_raft_derived_log_mode);
  RETURN_NOT_OK(AsyncAllocateSegment());
  return RollOver();
}

FsManager* Log::GetFsManager() {
  return fs_manager_;
}

Status Log::Sync() {
  CHECK(!FLAGS_raft_derived_log_mode);
  TRACE_EVENT0("log", "Sync");
  SCOPED_LATENCY_METRIC(metrics_, sync_latency);

  if (PREDICT_FALSE(FLAGS_log_inject_latency && !sync_disabled_)) {
    Random r(GetCurrentTimeMicros());
    int sleep_ms = r.Normal(FLAGS_log_inject_latency_ms_mean,
                            FLAGS_log_inject_latency_ms_stddev);
    if (sleep_ms > 0) {
      LOG_WITH_PREFIX(WARNING) << "Injecting " << sleep_ms << "ms of latency in Log::Sync()";
      SleepFor(MonoDelta::FromMilliseconds(sleep_ms));
    }
  }

  if (force_sync_all_ && !sync_disabled_) {
    LOG_SLOW_EXECUTION(WARNING, 50, Substitute("$0Fsync log took a long time", LogPrefix())) {
      RETURN_NOT_OK(active_segment_->Sync());

      if (log_hooks_) {
        RETURN_NOT_OK_PREPEND(log_hooks_->PostSyncIfFsyncEnabled(),
                              "PostSyncIfFsyncEnabled hook failed");
      }
    }
  }

  if (log_hooks_) {
    RETURN_NOT_OK_PREPEND(log_hooks_->PostSync(), "PostSync hook failed");
  }
  return Status::OK();
}

int GetPrefixSizeToGC(RetentionIndexes retention_indexes, const SegmentSequence& segments) {
  CHECK(!FLAGS_raft_derived_log_mode);
  int rem_segs = segments.size();
  int prefix_size = 0;
  for (const scoped_refptr<ReadableLogSegment>& segment : segments) {
    if (rem_segs <= FLAGS_log_min_segments_to_retain) {
      break;
    }

    if (!segment->HasFooter()) break;

    int64_t seg_max_idx = segment->footer().max_replicate_index();
    // If removing this segment would compromise durability, we cannot remove it.
    if (seg_max_idx >= retention_indexes.for_durability) {
      break;
    }

    // Check if removing this segment would compromise the ability to catch up a peer,
    // we should retain it, unless this would break the max_segments flag.
    if (seg_max_idx >= retention_indexes.for_peers &&
        rem_segs <= FLAGS_log_max_segments_to_retain) {
      break;
    }

    prefix_size++;
    rem_segs--;
  }
  return prefix_size;
}

Status Log::GetSegmentsToGCUnlocked(RetentionIndexes retention_indexes,
                                    SegmentSequence* segments_to_gc) const {
  CHECK(!FLAGS_raft_derived_log_mode);
  RETURN_NOT_OK(reader_->GetSegmentsSnapshot(segments_to_gc));
  segments_to_gc->resize(GetPrefixSizeToGC(retention_indexes, *segments_to_gc));
  return Status::OK();
}

Status Log::Append(LogEntryPB* entry) {
  CHECK(!FLAGS_raft_derived_log_mode);
  unique_ptr<LogEntryBatchPB> entry_batch_pb(new LogEntryBatchPB);
  entry_batch_pb->mutable_entry()->AddAllocated(entry);
  LogEntryBatch entry_batch(entry->type(), std::move(entry_batch_pb), 1);
  entry_batch.Serialize();
  Status s = DoAppend(&entry_batch);
  if (s.ok()) {
    s = Sync();
  }
  entry_batch.entry_batch_pb_->mutable_entry()->ExtractSubrange(0, 1, nullptr);
  return s;
}

Status Log::WaitUntilAllFlushed() {
  // In order to make sure we empty the queue we need to use
  // the async api.
  CHECK(!FLAGS_raft_derived_log_mode);
  unique_ptr<LogEntryBatchPB> entry_batch(new LogEntryBatchPB);
  entry_batch->add_entry()->set_type(log::FLUSH_MARKER);
  unique_ptr<LogEntryBatch> reserved_entry_batch;
  RETURN_NOT_OK(CreateBatchFromPB(FLUSH_MARKER, std::move(entry_batch), &reserved_entry_batch));
  Synchronizer s;
  AsyncAppend(std::move(reserved_entry_batch), s.AsStatusCallback());
  return s.Wait();
}

Status Log::TruncateOpsAfter(int64_t index) {
  // In base implementation, truncation is not needed
  // as next_sequential_op_index_ is updated, as an alternative
  // to actual trimming
  return Status::OK();
}

Status Log::GC(RetentionIndexes retention_indexes, int32_t* num_gced) {
  CHECK_GE(retention_indexes.for_durability, 0);
  CHECK(!FLAGS_raft_derived_log_mode);

  VLOG_WITH_PREFIX(1) << "Running Log GC on " << log_dir_ << ": retaining "
      "ops >= " << retention_indexes.for_durability << " for durability, "
      "ops >= " << retention_indexes.for_peers << " for peers";
  VLOG_TIMING(1, Substitute("$0Log GC", LogPrefix())) {
    SegmentSequence segments_to_delete;

    {
      std::lock_guard<percpu_rwlock> l(state_lock_);
      CHECK_EQ(kLogWriting, log_state_);

      RETURN_NOT_OK(GetSegmentsToGCUnlocked(retention_indexes, &segments_to_delete));

      if (segments_to_delete.empty()) {
        VLOG_WITH_PREFIX(1) << "No segments to delete.";
        *num_gced = 0;
        return Status::OK();
      }
      // Trim the prefix of segments from the reader so that they are no longer
      // referenced by the log.
      RETURN_NOT_OK(reader_->TrimSegmentsUpToAndIncluding(
          segments_to_delete[segments_to_delete.size() - 1]->header().sequence_number()));
    }

    // Now that they are no longer referenced by the Log, delete the files.
    *num_gced = 0;
    for (const scoped_refptr<ReadableLogSegment>& segment : segments_to_delete) {
      string ops_str;
      if (segment->HasFooter() && segment->footer().has_min_replicate_index()) {
        DCHECK(segment->footer().has_max_replicate_index());
        ops_str = Substitute(" (ops $0-$1)",
                             segment->footer().min_replicate_index(),
                             segment->footer().max_replicate_index());
      }
      LOG_WITH_PREFIX(INFO) << "Deleting log segment in path: " << segment->path() << ops_str;
      RETURN_NOT_OK(fs_manager_->env()->DeleteFile(segment->path()));
      (*num_gced)++;
    }

    // Determine the minimum remaining replicate index in order to properly GC
    // the index chunks.
    int64_t min_remaining_op_idx = reader_->GetMinReplicateIndex();
    if (min_remaining_op_idx > 0) {
      log_index_->GC(min_remaining_op_idx);
    }
  }
  return Status::OK();
}

int64_t Log::GetGCableDataSize(RetentionIndexes retention_indexes) const {
  CHECK(!FLAGS_raft_derived_log_mode);
  CHECK_GE(retention_indexes.for_durability, 0);
  SegmentSequence segments_to_delete;
  {
    shared_lock<rw_spinlock> l(state_lock_.get_lock());
    CHECK_EQ(kLogWriting, log_state_);
    Status s = GetSegmentsToGCUnlocked(retention_indexes, &segments_to_delete);

    if (!s.ok() || segments_to_delete.empty()) {
      return 0;
    }
  }
  int64_t total_size = 0;
  for (const scoped_refptr<ReadableLogSegment>& segment : segments_to_delete) {
    total_size += segment->file_size();
  }
  return total_size;
}

void Log::GetReplaySizeMap(std::map<int64_t, int64_t>* replay_size) const {
  CHECK(!FLAGS_raft_derived_log_mode);
  replay_size->clear();
  SegmentSequence segments;
  {
    shared_lock<rw_spinlock> l(state_lock_.get_lock());
    CHECK_EQ(kLogWriting, log_state_);
    CHECK_OK(reader_->GetSegmentsSnapshot(&segments));
  }

  int64_t cumulative_size = 0;
  for (const auto& segment : boost::adaptors::reverse(segments)) {
    if (!segment->HasFooter()) continue;
    cumulative_size += segment->file_size();
    int64_t max_repl_idx = segment->footer().max_replicate_index();
    (*replay_size)[max_repl_idx] = cumulative_size;
  }
}

int64_t Log::OnDiskSize() {
  CHECK(!FLAGS_raft_derived_log_mode);
  SegmentSequence segments;
  {
    shared_lock<rw_spinlock> l(state_lock_.get_lock());
    // If the log is closed, the tablet is either being deleted or tombstoned,
    // so we don't count the size of its log anymore as it should be deleted.
    if (log_state_ == kLogClosed || !reader_->GetSegmentsSnapshot(&segments).ok()) {
      return on_disk_size_.load();
    }
  }
  int64_t ret = 0;
  for (const auto& segment : segments) {
    ret += segment->file_size();
  }

  on_disk_size_.store(ret, std::memory_order_relaxed);
  return ret;
}

#ifdef FB_DO_NOT_REMOVE

void Log::SetSchemaForNextLogSegment(const Schema& schema,
                                     uint32_t version) {
  std::lock_guard<rw_spinlock> l(schema_lock_);
  schema_ = schema;
  schema_version_ = version;
}
#endif

Status Log::ReadReplicatesInRange(
    int64_t starting_at,
    int64_t up_to,
    int64_t max_bytes_to_read,
    std::vector<consensus::ReplicateMsg*>* replicates) const {
  return reader()->ReadReplicatesInRange(
      starting_at, up_to, max_bytes_to_read, replicates);
}

Status Log::LookupOpId(int64_t op_index, OpId* op_id) const {
  return reader()->LookupOpId(op_index, op_id);
}

Status Log::Close() {
  CHECK(!FLAGS_raft_derived_log_mode);
  allocation_pool_->Shutdown();
  append_thread_->Shutdown();

  std::lock_guard<percpu_rwlock> l(state_lock_);
  switch (log_state_) {
    case kLogWriting:
      if (log_hooks_) {
        RETURN_NOT_OK_PREPEND(log_hooks_->PreClose(),
                              "PreClose hook failed");
      }
      RETURN_NOT_OK(Sync());
      RETURN_NOT_OK(CloseCurrentSegment());
      RETURN_NOT_OK(ReplaceSegmentInReaderUnlocked());
      log_state_ = kLogClosed;
      VLOG_WITH_PREFIX(1) << "Log closed";

      // Release FDs held by these objects.
      log_index_.reset();
      reader_.reset();

      if (log_hooks_) {
        RETURN_NOT_OK_PREPEND(log_hooks_->PostClose(),
                              "PostClose hook failed");
      }
      return Status::OK();

    case kLogClosed:
      VLOG_WITH_PREFIX(1) << "Log already closed";
      return Status::OK();

    default:
      return Status::IllegalState(Substitute("Log not open. State: $0", log_state_));
  }
}

bool Log::HasOnDiskData(FsManager* fs_manager, const string& tablet_id) {
  string wal_dir = fs_manager->GetTabletWalDir(tablet_id);
  return fs_manager->env()->FileExists(wal_dir);
}

Status Log::DeleteOnDiskData(FsManager* fs_manager, const string& tablet_id) {
  CHECK(!FLAGS_raft_derived_log_mode);
  string wal_dir = fs_manager->GetTabletWalDir(tablet_id);
  Env* env = fs_manager->env();
  if (!env->FileExists(wal_dir)) {
    return Status::OK();
  }
  LOG(INFO) << Substitute("T $0 P $1: Deleting WAL directory at $2",
                          tablet_id, fs_manager->uuid(), wal_dir);
  RETURN_NOT_OK_PREPEND(env->DeleteRecursively(wal_dir),
                        "Unable to recursively delete WAL dir for tablet " + tablet_id);
  return Status::OK();
}

Status Log::RemoveRecoveryDirIfExists(FsManager* fs_manager, const string& tablet_id) {
  CHECK(!FLAGS_raft_derived_log_mode);
  string recovery_path = fs_manager->GetTabletWalRecoveryDir(tablet_id);
  const auto kLogPrefix = Substitute("T $0 P $1: ", tablet_id, fs_manager->uuid());
  if (!fs_manager->Exists(recovery_path)) {
    VLOG(1) << kLogPrefix << "Tablet WAL recovery dir " << recovery_path <<
            " does not exist.";
    return Status::OK();
  }

  VLOG(1) << kLogPrefix << "Preparing to delete log recovery files and directory " << recovery_path;

  string tmp_path = Substitute("$0-$1", recovery_path, GetCurrentTimeMicros());
  VLOG(1) << kLogPrefix << "Renaming log recovery dir from "  << recovery_path
          << " to " << tmp_path;
  RETURN_NOT_OK_PREPEND(fs_manager->env()->RenameFile(recovery_path, tmp_path),
                        Substitute("Could not rename old recovery dir from: $0 to: $1",
                                   recovery_path, tmp_path));

  if (FLAGS_skip_remove_old_recovery_dir) {
    LOG(INFO) << kLogPrefix << "--skip_remove_old_recovery_dir enabled. NOT deleting " << tmp_path;
    return Status::OK();
  }
  VLOG(1) << kLogPrefix << "Deleting all files from renamed log recovery directory " << tmp_path;
  RETURN_NOT_OK_PREPEND(fs_manager->env()->DeleteRecursively(tmp_path),
                        "Could not remove renamed recovery dir " + tmp_path);
  VLOG(1) << kLogPrefix << "Completed deletion of old log recovery files and directory "
          << tmp_path;
  return Status::OK();
}

Status Log::PreAllocateNewSegment() {
  CHECK(!FLAGS_raft_derived_log_mode);
  TRACE_EVENT1("log", "PreAllocateNewSegment", "file", next_segment_path_);
  CHECK_EQ(allocation_state(), kAllocationInProgress);

  // We must mark allocation as finished when returning from this method.
  auto alloc_finished = MakeScopedCleanup([&] () {
    std::lock_guard<RWMutex> l(allocation_lock_);
    allocation_state_ = kAllocationFinished;
  });

  WritableFileOptions opts;
  opts.sync_on_close = force_sync_all_;
  RETURN_NOT_OK(CreatePlaceholderSegment(opts, &next_segment_path_, &next_segment_file_));

  MAYBE_RETURN_FAILURE(FLAGS_log_inject_io_error_on_preallocate_fraction,
                       Status::IOError("Injected IOError in Log::PreAllocateNewSegment()"));

  if (options_.preallocate_segments) {
    TRACE("Preallocating $0 byte segment in $1", max_segment_size_, next_segment_path_);
    RETURN_NOT_OK(env_util::VerifySufficientDiskSpace(fs_manager_->env(),
                                                      next_segment_path_,
                                                      max_segment_size_,
                                                      FLAGS_fs_wal_dir_reserved_bytes));
    // TODO (perf) zero the new segments -- this could result in
    // additional performance improvements.
    RETURN_NOT_OK(next_segment_file_->PreAllocate(max_segment_size_));
  }

  return Status::OK();
}

Status Log::SwitchToAllocatedSegment() {
  CHECK(!FLAGS_raft_derived_log_mode);
  CHECK_EQ(allocation_state(), kAllocationFinished);

  // Increment "next" log segment seqno.
  active_segment_sequence_number_++;

  string new_segment_path = fs_manager_->GetWalSegmentFileName(tablet_id_,
                                                               active_segment_sequence_number_);

  RETURN_NOT_OK(fs_manager_->env()->RenameFile(next_segment_path_, new_segment_path));
  if (force_sync_all_) {
    RETURN_NOT_OK(fs_manager_->env()->SyncDir(log_dir_));
  }

  // Create a new segment.
  gscoped_ptr<WritableLogSegment> new_segment(
      new WritableLogSegment(new_segment_path, next_segment_file_));

  // Set up the new header and footer.
  LogSegmentHeaderPB header;
  header.set_sequence_number(active_segment_sequence_number_);
  header.set_tablet_id(tablet_id_);

  if (codec_) {
    header.set_compression_codec(codec_->type());
  }

  // Set up the new footer. This will be maintained as the segment is written.
  footer_builder_.Clear();
  footer_builder_.set_num_entries(0);


#ifdef FB_DO_NOT_REMOVE
  // Set the new segment's schema.
  {
    shared_lock<rw_spinlock> l(schema_lock_);
    RETURN_NOT_OK(SchemaToPB(schema_, header.mutable_schema()));
    header.set_schema_version(schema_version_);
  }
#endif

  RETURN_NOT_OK(new_segment->WriteHeaderAndOpen(header));

  // Transform the currently-active segment into a readable one, since we
  // need to be able to replay the segments for other peers.
  {
    if (active_segment_.get() != nullptr) {
      std::lock_guard<percpu_rwlock> l(state_lock_);
      CHECK_OK(ReplaceSegmentInReaderUnlocked());
    }
  }

  // Open the segment we just created in readable form and add it to the reader.
  // TODO(todd): consider using a global FileCache here? With short log segments and
  // lots of tablets, this file descriptor usage may add up.
  unique_ptr<RandomAccessFile> readable_file;

  RandomAccessFileOptions opts;
  RETURN_NOT_OK(fs_manager_->env()->NewRandomAccessFile(opts, new_segment_path, &readable_file));
  scoped_refptr<ReadableLogSegment> readable_segment(
    new ReadableLogSegment(new_segment_path,
                           shared_ptr<RandomAccessFile>(readable_file.release())));
  RETURN_NOT_OK(readable_segment->Init(header, new_segment->first_entry_offset()));
  RETURN_NOT_OK(reader_->AppendEmptySegment(readable_segment));

  // Now set 'active_segment_' to the new segment.
  active_segment_.reset(new_segment.release());

  allocation_state_ = kAllocationNotStarted;

  return Status::OK();
}

Status Log::ReplaceSegmentInReaderUnlocked() {
  CHECK(!FLAGS_raft_derived_log_mode);
  // We should never switch to a new segment if we wrote nothing to the old one.
  CHECK(active_segment_->IsClosed());
  shared_ptr<RandomAccessFile> readable_file;
  RETURN_NOT_OK(OpenFileForRandom(fs_manager_->env(), active_segment_->path(), &readable_file));
  scoped_refptr<ReadableLogSegment> readable_segment(
      new ReadableLogSegment(active_segment_->path(),
                             readable_file));
  // Note: active_segment_->header() will only contain an initialized PB if we
  // wrote the header out.
  RETURN_NOT_OK(readable_segment->Init(active_segment_->header(),
                                       active_segment_->footer(),
                                       active_segment_->first_entry_offset()));

  return reader_->ReplaceLastSegment(readable_segment);
}

Status Log::CreatePlaceholderSegment(const WritableFileOptions& opts,
                                     string* result_path,
                                     shared_ptr<WritableFile>* out) {
  CHECK(!FLAGS_raft_derived_log_mode);
  string tmp_suffix = strings::Substitute("$0$1", kTmpInfix, ".newsegmentXXXXXX");
  string path_tmpl = JoinPathSegments(log_dir_, tmp_suffix);
  VLOG_WITH_PREFIX(2) << "Creating temp. file for place holder segment, template: " << path_tmpl;
  unique_ptr<WritableFile> segment_file;
  RETURN_NOT_OK(fs_manager_->env()->NewTempWritableFile(opts,
                                                        path_tmpl,
                                                        result_path,
                                                        &segment_file));
  VLOG_WITH_PREFIX(1) << "Created next WAL segment, placeholder path: " << *result_path;
  out->reset(segment_file.release());
  return Status::OK();
}

std::string Log::LogPrefix() const {
  return Substitute("T $0 P $1: ", tablet_id_, fs_manager_->uuid());
}

Log::~Log() {
  WARN_NOT_OK(Close(), "Error closing log");
}

LogEntryBatch::LogEntryBatch(LogEntryTypePB type,
                             unique_ptr<LogEntryBatchPB> entry_batch_pb,
                             size_t count)
    : type_(type),
      entry_batch_pb_(std::move(entry_batch_pb)),
      total_size_bytes_(
          PREDICT_FALSE(count == 1 && entry_batch_pb_->entry(0).type() == FLUSH_MARKER) ?
          0 : entry_batch_pb_->ByteSize()),
      count_(count) {
}

LogEntryBatch::~LogEntryBatch() {
  if (type_ == REPLICATE && entry_batch_pb_) {
    for (LogEntryPB& entry : *entry_batch_pb_->mutable_entry()) {
      // ReplicateMsg elements are owned by and must be freed by the caller
      // (e.g. the LogCache).
      entry.release_replicate();
    }
  }
}

void LogEntryBatch::Serialize() {
  DCHECK_EQ(buffer_.size(), 0);
  // FLUSH_MARKER LogEntries are markers and are not serialized.
  if (PREDICT_FALSE(count() == 1 && entry_batch_pb_->entry(0).type() == FLUSH_MARKER)) {
    return;
  }
  buffer_.reserve(total_size_bytes_);
  pb_util::AppendToString(*entry_batch_pb_, &buffer_);
}


}  // namespace log
}  // namespace kudu
