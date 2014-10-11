// Copyright (c) 2013, Cloudera, inc.

#include "kudu/consensus/log.h"

#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/log_metrics.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/log_util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/coding.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/env_util.h"
#include "kudu/util/kernel_stack_watchdog.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/thread.h"
#include "kudu/util/trace.h"
#include "kudu/util/stopwatch.h"

DEFINE_int32(group_commit_queue_size_bytes, 4 * 1024 * 1024,
             "Maximum size of the group commit queue in bytes");

static const char kSegmentPlaceholderFileTemplate[] = ".tmp.newsegmentXXXXXX";

namespace kudu {
namespace log {

using consensus::CommitMsg;
using consensus::OpId;
using env_util::OpenFileForRandom;
using strings::Substitute;
using std::tr1::shared_ptr;

// This class is responsible for managing the thread that appends to
// the log file.
class Log::AppendThread {
 public:
  explicit AppendThread(Log* log);

  // Initializes the objects and starts the thread.
  Status Init();

  // Waits until the last enqueued elements are processed, sets the
  // Appender thread to closing state. If any entries are added to the
  // queue during the process, invoke their callbacks' 'OnFailure()'
  // method.
  void Shutdown();

  bool closing() const {
    boost::lock_guard<boost::mutex> lock_guard(lock_);
    return closing_;
  }

 private:
  void RunThread();

  mutable boost::mutex lock_;
  Log* log_;
  bool closing_;
  scoped_refptr<kudu::Thread> thread_;
};


Log::AppendThread::AppendThread(Log *log)
  : log_(log),
    closing_(false) {
}

Status Log::AppendThread::Init() {
  DCHECK(thread_.get() == NULL) << "Already initialized";
  VLOG(1) << "Starting log append thread for tablet " << log_->tablet_id();
  RETURN_NOT_OK(kudu::Thread::Create("log", "appender",
      &AppendThread::RunThread, this, &thread_));
  return Status::OK();
}

void Log::PostponeEarlyCommits(vector<LogEntryBatch*>* batches) {
  vector<LogEntryBatch*> ret_batches;
  ret_batches.reserve(batches->size());

  OpId max_repl_id = last_entry_op_id_;

  // First pull out any COMMIT messages into the postponed list.
  // It would be possible to implement this in-place and avoid some
  // allocation, but the code's more complex. Consider doing that if
  // this looks like a perf hot-spot in the future.
  BOOST_FOREACH(LogEntryBatch* batch, *batches) {
    switch (batch->type_) {
      case COMMIT:
      {
        DCHECK_EQ(1, batch->count())
          << "Currently don't support multiple COMMITs in a batch";

        const OpId& committed = batch->entry_batch_pb_->entry(0).commit().commited_op_id();
        if (OpIdCompare(committed, max_repl_id) <= 0) {
          // If the commit is for an operation that we've already replicated (or already
          // seen in this loop), we can append it in its original position.
          ret_batches.push_back(batch);
        } else {
          // Otherwise we have to postpone it.
          postponed_commit_batches_.push_back(batch);
        }
        break;
      }
      case REPLICATE:
        max_repl_id = batch->MaxReplicateOpId();
        FALLTHROUGH_INTENDED;
      default:
        ret_batches.push_back(batch);
        break;
    }
  }

  // Now go through the postponed list and re-add any batches which
  // are now able to be processed, removing them from the postponed list.
  for (std::list<LogEntryBatch*>::iterator iter = postponed_commit_batches_.begin();
       iter != postponed_commit_batches_.end();) {
    LogEntryBatch* postponed = *iter;

    const OpId& committed = postponed->entry_batch_pb_->entry(0).commit().commited_op_id();
    if (OpIdCompare(committed, max_repl_id) <= 0) {
      ret_batches.push_back(postponed);
      iter = postponed_commit_batches_.erase(iter);
    } else {
      ++iter;
    }
  }
  batches->swap(ret_batches);
}

void Log::AppendThread::RunThread() {
  LogEntryBatchQueue* queue = log_->entry_queue();
  while (PREDICT_TRUE(!closing())) {
    std::vector<LogEntryBatch*> entry_batches;
    ElementDeleter d(&entry_batches);

    if (PREDICT_FALSE(!queue->BlockingDrainTo(&entry_batches))) {
      break;
    }

    if (log_->metrics_) {
      log_->metrics_->entry_batches_per_group->Increment(entry_batches.size());
    }

    log_->PostponeEarlyCommits(&entry_batches);

    SCOPED_LATENCY_METRIC(log_->metrics_, group_commit_latency);

    BOOST_FOREACH(LogEntryBatch* entry_batch, entry_batches) {
      entry_batch->WaitForReady();
      Status s = log_->DoAppend(entry_batch);
      if (PREDICT_FALSE(!s.ok())) {
        LOG(ERROR) << "Error appending to the log: " << s.ToString();
        DLOG(FATAL) << "Aborting: " << s.ToString();
        entry_batch->set_failed_to_append();
        // TODO If a single transaction fails to append, should we
        // abort all subsequent transactions in this batch or allow
        // them to be appended? What about transactions in future
        // batches?
        if (!entry_batch->callback().is_null()) {
          entry_batch->callback().Run(s);
        }
      }
    }

    Status s = log_->Sync();
    if (PREDICT_FALSE(!s.ok())) {
      LOG(ERROR) << "Error syncing log" << s.ToString();
      DLOG(FATAL) << "Aborting: " << s.ToString();
      BOOST_FOREACH(LogEntryBatch* entry_batch, entry_batches) {
        if (!entry_batch->callback().is_null()) {
          entry_batch->callback().Run(s);
        }
      }
    } else {
      VLOG(2) << "Synchronized " << entry_batches.size() << " entry batches";
      BOOST_FOREACH(LogEntryBatch* entry_batch, entry_batches) {
        if (PREDICT_TRUE(!entry_batch->failed_to_append()
                         && !entry_batch->callback().is_null())) {
          entry_batch->callback().Run(Status::OK());
        }
      }
    }
  }
  VLOG(1) << "Exiting AppendThread for tablet " << log_->tablet_id();
}

void Log::AppendThread::Shutdown() {
  {
    boost::lock_guard<boost::mutex> lock_guard(lock_);
    if (closing_) {
      return;
    }
    closing_ = true;
  }

  VLOG(1) << "Shutting down log append thread for tablet " << log_->tablet_id();

  LogEntryBatchQueue* queue = log_->entry_queue();
  queue->Shutdown();
  if (thread_) {
    CHECK_OK(ThreadJoiner(thread_.get()).Join());
    thread_.reset();
  }

  VLOG(1) << "Log append thread for tablet " << log_->tablet_id() << " is shut down";
}

// This task is submitted to allocation_executor_ in order to
// asynchronously pre-allocate new log segments.
class Log::SegmentAllocationTask : public Task {
 public:
  explicit SegmentAllocationTask(Log* log)
    : log_(log) {
  }

  Status Run() OVERRIDE {
    RETURN_NOT_OK(log_->PreAllocateNewSegment());
    return Status::OK();
  }

  bool Abort() OVERRIDE { return false; }
 private:
  Log* log_;
  DISALLOW_COPY_AND_ASSIGN(SegmentAllocationTask);
};

const Status Log::kLogShutdownStatus(
    Status::ServiceUnavailable("WAL is shutting down", "", ESHUTDOWN));

const uint64_t Log::kInitialLogSegmentSequenceNumber = 0L;

Status Log::Open(const LogOptions &options,
                 FsManager *fs_manager,
                 const std::string& tablet_id,
                 const Schema& schema,
                 MetricContext* parent_metrics_context,
                 gscoped_ptr<Log> *log) {

  RETURN_NOT_OK(fs_manager->CreateDirIfMissing(fs_manager->GetWalsRootDir()));
  string tablet_wal_path = fs_manager->GetTabletWalDir(tablet_id);
  RETURN_NOT_OK(fs_manager->CreateDirIfMissing(tablet_wal_path));

  gscoped_ptr<Log> new_log(new Log(options,
                                   fs_manager,
                                   tablet_wal_path,
                                   tablet_id,
                                   schema,
                                   parent_metrics_context));
  RETURN_NOT_OK(new_log->Init());
  log->reset(new_log.release());
  return Status::OK();
}

Log::Log(const LogOptions &options,
         FsManager *fs_manager,
         const string& log_path,
         const string& tablet_id,
         const Schema& schema,
         MetricContext* parent_metric_context)
  : options_(options),
    fs_manager_(fs_manager),
    log_dir_(log_path),
    tablet_id_(tablet_id),
    schema_(schema),
    active_segment_sequence_number_(0),
    log_state_(kLogInitialized),
    total_ops_in_last_seg_(0),
    max_segment_size_(options_.segment_size_mb * 1024 * 1024),
    entry_batch_queue_(FLAGS_group_commit_queue_size_bytes),
    append_thread_(new AppendThread(this)),
    force_sync_all_(options_.force_fsync_all),
    allocation_state_(kAllocationNotStarted) {
  CHECK_OK(TaskExecutorBuilder("log-alloc").set_max_threads(1).Build(&allocation_executor_));
  if (parent_metric_context) {
    metric_context_.reset(new MetricContext(*parent_metric_context,
                                            strings::Substitute("log.tablet-$0", tablet_id)));
    metrics_.reset(new LogMetrics(*metric_context_));
  }
}

Status Log::Init() {
  boost::lock_guard<percpu_rwlock> write_lock(state_lock_);
  CHECK_EQ(kLogInitialized, log_state_);

  // Reader for previous segments.
  RETURN_NOT_OK(LogReader::Open(fs_manager_,
                                tablet_id_,
                                &reader_));

  // The case where we are continuing an existing log.
  // We must pick up where the previous WAL left off in terms of
  // sequence numbers.
  if (reader_->num_segments() != 0) {
    VLOG(1) << "Using existing " << reader_->num_segments()
            << " segments from path: " << fs_manager_->GetWalsRootDir();

    vector<scoped_refptr<ReadableLogSegment> > segments;
    RETURN_NOT_OK(reader_->GetSegmentsSnapshot(&segments));
    active_segment_sequence_number_ = segments.back()->header().sequence_number();
  }

  if (force_sync_all_) {
    KLOG_FIRST_N(INFO, 1) << "Log is configured to fsync() on all Append() calls";
  } else {
    KLOG_FIRST_N(INFO, 1) << "Log is configured to *not* fsync() on all Append() calls";
  }

  // We always create a new segment when the log starts.
  RETURN_NOT_OK(AsyncAllocateSegment());
  allocation_future_->Wait();
  RETURN_NOT_OK(allocation_future_->status());
  RETURN_NOT_OK(SwitchToAllocatedSegment());

  RETURN_NOT_OK(append_thread_->Init());
  log_state_ = kLogWriting;
  return Status::OK();
}

Status Log::AsyncAllocateSegment() {
  boost::lock_guard<boost::shared_mutex> lock_guard(allocation_lock_);
  CHECK_EQ(allocation_state_, kAllocationNotStarted);
  allocation_future_.reset();
  shared_ptr<Task> allocation_task(new SegmentAllocationTask(this));
  allocation_state_ = kAllocationInProgress;
  RETURN_NOT_OK(allocation_executor_->Submit(allocation_task, &allocation_future_));
  return Status::OK();
}

Status Log::CloseCurrentSegment() {
  LogSegmentFooterPB footer;

  if (!first_op_in_seg_.IsInitialized()) {
    VLOG(1) << "Writing a segment without any REPLICATE message. "
        "Segment: " << active_segment_->path();
  } else {
    footer.add_idx_entry()->CopyFrom(first_op_in_seg_);
  }

  footer.set_num_entries(total_ops_in_last_seg_);
  RETURN_NOT_OK(active_segment_->WriteFooterAndClose(footer));

  total_ops_in_last_seg_ = 0;
  first_op_in_seg_.Clear();

  return Status::OK();
}

Status Log::RollOver() {
  SCOPED_LATENCY_METRIC(metrics_, roll_latency);

  // Check if any errors have occurred during allocation
  allocation_future_->Wait();
  RETURN_NOT_OK(allocation_future_->status());

  DCHECK_EQ(allocation_state(), kAllocationFinished);

  RETURN_NOT_OK(Sync());
  RETURN_NOT_OK(CloseCurrentSegment());

  RETURN_NOT_OK(SwitchToAllocatedSegment());

  LOG(INFO) << "Rolled over to a new segment: " << active_segment_->path();
  return Status::OK();
}

Status Log::Reserve(LogEntryTypePB type,
                    gscoped_ptr<LogEntryBatchPB> entry_batch,
                    LogEntryBatch** reserved_entry) {
  DCHECK(reserved_entry != NULL);
  {
    boost::shared_lock<rw_spinlock> read_lock(state_lock_.get_lock());
    CHECK_EQ(kLogWriting, log_state_);
  }

  // In DEBUG builds, verify that all of the entries in the batch match the specified type.
  // In non-debug builds the foreach loop gets optimized out.
  #ifndef NDEBUG
  BOOST_FOREACH(const LogEntryPB& entry, entry_batch->entry()) {
    DCHECK_EQ(entry.type(), type) << "Bad batch: " << entry_batch->DebugString();
  }
  #endif

  int num_ops = entry_batch->entry_size();
  gscoped_ptr<LogEntryBatch> new_entry_batch(new LogEntryBatch(type, entry_batch.Pass(), num_ops));
  new_entry_batch->MarkReserved();

  if (PREDICT_FALSE(!entry_batch_queue_.BlockingPut(new_entry_batch.get()))) {
    return kLogShutdownStatus;
  }

  // Release the memory back to the caller: this will be freed when
  // the entry is removed from the queue.
  //
  // TODO (perf) Use a ring buffer instead of a blocking queue and set
  // 'reserved_entry' to a pre-allocated slot in the buffer.
  *reserved_entry = new_entry_batch.release();
  return Status::OK();
}

Status Log::AsyncAppend(LogEntryBatch* entry_batch, const StatusCallback& callback) {
  {
    boost::shared_lock<rw_spinlock> read_lock(state_lock_.get_lock());
    CHECK_EQ(kLogWriting, log_state_);
  }

  RETURN_NOT_OK(entry_batch->Serialize());
  entry_batch->set_callback(callback);
  TRACE("Serialized $0 byte log entry", entry_batch->total_size_bytes());
  entry_batch->MarkReady();

  return Status::OK();
}

Status Log::AsyncAppendReplicates(const consensus::ReplicateMsg* const* msgs,
                                  int num_msgs,
                                  const StatusCallback& callback) {
  gscoped_ptr<LogEntryBatchPB> batch;
  CreateBatchFromAllocatedOperations(msgs, num_msgs, &batch);

  LogEntryBatch* reserved_entry_batch;
  RETURN_NOT_OK(Reserve(REPLICATE, batch.Pass(), &reserved_entry_batch));

  RETURN_NOT_OK(AsyncAppend(reserved_entry_batch, callback));
  return Status::OK();
}

Status Log::AsyncAppendCommit(gscoped_ptr<consensus::CommitMsg> commit_msg,
                              const StatusCallback& callback) {

  gscoped_ptr<LogEntryBatchPB> batch(new LogEntryBatchPB);
  LogEntryPB* entry = batch->add_entry();
  entry->set_type(COMMIT);
  entry->set_allocated_commit(commit_msg.release());

  LogEntryBatch* reserved_entry_batch;
  RETURN_NOT_OK(Reserve(COMMIT, batch.Pass(), &reserved_entry_batch));

  RETURN_NOT_OK(AsyncAppend(reserved_entry_batch, callback));
  return Status::OK();
}

Status Log::DoAppend(LogEntryBatch* entry_batch, bool caller_owns_operation) {
  size_t num_entries = entry_batch->count();
  DCHECK_GT(num_entries, 0) << "Cannot call DoAppend() with zero entries reserved";

  Slice entry_batch_data = entry_batch->data();
  uint32_t entry_batch_bytes = entry_batch->total_size_bytes();
  // If there is no data to write return OK.
  if (PREDICT_FALSE(entry_batch_bytes == 0)) {
    return Status::OK();
  }

  // We keep track of the last-written OpId here.
  // This is needed to initialize Consensus on startup.
  // We also retrieve the opid of the first operation in the batch so that, if
  // we roll over to a new segment, we set the first operation in the footer
  // immediately.
  OpId first_opid_in_batch;
  if (entry_batch->type_ == REPLICATE) {
    first_opid_in_batch.CopyFrom(entry_batch->MinReplicateOpId());

    // TODO Probably remove the code below as it looks suspicious: Tablet peer uses this
    // as 'safe' anchor as it believes it in the log, when it actually isn't, i.e. this
    // is not the last durable operation. Either move this to tablet peer (since we're
    // using in flights anyway no need to scan for ids here) or actually delay doing this
    // until fsync() has been done. See KUDU-527.
    boost::lock_guard<rw_spinlock> write_lock(last_entry_op_id_lock_);
    last_entry_op_id_.CopyFrom(entry_batch->MaxReplicateOpId());
  }

  // For REPLICATE batches, we expect the caller to free the actual entries if
  // caller_owns_operation is set.
  if (entry_batch->type_ == REPLICATE && caller_owns_operation) {
    for (int i = 0; i < entry_batch->entry_batch_pb_->entry_size(); i++) {
      LogEntryPB* entry_pb = entry_batch->entry_batch_pb_->mutable_entry(i);
      entry_pb->release_replicate();
    }
  }

  if (metrics_) {
    metrics_->bytes_logged->IncrementBy(entry_batch_bytes);
  }

  // if the size of this entry overflows the current segment, get a new one
  if (allocation_state() == kAllocationNotStarted) {
    if ((active_segment_->Size() + entry_batch_bytes + 4) > max_segment_size_) {
      LOG(INFO) << "Max segment size reached. Starting new segment allocation. ";
      RETURN_NOT_OK(AsyncAllocateSegment());
      if (!options_.async_preallocate_segments) {
        LOG_SLOW_EXECUTION(WARNING, 50, "Log roll took a long time") {
          RETURN_NOT_OK(RollOver());
        }
      }
    }
  } else if (allocation_state() == kAllocationFinished) {
    LOG_SLOW_EXECUTION(WARNING, 50, "Log roll took a long time") {
      RETURN_NOT_OK(RollOver());
    }
  } else {
    VLOG(1) << "Segment allocation already in progress...";
  }

  // If we haven't seen a REPLICATE in this segment yet, add the first REPLICATE op's id
  // to the footer.
  if (!first_op_in_seg_.IsInitialized() && entry_batch->type_ == REPLICATE) {
    first_op_in_seg_.mutable_id()->CopyFrom(first_opid_in_batch);
  }

  LOG_SLOW_EXECUTION(WARNING, 50, "Append to log took a long time") {
    SCOPED_LATENCY_METRIC(metrics_, append_latency);
    SCOPED_WATCH_KERNEL_STACK();

    RETURN_NOT_OK(active_segment_->Append(
      Slice(reinterpret_cast<uint8_t *>(&entry_batch_bytes), 4)));
    RETURN_NOT_OK(active_segment_->Append(entry_batch_data));
    total_ops_in_last_seg_ += entry_batch->count();
    if (log_hooks_) {
      RETURN_NOT_OK_PREPEND(log_hooks_->PostAppend(), "PostAppend hook failed");
    }
  }
  // TODO: Add a record checksum to each WAL record (see KUDU-109).
  return Status::OK();
}

Status Log::AllocateSegmentAndRollOver() {
  RETURN_NOT_OK(AsyncAllocateSegment());
  return RollOver();
}

FsManager* Log::GetFsManager() {
  return fs_manager_;
}

Status Log::Sync() {
  SCOPED_LATENCY_METRIC(metrics_, sync_latency);

  if (force_sync_all_) {
    LOG_SLOW_EXECUTION(WARNING, 50, "Fsync log took a long time") {
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

Status Log::Append(LogEntryPB* phys_entry) {
  gscoped_ptr<LogEntryBatchPB> entry_batch_pb(new LogEntryBatchPB);
  entry_batch_pb->mutable_entry()->AddAllocated(phys_entry);
  LogEntryBatch entry_batch(phys_entry->type(), entry_batch_pb.Pass(), 1);
  entry_batch.state_ = LogEntryBatch::kEntryReserved;
  Status s = entry_batch.Serialize();
  if (s.ok()) {
    entry_batch.state_ = LogEntryBatch::kEntryReady;
    s = DoAppend(&entry_batch, false);
    if (s.ok()) {
      s = Sync();
    }
  }
  entry_batch.entry_batch_pb_->mutable_entry()->ExtractSubrange(0, 1, NULL);
  return s;
}

Status Log::WaitUntilAllFlushed() {
  // In order to make sure we empty the queue we need to use
  // the async api.
  gscoped_ptr<LogEntryBatchPB> entry_batch(new LogEntryBatchPB);
  entry_batch->add_entry()->set_type(log::FLUSH_MARKER);
  LogEntryBatch* reserved_entry_batch;
  RETURN_NOT_OK(Reserve(FLUSH_MARKER, entry_batch.Pass(), &reserved_entry_batch));
  Synchronizer s;
  RETURN_NOT_OK(AsyncAppend(reserved_entry_batch, s.AsStatusCallback()));
  return s.Wait();
}

Status Log::GetLastEntryOpId(consensus::OpId* op_id) const {
  boost::shared_lock<rw_spinlock> read_lock(last_entry_op_id_lock_);
  if (last_entry_op_id_.IsInitialized()) {
    DCHECK_NOTNULL(op_id)->CopyFrom(last_entry_op_id_);
    return Status::OK();
  } else {
    return Status::NotFound("No OpIds have ever been written to the log");
  }
}

Status Log::GC(const consensus::OpId& min_op_id, int32_t* num_gced) {
  CHECK(min_op_id.IsInitialized()) << "Invalid min_op_id: " << min_op_id.ShortDebugString();

  VLOG(1) << "Running Log GC on " << log_dir_;
  VLOG_TIMING(1, "Log GC") {
    SegmentSequence segments_to_delete;

    {
      boost::lock_guard<percpu_rwlock> l(state_lock_);
      CHECK_EQ(kLogWriting, log_state_);

      // Find the prefix of segments in the segment sequence that is guaranteed not to include
      // 'min_op_id'.
      RETURN_NOT_OK(reader_->GetSegmentPrefixNotIncluding(min_op_id, &segments_to_delete));

      if (segments_to_delete.size() == 0) {
        VLOG(1) << "No segments to delete.";
        *num_gced = 0;
        return Status::OK();
      }

      // Trim the prefix of segments from the reader so that they are no longer
      // referenced by the log.
      reader_->TrimSegmentsUpToAndIncluding(
          segments_to_delete[segments_to_delete.size() - 1]->header().sequence_number());
    }

    // Now that they are no longer referenced by the Log, delete the files.
    *num_gced = 0;
    BOOST_FOREACH(const scoped_refptr<ReadableLogSegment>& segment, segments_to_delete) {
      LOG(INFO) << "Deleting Log file in path: " << segment->path();
      RETURN_NOT_OK(fs_manager_->env()->DeleteFile(segment->path()));
      (*num_gced)++;
    }
  }
  return Status::OK();
}

LogReader* Log::GetLogReader() const {
  return reader_.get();
}

void Log::SetSchemaForNextLogSegment(const Schema& schema) {
  boost::lock_guard<rw_spinlock> l(schema_lock_);
  schema_ = schema;
}

Status Log::Close() {
  if (log_hooks_) {
    RETURN_NOT_OK_PREPEND(log_hooks_->PreClose(),
                          "PreClose hook failed");
  }

  allocation_executor_->Shutdown();
  if (append_thread_) {
    append_thread_->Shutdown();
    VLOG(1) << "Append thread Shutdown()";
  }

  boost::lock_guard<percpu_rwlock> l(state_lock_);
  switch (log_state_) {
    case kLogWriting:
      RETURN_NOT_OK(Sync());
      RETURN_NOT_OK(CloseCurrentSegment());
      RETURN_NOT_OK(ReplaceSegmentInReaderUnlocked());
      log_state_ = kLogClosed;
      VLOG(1) << "Log closed";
      return Status::OK();
    case kLogClosed:
      VLOG(1) << "Log already closed";
      return Status::OK();
    default:
      return Status::IllegalState(Substitute("Bad state for Close() $0", log_state_));
  }

  CHECK(postponed_commit_batches_.empty())
    << "TODO: handle COMMITs which never were committable at shutdown";

  if (log_hooks_) {
    RETURN_NOT_OK_PREPEND(log_hooks_->PostClose(),
                          "PostClose hook failed");
  }

  return Status::OK();
}

Status Log::PreAllocateNewSegment() {
  CHECK_EQ(allocation_state(), kAllocationInProgress);

  WritableFileOptions opts;
  opts.mmap_file = false;
  opts.sync_on_close = force_sync_all_;
  RETURN_NOT_OK(CreatePlaceholderSegment(opts, &next_segment_path_, &next_segment_file_));

  if (options_.preallocate_segments) {
    TRACE("Preallocating $0 byte segment in $1", max_segment_size_, next_segment_path_);
    // TODO (perf) zero the new segments -- this could result in
    // additional performance improvements.
    RETURN_NOT_OK(next_segment_file_->PreAllocate(max_segment_size_));
  }

  {
    boost::lock_guard<boost::shared_mutex> lock_guard(allocation_lock_);
    allocation_state_ = kAllocationFinished;
  }
  return Status::OK();
}

Status Log::SwitchToAllocatedSegment() {
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

  LogSegmentHeaderPB header;

  header.set_major_version(kLogMajorVersion);
  header.set_minor_version(kLogMinorVersion);
  header.set_sequence_number(active_segment_sequence_number_);
  header.set_tablet_id(tablet_id_);

  // Set the new segment's schema.
  {
    boost::shared_lock<rw_spinlock> l(schema_lock_);
    RETURN_NOT_OK(SchemaToPB(schema_, header.mutable_schema()));
  }

  RETURN_NOT_OK(new_segment->WriteHeaderAndOpen(header));

  // Transform the currently-active segment into a readable one, since we
  // need to be able to replay the segments for other peers.
  {
    if (active_segment_.get() != NULL) {
      boost::lock_guard<percpu_rwlock> l(state_lock_);
      CHECK_OK(ReplaceSegmentInReaderUnlocked());
    }
  }

  // Open the segment we just created in readable form and add it to the reader.
  gscoped_ptr<RandomAccessFile> readable_file;

  // Don't allow mmapping the file. The mmapped IO path does
  // not support reading from files which change length. In the case
  // of reading from the in-progress segment, that's likely to happen.
  RandomAccessFileOptions opts;
  opts.mmap_file = false;
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
  string path_tmpl = JoinPathSegments(log_dir_, kSegmentPlaceholderFileTemplate);
  VLOG(2) << "Creating temp. file for place holder segment, template: " << path_tmpl;
  gscoped_ptr<WritableFile> segment_file;
  RETURN_NOT_OK(fs_manager_->env()->NewTempWritableFile(opts,
                                                        path_tmpl,
                                                        result_path,
                                                        &segment_file));
  VLOG(1) << "Created next WAL segment, placeholder path: " << *result_path;
  out->reset(segment_file.release());
  return Status::OK();
}

Log::~Log() {
  {
    boost::shared_lock<rw_spinlock> l(state_lock_.get_lock());
    if (log_state_ == kLogClosed) {
      return;
    }
  }
  WARN_NOT_OK(Close(), "Error closing log");
}

LogEntryBatch::LogEntryBatch(LogEntryTypePB type,
                             gscoped_ptr<LogEntryBatchPB> entry_batch_pb, size_t count)
    : type_(type),
      entry_batch_pb_(entry_batch_pb.Pass()),
      total_size_bytes_(
          PREDICT_FALSE(count == 1 && entry_batch_pb_->entry(0).type() == FLUSH_MARKER) ?
          0 : entry_batch_pb_->ByteSize()),
      count_(count),
      state_(kEntryInitialized) {
}

LogEntryBatch::~LogEntryBatch() {
}

void LogEntryBatch::MarkReserved() {
  DCHECK_EQ(state_, kEntryInitialized);
  ready_lock_.Lock();
  state_ = kEntryReserved;
}

Status LogEntryBatch::Serialize() {
  DCHECK_EQ(state_, kEntryReserved);
  buffer_.clear();
  // FLUSH_MARKER LogEntries are markers and are not serialized.
  if (PREDICT_FALSE(count() == 1 && entry_batch_pb_->entry(0).type() == FLUSH_MARKER)) {
    state_ = kEntrySerialized;
    return Status::OK();
  }
  buffer_.reserve(total_size_bytes_);

  if (!pb_util::AppendToString(*entry_batch_pb_, &buffer_)) {
    return Status::IOError(Substitute("unable to serialize the entry batch, contents: $1",
                                      entry_batch_pb_->DebugString()));
  }

  state_ = kEntrySerialized;
  return Status::OK();
}

void LogEntryBatch::MarkReady() {
  DCHECK_EQ(state_, kEntrySerialized);
  state_ = kEntryReady;
  ready_lock_.Unlock();
}

void LogEntryBatch::WaitForReady() {
  ready_lock_.Lock();
  DCHECK_EQ(state_, kEntryReady);
  ready_lock_.Unlock();
}

}  // namespace log
}  // namespace kudu
