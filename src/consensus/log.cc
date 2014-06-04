// Copyright (c) 2013, Cloudera, inc.

#include "consensus/log.h"

#include "consensus/log_metrics.h"
#include "consensus/log_reader.h"
#include "consensus/log_util.h"
#include "gutil/map-util.h"
#include "gutil/ref_counted.h"
#include "gutil/stl_util.h"
#include "gutil/strings/substitute.h"
#include "server/fsmanager.h"
#include "util/coding.h"
#include "util/countdown_latch.h"
#include "util/env_util.h"
#include "util/kernel_stack_watchdog.h"
#include "util/logging.h"
#include "util/metrics.h"
#include "util/path_util.h"
#include "util/pb_util.h"
#include "util/thread.h"
#include "util/trace.h"
#include "util/stopwatch.h"

DEFINE_int32(group_commit_queue_size_bytes, 4 * 1024 * 1024,
             "Maxmimum size of the group commit queue in bytes");

static const char kSegmentPlaceholderFileTemplate[] = ".tmp.newsegmentXXXXXX";

namespace kudu {
namespace log {

using consensus::OpId;
using env_util::OpenFileForRandom;
using strings::Substitute;

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
  CountDownLatch finished_;
};


Log::AppendThread::AppendThread(Log *log)
    : log_(log),
      closing_(false),
      finished_(1) {
}

Status Log::AppendThread::Init() {
  DCHECK(thread_.get() == NULL) << "Already initialized";
  RETURN_NOT_OK(kudu::Thread::Create("log", "appender",
      &AppendThread::RunThread, this, &thread_));
  return Status::OK();
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
        entry_batch->callback()(s);
      }
    }

    Status s = log_->Sync();
    if (PREDICT_FALSE(!s.ok())) {
      LOG(ERROR) << "Error syncing log" << s.ToString();
      DLOG(FATAL) << "Aborting: " << s.ToString();
      BOOST_FOREACH(LogEntryBatch* entry_batch, entry_batches) {
        if (entry_batch->callback() != NULL) {
          entry_batch->callback()(s);
        }
      }
    } else {
      VLOG(2) << "Synchronized " << entry_batches.size() << " entry batches";
      BOOST_FOREACH(LogEntryBatch* entry_batch, entry_batches) {
        if (PREDICT_TRUE(!entry_batch->failed_to_append()
                         && entry_batch->callback() != NULL)) {
          entry_batch->callback()(Status::OK());
        }
      }
    }
  }
  finished_.CountDown();
  VLOG(1) << "Finished AppendThread()";
}

void Log::AppendThread::Shutdown() {
  if (closing()) {
    return;
  }

  VLOG(1) << "Shutting down Log append thread!";

  LogEntryBatchQueue* queue = log_->entry_queue();
  queue->Shutdown();

  {
    boost::lock_guard<boost::mutex> lock_guard(lock_);
    closing_ = true;
  }

  finished_.Wait();

  VLOG(1) << "Log append thread shut down!";

  CHECK_OK(ThreadJoiner(thread_.get()).Join())
}

// This task is submitted to allocation_executor_ in order to
// asynchronously pre-allocate new log segments.
class Log::SegmentAllocationTask : public Task {
 public:
  explicit SegmentAllocationTask(Log* log)
    : log_(log) {
  }

  Status Run() {
    RETURN_NOT_OK(log_->PreAllocateNewSegment());
    return Status::OK();
  }

  bool Abort() { return false; }
 private:
  Log* log_;
};

const Status Log::kLogShutdownStatus(
    Status::ServiceUnavailable("WAL is shutting down", "", ESHUTDOWN));

const uint64_t Log::kInitialLogSegmentSequenceNumber = 0L;

Status Log::Open(const LogOptions &options,
                 FsManager *fs_manager,
                 const std::string& tablet_id,
                 MetricContext* parent_metrics_context,
                 gscoped_ptr<Log> *log) {

  RETURN_NOT_OK(fs_manager->CreateDirIfMissing(fs_manager->GetWalsRootDir()));

  string tablet_wal_path = JoinPathSegments(fs_manager->GetWalsRootDir(), tablet_id);
  RETURN_NOT_OK(fs_manager->CreateDirIfMissing(tablet_wal_path));

  gscoped_ptr<Log> new_log(new Log(options,
                                   fs_manager,
                                   tablet_wal_path,
                                   tablet_id,
                                   parent_metrics_context));
  RETURN_NOT_OK(new_log->Init());
  log->reset(new_log.release());
  return Status::OK();
}

// TODO It's inefficient to have an executor here with a fixed number
// of threads: pre-allocations are relatively short lived and
// infrequent. It makes sense to implement and use something like
// Java's CachedThreadPool -- which has a min and max number of
// threads, where min could be 0.
Log::Log(const LogOptions &options,
         FsManager *fs_manager,
         const string& log_path,
         const string& tablet_id,
         MetricContext* parent_metric_context)
: options_(options),
  fs_manager_(fs_manager),
  log_dir_(log_path),
  tablet_id_(tablet_id),
  log_state_(kLogInitialized),
  max_segment_size_(options_.segment_size_mb * 1024 * 1024),
  entry_batch_queue_(FLAGS_group_commit_queue_size_bytes),
  append_thread_(new AppendThread(this)),
  allocation_executor_(TaskExecutor::CreateNew("allocation exec", 1)),
  force_sync_all_(options_.force_fsync_all),
  allocation_state_(kAllocationNotStarted) {

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
  gscoped_ptr<LogReader> previous_segments_reader;
  RETURN_NOT_OK(LogReader::Open(fs_manager_,
                                tablet_id_,
                                &previous_segments_reader));

  // The case where we are continuing an existing log.
  // We must pick up where the previous WAL left off in terms of
  // sequence numbers.
  if (previous_segments_reader->size() != 0) {
    previous_segments_ = previous_segments_reader->segments();
    VLOG(1) << "Using existing " << previous_segments_.size()
            << " segments from path: " << fs_manager_->GetWalsRootDir();

    const scoped_refptr<ReadableLogSegment>& segment = previous_segments_.rbegin()->second;
    uint64_t last_written_seqno = segment->header().sequence_number();
    active_segment_sequence_number_ = last_written_seqno + 1;
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

Status Log::RollOver() {
  SCOPED_LATENCY_METRIC(metrics_, roll_latency);

  // Check if any errors have occurred during allocation
  allocation_future_->Wait();
  RETURN_NOT_OK(allocation_future_->status());

  DCHECK_EQ(allocation_state(), kAllocationFinished);

  RETURN_NOT_OK(Sync());
  RETURN_NOT_OK(active_segment_->writable_file()->Close());

  RETURN_NOT_OK(SwitchToAllocatedSegment());

  LOG(INFO) << "Rolled over to a new segment: " << active_segment_->path();
  return Status::OK();
}

Status Log::Reserve(const vector<const consensus::OperationPB*>& ops,
                    LogEntryBatch** reserved_entry) {
  DCHECK(reserved_entry != NULL);
  {
    boost::shared_lock<rw_spinlock> read_lock(state_lock_.get_lock());
    CHECK_EQ(kLogWriting, log_state_);
  }

  size_t num_ops = ops.size();
  gscoped_ptr<LogEntryBatchPB> entry_batch(new LogEntryBatchPB);
  for (size_t i = 0; i < num_ops; i++) {
    // We want to re-use the existing objects here, so const-casting allows
    // us to put a reference in the new PB.
    consensus::OperationPB* op = const_cast<consensus::OperationPB*>(ops[i]);
    LogEntryPB* entry_pb = entry_batch->add_entry();
    entry_pb->set_type(log::OPERATION);
    entry_pb->set_allocated_operation(op);
  }
  return DoReserve(entry_batch.Pass(), reserved_entry);
}

Status Log::DoReserve(gscoped_ptr<LogEntryBatchPB> entry_batch,
                    LogEntryBatch** reserved_entry) {
  int num_ops = entry_batch->entry_size();
  gscoped_ptr<LogEntryBatch> new_entry_batch(new LogEntryBatch(entry_batch.Pass(), num_ops));
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

Status Log::AsyncAppend(LogEntryBatch* entry_batch) {
  {
    boost::shared_lock<rw_spinlock> read_lock(state_lock_.get_lock());
    CHECK_EQ(kLogWriting, log_state_);
  }

  RETURN_NOT_OK(entry_batch->Serialize());
  entry_batch->MarkReady();

  return Status::OK();
}

Status Log::DoAppend(LogEntryBatch* entry_batch, bool caller_owns_operation) {
  // TODO (perf) make this more efficient, cache the highest id
  // during LogEntryBatch::Serialize()

  size_t num_entries = entry_batch->count();
  DCHECK_GT(num_entries, 0) << "Cannot call DoAppend() with zero entries reserved";

  Slice entry_batch_data = entry_batch->data();
  uint32_t entry_batch_bytes = entry_batch->total_size_bytes();
  // If there is no data to write return OK.
  if (PREDICT_FALSE(entry_batch_bytes == 0)) {
    return Status::OK();
  }

  // Keep track of the first OpId seen in this batch.
  // This is needed for writing initial_opid into each log segment header.
  OpId first_op_id;
  first_op_id.CopyFrom(entry_batch->entry_batch_pb_->entry(0).operation().id());
  DCHECK(first_op_id.IsInitialized());

  // We keep track of the last-written OpId here.
  // This is needed to initialize Consensus on startup.
  {
    boost::lock_guard<rw_spinlock> write_lock(last_entry_op_id_lock_);
    last_entry_op_id_.CopyFrom(
        entry_batch->entry_batch_pb_->entry(num_entries - 1).operation().id());
    DCHECK(last_entry_op_id_.IsInitialized());
  }

  for (size_t i = 0; i < num_entries; i++) {
    LogEntryPB* entry_pb = entry_batch->entry_batch_pb_->mutable_entry(i);
    CHECK_EQ(OPERATION, entry_pb->type())
        << "Unexpected log entry type: " << entry_pb->DebugString();
    DCHECK(entry_pb->operation().has_id());
    DCHECK(entry_pb->operation().id().IsInitialized());

    if (caller_owns_operation) {
      // If the OperationPB was allocated by another thread, we must release
      // it to avoid freeing the memory.
      entry_pb->release_operation();
    }
  }

  // Write the header, if we've never written it before.
  // We always lazily write the header on the first batch.
  if (!active_segment_->IsHeaderWritten()) {
    WriteHeader(first_op_id);
  }

  if (metrics_) {
    metrics_->bytes_logged->IncrementBy(entry_batch_bytes);
  }

  // if the size of this entry overflows the current segment, get a new one
  if (allocation_state() == kAllocationNotStarted) {
    if ((active_segment_->writable_file()->Size() + entry_batch_bytes + 4) > max_segment_size_) {
      LOG(INFO) << "Max segment size reached. Starting new segment allocation. ";
      RETURN_NOT_OK(AsyncAllocateSegment());
      if (!options_.async_preallocate_segments) {
        LOG_SLOW_EXECUTION(WARNING, 50, "Log roll took a long time") {
          RETURN_NOT_OK(RollOver());
        }
        WriteHeader(first_op_id);
      }
    }
  } else if (allocation_state() == kAllocationFinished) {
    LOG_SLOW_EXECUTION(WARNING, 50, "Log roll took a long time") {
      RETURN_NOT_OK(RollOver());
    }
    WriteHeader(first_op_id);
  } else {
    VLOG(1) << "Segment allocation already in progress...";
  }

  LOG_SLOW_EXECUTION(WARNING, 50, "Append to log took a long time") {
    SCOPED_LATENCY_METRIC(metrics_, append_latency);
    SCOPED_WATCH_KERNEL_STACK();

    RETURN_NOT_OK(active_segment_->writable_file()->Append(
      Slice(reinterpret_cast<uint8_t *>(&entry_batch_bytes), 4)));
    RETURN_NOT_OK(active_segment_->writable_file()->Append(entry_batch_data));
  }
  // TODO: Add a record checksum to each WAL record (see KUDU-109).
  return Status::OK();
}

Status Log::WriteHeader(const OpId& initial_op_id) {
  CHECK(initial_op_id.IsInitialized())
      << "Uninitialized OpId: " << initial_op_id.InitializationErrorString();
  LogSegmentHeaderPB header;
  DCHECK(!header.initial_id().IsInitialized())
      << "Some OpId fields must be required, log invariant checks compromised";

  header.set_major_version(kLogMajorVersion);
  header.set_minor_version(kLogMinorVersion);
  header.set_sequence_number(active_segment_sequence_number_);
  header.set_tablet_id(tablet_id_);
  header.mutable_initial_id()->CopyFrom(initial_op_id);

  return active_segment_->WriteHeader(header);
}

Status Log::WriteHeaderForTests() {
  OpId zero(MinimumOpId());
  return WriteHeader(zero);
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
      RETURN_NOT_OK(active_segment_->writable_file()->Sync());
    }
  }
  return Status::OK();
}

Status Log::Append(LogEntryPB* phys_entry) {
  gscoped_ptr<LogEntryBatchPB> entry_batch_pb(new LogEntryBatchPB);
  entry_batch_pb->mutable_entry()->AddAllocated(phys_entry);
  LogEntryBatch entry_batch(entry_batch_pb.Pass(), 1);
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
  RETURN_NOT_OK(DoReserve(entry_batch.Pass(), &reserved_entry_batch));
  Synchronizer s;
  RETURN_NOT_OK(AsyncAppend(reserved_entry_batch, s.callback()));
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
    vector<string> stale_segment_paths;

    {
      boost::lock_guard<percpu_rwlock> l(state_lock_);
      CHECK_EQ(kLogWriting, log_state_);

      OpIdRange initial_opids;
      size_t num_stale_segments =
          FindStaleSegmentsPrefixSize(previous_segments_, min_op_id, &initial_opids);

      if (num_stale_segments > 0) {
        LOG(INFO) << "Found " << num_stale_segments << " stale log segments.";
        const ReadableLogSegmentMap::iterator& first = previous_segments_.find(initial_opids.first);
        // Sanity check.
        const OpId& earliest_available_opid = previous_segments_.begin()->first;
        CHECK(OpIdEquals(initial_opids.first, earliest_available_opid))
            << "OpIds should be equal: " << initial_opids.first.ShortDebugString()
            << " and " << earliest_available_opid.ShortDebugString();
        // The last OpId in the initial_opids range is not inclusive.
        const ReadableLogSegmentMap::iterator& last = previous_segments_.find(initial_opids.second);
        ReadableLogSegmentMap::iterator copy_iter = first;
        while (copy_iter != last) {
          stale_segment_paths.push_back(copy_iter->second->path());
          copy_iter++;
        }
        previous_segments_.erase(first, last);
      }
    }

    // Now that they are no longer referenced by the Log, delete the files.
    *num_gced = 0;
    BOOST_FOREACH(const string& path, stale_segment_paths) {
      LOG(INFO) << "Deleting Log file in path: " << path;
      RETURN_NOT_OK(fs_manager_->env()->DeleteFile(path));
      (*num_gced)++;
    }
  }
  return Status::OK();
}

void Log::GetReadableLogSegments(ReadableLogSegmentMap* segments) const {
  DCHECK(segments);
  boost::shared_lock<rw_spinlock> l(state_lock_.get_lock());
  *segments = previous_segments_;
}

int Log::GetNumReadableLogSegmentsForTests() const {
  boost::shared_lock<rw_spinlock> l(state_lock_.get_lock());
  return previous_segments_.size();
}

Status Log::Close() {
  allocation_executor_->Shutdown();
  if (append_thread_.get() != NULL) {
    append_thread_->Shutdown();
    VLOG(1) << "Append thread Shutdown()";
  }

  boost::lock_guard<percpu_rwlock> l(state_lock_);
  switch (log_state_) {
    case kLogWriting:
      RETURN_NOT_OK(Sync());
      RETURN_NOT_OK(active_segment_->writable_file()->Close());
      log_state_ = kLogClosed;
      VLOG(1) << "Log closed";
      return Status::OK();
    case kLogClosed:
      VLOG(1) << "Log already closed";
      return Status::OK();
    default:
      return Status::IllegalState(Substitute("Bad state for Close() $0", log_state_));
  }
}

Status Log::PreAllocateNewSegment() {
  CHECK_EQ(allocation_state(), kAllocationInProgress);

  WritableFileOptions opts;
  opts.mmap_file = false;
  opts.sync_on_close = force_sync_all_;
  RETURN_NOT_OK(CreatePlaceholderSegment(opts, &next_segment_path_, &next_segment_file_));

  if (options_.preallocate_segments) {
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

  string new_segment_path = CreateSegmentFileName(active_segment_sequence_number_);

  // TODO KUDU-261: Technically, we need to fsync DirName(new_segment_path)
  // after RenameFile().
  RETURN_NOT_OK(fs_manager_->env()->RenameFile(next_segment_path_, new_segment_path));

  // Create a new segment.
  gscoped_ptr<WritableLogSegment> new_segment(
      new WritableLogSegment(new_segment_path, next_segment_file_));

  // Transform the currently-active segment into a readable one, since we
  // need to be able to replay the segments for other peers.
  if (active_segment_.get() != NULL) {
    // We should never switch to a new segment if we wrote nothing to the old one.
    CHECK(active_segment_->IsHeaderWritten());
    shared_ptr<RandomAccessFile> readable_file;
    RETURN_NOT_OK(OpenFileForRandom(fs_manager_->env(), active_segment_->path(), &readable_file));
    scoped_refptr<ReadableLogSegment> readable_segment(
        new ReadableLogSegment(active_segment_->path(),
                               active_segment_->writable_file()->Size(),
                               readable_file));
    // Note: active_segment_->header() will only contain an initialized PB if we
    // wrote the header out.
    readable_segment->Init(active_segment_->header(), active_segment_->first_entry_offset());
    const OpId& op_id = readable_segment->header().initial_id();

    boost::lock_guard<percpu_rwlock> l(state_lock_);
    InsertOrDie(&previous_segments_, op_id, readable_segment);
  }

  // Now set 'active_segment_' to the new segment.
  active_segment_.reset(new_segment.release());

  allocation_state_ = kAllocationNotStarted;

  return Status::OK();
}

string Log::CreateSegmentFileName(uint64_t sequence_number) {
  return JoinPathSegments(log_dir_,
                          strings::Substitute("$0-$1",
                                              kLogPrefix,
                                              StringPrintf("%09lu", sequence_number)));
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

LogEntryBatch::LogEntryBatch(gscoped_ptr<LogEntryBatchPB> entry_batch_pb, size_t count)
    : entry_batch_pb_(entry_batch_pb.Pass()),
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
