// Copyright (c) 2013, Cloudera, inc.

#include "consensus/log.h"

#include "consensus/log_reader.h"
#include "gutil/strings/substitute.h"
#include "gutil/stl_util.h"
#include "server/fsmanager.h"
#include "util/coding.h"
#include "util/env_util.h"
#include "util/pb_util.h"
#include "util/stopwatch.h"
#include "util/countdown_latch.h"
#include "util/thread_util.h"

// TODO Size of the queue/buffer should be defined bytewise, rather
// than by the element count.
DEFINE_int32(group_commit_queue_size, 1024,
             "Maxmimum size of the group commit queue. TODO: name does not account"
             "for the fact that elements of the queue can contain multiple operations");

namespace kudu {
namespace log {

using consensus::OpId;
using consensus::MISSED_DELTA;
using env_util::OpenFileForRandom;

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
  gscoped_ptr<boost::thread> thread_;
  CountDownLatch finished_;
};


Log::AppendThread::AppendThread(Log *log)
    : log_(log),
      closing_(false),
      finished_(1) {
}

Status Log::AppendThread::Init() {
  DCHECK(thread_.get() == NULL) << "Already initialized";
  RETURN_NOT_OK(StartThread(boost::bind(
      &AppendThread::RunThread, this), &thread_));
  return Status::OK();
}

void Log::AppendThread::RunThread() {
  BlockingQueue<LogEntry*>* queue = log_->entry_queue();
  while (PREDICT_TRUE(!closing())) {
    std::vector<LogEntry*> entries;
    ElementDeleter d(&entries);

    if (PREDICT_FALSE(!queue->BlockingDrainTo(&entries))) {
      break;
    }

    BOOST_FOREACH(LogEntry* entry, entries) {
      entry->WaitForReady();
      Status s = log_->DoAppend(entry);
      if (PREDICT_FALSE(!s.ok())) {
        LOG(ERROR) << "Error appending to the log: " << s.ToString();
        DLOG(FATAL) << "Aborting: " << s.ToString();
        entry->set_failed_to_append();
        // TODO If a single transaction fails to append, should we
        // abort all subsequent transactions in this batch or allow
        // them to be appended? What about transactions in future
        // batches?
        entry->callback()->OnFailure(s);
      }
    }

    Status s = log_->Sync();
    if (PREDICT_FALSE(!s.ok())) {
      LOG(ERROR) << "Error syncing log" << s.ToString();
      DLOG(FATAL) << "Aborting: " << s.ToString();
      BOOST_FOREACH(LogEntry* entry, entries) {
        entry->callback()->OnFailure(s);
      }
    } else {
      VLOG(2) << "Synchronized " << entries.size() << " entries";
      BOOST_FOREACH(LogEntry* entry, entries) {
        if (PREDICT_TRUE(!entry->failed_to_append())) {
          entry->callback()->OnSuccess();
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

  LOG(INFO) << "Shutting down Log append thread!";

  BlockingQueue<LogEntry*>* queue = log_->entry_queue();
  queue->Shutdown();

  {
    boost::lock_guard<boost::mutex> lock_guard(lock_);
    closing_ = true;
  }

  finished_.Wait();

  LOG(INFO) << "Log append thread shut down!";

  CHECK_OK(ThreadJoiner(thread_.get(), "log appender thread").Join())
}

const Status Log::kLogShutdownStatus(
    Status::ServiceUnavailable("WAL is shutting down", "", ESHUTDOWN));

const uint64_t Log::kInitialLogSegmentSequenceNumber = 0L;

Status Log::Open(const LogOptions &options,
                 FsManager *fs_manager,
                 const metadata::TabletSuperBlockPB& super_block,
                 const consensus::OpId& current_id,
                 const std::string& tablet_id,
                 gscoped_ptr<Log> *log) {

  gscoped_ptr<LogSegmentHeaderPB> header(new LogSegmentHeaderPB);
  header->set_major_version(kLogMajorVersion);
  header->set_minor_version(kLogMinorVersion);
  header->mutable_tablet_meta()->CopyFrom(super_block);
  header->mutable_initial_id()->CopyFrom(current_id);
  header->set_tablet_id(tablet_id);
  // TODO: Would rather set this only once but API needs some cleanup.
  // If there are existing log segments, we overwrite this seqno value later.
  header->set_sequence_number(kInitialLogSegmentSequenceNumber);

  RETURN_NOT_OK(fs_manager->CreateDirIfMissing(fs_manager->GetWalsRootDir()));

  string tablet_wal_path = fs_manager->env()->JoinPathSegments(
      fs_manager->GetWalsRootDir(),
      super_block.oid());

  RETURN_NOT_OK(fs_manager->CreateDirIfMissing(tablet_wal_path));

  gscoped_ptr<Log> new_log(new Log(options,
                                   fs_manager,
                                   tablet_wal_path,
                                   header.Pass()));
  RETURN_NOT_OK(new_log->Init());
  log->reset(new_log.release());
  return Status::OK();
}

Log::Log(const LogOptions &options,
         FsManager *fs_manager,
         const string& log_path,
         gscoped_ptr<LogSegmentHeaderPB> header)
: options_(options),
  fs_manager_(fs_manager),
  log_dir_(log_path),
  next_segment_header_(header.Pass()),
  header_size_(0),
  max_segment_size_(options_.segment_size_mb * 1024 * 1024),
  entry_queue_(FLAGS_group_commit_queue_size),
  append_thread_(new AppendThread(this)),
  state_(kLogInitialized) {
}

Status Log::Init() {
  CHECK_EQ(state_, kLogInitialized);

  RETURN_NOT_OK(LogReader::Open(fs_manager_,
                                next_segment_header_->tablet_id(),
                                &previous_segments_reader_));

  // The case where we are continuing an existing log.
  // We must pick up where the previous WAL left off in terms of
  // sequence numbers.
  if (previous_segments_reader_->size() != 0) {
    previous_segments_ = previous_segments_reader_->segments();
    VLOG(1) << "Using existing " << previous_segments_.size()
            << " segments from path: " << fs_manager_->GetWalsRootDir();

    const shared_ptr<ReadableLogSegment>& segment = previous_segments_.back();
    uint64_t seqno = segment->header().sequence_number();
    // TODO: This is a bit hacky, clean up the Log construction API once we
    // remove the initial_opid and metadata from the log segment headers.
    next_segment_header_->set_sequence_number(seqno + 1);
  }

  if (options_.force_fsync_all) {
    LOG(INFO) << "Log is configured to fsync() on all Append() calls";
  } else {
    LOG(INFO) << "Log is configured to *not* fsync() on all Append() calls";
  }

  // we always create a new segment when the log starts.
  RETURN_NOT_OK(CreateNewSegment());
  RETURN_NOT_OK(append_thread_->Init());
  state_ = kLogWriting;
  return Status::OK();
}

Status Log::RollOver() {
  CHECK_EQ(state_, kLogWriting);
  RETURN_NOT_OK(Sync());
  RETURN_NOT_OK(active_segment_->writable_file()->Close());
  RETURN_NOT_OK(CreateNewSegment());
  return Status::OK();
}

Status Log::Reserve(const vector<consensus::OperationPB*>& ops,
                    LogEntry** reserved_entry) {
  DCHECK(reserved_entry != NULL);
  CHECK_EQ(state_, kLogWriting);

  size_t num_ops = ops.size();
  gscoped_ptr<LogEntryPB[]> entries(new LogEntryPB[num_ops]);
  for (size_t i = 0; i < num_ops; i++) {
    consensus::OperationPB* op = ops[i];
    LogEntryPB& entry = entries[i];
    entry.set_type(log::OPERATION);
    entry.set_allocated_operation(op);
  }
  gscoped_ptr<LogEntry> new_entry(new LogEntry(entries.Pass(),
                                                   num_ops));
  new_entry->MarkReserved();

  if (PREDICT_FALSE(!entry_queue_.BlockingPut(new_entry.get()))) {
    return kLogShutdownStatus;
  }

  // Release the memory back to the caller: this will be freed when
  // the entry is removed from the queue.
  //
  // TODO (perf) Use a ring buffer instead of a blocking queue and set
  // 'reserved_entry' to a pre-allocated slot in the buffer.
  *reserved_entry = new_entry.release();
  return Status::OK();
}

Status Log::AsyncAppend(LogEntry* entry, const shared_ptr<FutureCallback>& callback) {
  CHECK_EQ(state_, kLogWriting);

  RETURN_NOT_OK(entry->Serialize());
  entry->set_callback(callback);
  entry->MarkReady();

  return Status::OK();
}

Status Log::DoAppend(LogEntry* entry, bool caller_owns_operation) {
  // TODO (perf) make this more efficient, cache the highest id
  // during LogEntry::Serialize()
  for (size_t i = 0; i < entry->count(); i++) {
    LogEntryPB& entry_pb = entry->phys_entries_[i];
    switch (entry_pb.type()) {
      case OPERATION: {
        if (PREDICT_TRUE(entry_pb.operation().has_id())) {
          next_segment_header_->mutable_initial_id()->CopyFrom(entry_pb.operation().id());
        } else {
          DCHECK(entry_pb.operation().has_commit()
                 && entry_pb.operation().commit().op_type() == MISSED_DELTA)
              << "Operation did not have an id. Only COMMIT operations of"
              " MISSED_DELTA type are allowed not to have ids.";
        }
        if (caller_owns_operation) {
          // If operation is allocated by another thread, so we must release it
          // to avoid freeing the memory
          entry_pb.release_operation();
        }
        break;
      }
      case TABLET_METADATA: {
        next_segment_header_->mutable_tablet_meta()->CopyFrom(entry_pb.tablet_meta());
        break;
      }
      default: {
        LOG(FATAL) << "Unexpected log entry type: " << entry_pb.DebugString();
      }
    }
  }
  Slice entry_data = entry->data();
  uint32_t entry_size = entry_data.size();

  // if the size of this entry overflows the current segment, get a new one
  if ((active_segment_->writable_file()->Size() + entry_size + 4) > max_segment_size_) {
    LOG_SLOW_EXECUTION(WARNING, 50, "Log roll took a long time") {
      RETURN_NOT_OK(RollOver());
    }
    LOG(INFO) << "Max segment size reached. Rolled over to new segment: "
              << active_segment_->path();
  }
  LOG_SLOW_EXECUTION(WARNING, 25, "Append to log took a long time") {
    RETURN_NOT_OK(active_segment_->writable_file()->Append(
      Slice(reinterpret_cast<uint8_t *>(&entry_size), 4)));
  }
  RETURN_NOT_OK(active_segment_->writable_file()->Append(entry_data));
  // TODO: Add a record checksum to each WAL record (see KUDU-109).
  return Status::OK();
}

Status Log::Sync() {
  if (options_.force_fsync_all) {
    LOG_SLOW_EXECUTION(WARNING, 50, "Fsync log took a long time") {
      RETURN_NOT_OK(active_segment_->writable_file()->Sync());
    }
  }
  return Status::OK();
}

Status Log::Append(LogEntryPB* phys_entry) {
  gscoped_ptr<LogEntryPB[]> phys_entries(phys_entry);
  LogEntry entry(phys_entries.Pass(), 1);
  entry.state_ = LogEntry::kEntryReserved;
  Status s = entry.Serialize();
  if (s.ok()) {
    entry.state_ = LogEntry::kEntryReady;
    s = DoAppend(&entry, false);
    if (s.ok()) {
      s = Sync();
    }
  }
  ignore_result(entry.phys_entries_.release());
  return s;
}

Status Log::GC() {
  LOG(INFO) << "Running Log GC on " << log_dir_;
  LOG_TIMING(INFO, "Log GC") {
    uint32_t num_stale_segments = 0;
    RETURN_NOT_OK(FindStaleSegmentsPrefixSize(previous_segments_,
                                              next_segment_header_->tablet_meta(),
                                              &num_stale_segments));
    if (num_stale_segments > 0) {
      LOG(INFO) << "Found " << num_stale_segments << " stale segments.";
      // delete the files and erase the segments from previous_
      for (int i = 0; i < num_stale_segments; i++) {
        LOG(INFO) << "Deleting Log file in path: " << previous_segments_[i]->path();
        RETURN_NOT_OK(fs_manager_->env()->DeleteFile(previous_segments_[i]->path()));
      }
      previous_segments_.erase(previous_segments_.begin(),
                               previous_segments_.begin() + num_stale_segments);
    }
  }
  return Status::OK();
}

Status Log::Close() {
  if (append_thread_.get() != NULL) {
    append_thread_->Shutdown();
    VLOG(1) << "Append thread Shutdown()";
  }
  if (state_ == kLogWriting) {
    RETURN_NOT_OK(Sync());
    RETURN_NOT_OK(active_segment_->writable_file()->Close());
    state_ = kLogClosed;
    VLOG(1) << "Log Closed()";
    return Status::OK();
  }
  if (state_ == kLogClosed) {
    VLOG(1) << "Log already Closed()";
    return Status::OK();
  }
  return Status::IllegalState(strings::Substitute("Bad state for Close() $0", state_));
}

Status Log::CreateNewSegment() {

  LogSegmentHeaderPB header;
  header.CopyFrom(*next_segment_header_.get());

  // Increment "next" log segment seqno.
  next_segment_header_->set_sequence_number(next_segment_header_->sequence_number() + 1);

  // create a new segment file and pre-allocate the space
  shared_ptr<WritableFile> sink;
  string new_segment_path = CreateSegmentFileName(header.sequence_number());

  VLOG(1) << "Creating new WAL segment: " << new_segment_path;
  RETURN_NOT_OK(env_util::OpenFileForWrite(fs_manager_->env(), new_segment_path, &sink));

  if (options_.preallocate_segments) {
    RETURN_NOT_OK(sink->PreAllocate(max_segment_size_));
  }

  // Create a new segment.
  gscoped_ptr<WritableLogSegment> new_segment(
      new WritableLogSegment(header, new_segment_path, sink));
  // Write and sync the header.
  uint32_t pb_size = new_segment->header().ByteSize();
  faststring buf;
  // First the magic.
  buf.append(kMagicString);
  // Then Length-prefixed header.
  PutFixed32(&buf, pb_size);
  if (!pb_util::AppendToString(new_segment->header(), &buf)) {
    return Status::Corruption("unable to encode header");
  }

  RETURN_NOT_OK(new_segment->writable_file()->Append(Slice(buf)));
  uint32_t new_header_size = kMagicAndHeaderLength + pb_size;
  RETURN_NOT_OK(new_segment->writable_file()->Sync());

  // transform the current segment into a readable one (we might need to replay
  // stuff for other peers)
  if (active_segment_.get() != NULL) {
    shared_ptr<RandomAccessFile> source;
    RETURN_NOT_OK(OpenFileForRandom(fs_manager_->env(), active_segment_->path(), &source));
    shared_ptr<ReadableLogSegment> readable_segment(
        new ReadableLogSegment(active_segment_->header(),
                               active_segment_->path(),
                               header_size_,
                               active_segment_->writable_file()->Size(),
                               source));
    previous_segments_.push_back(readable_segment);
  }

  // Now set 'active_segment_' to the new segment.
  header_size_ = new_header_size;
  active_segment_.reset(new_segment.release());

  return Status::OK();
}

string Log::CreateSegmentFileName(uint64_t sequence_number) {
  return fs_manager_->env()->
      JoinPathSegments(log_dir_,
                       strings::Substitute("$0-$1",
                                           kLogPrefix,
                                           StringPrintf("%09lu", sequence_number)));
}

Log::~Log() {
  if (state_ == kLogWriting) {
    WARN_NOT_OK(Close(), "Error closing log");
  }
}

LogEntry::LogEntry(gscoped_ptr<LogEntryPB[]> phys_entries,
                  size_t count)
    : phys_entries_(phys_entries.Pass()),
      count_(count),
      state_(kEntryInitialized) {
}

LogEntry::~LogEntry() {
}

void LogEntry::MarkReserved() {
  DCHECK_EQ(state_, kEntryInitialized);
  ready_lock_.Lock();
  state_ = kEntryReserved;
}

Status LogEntry::Serialize() {
  DCHECK_EQ(state_, kEntryReserved);
  buffer_.clear();
  uint32_t totalByteSize = 0;
  for (int i = 0; i < count_; ++i) {
    totalByteSize += phys_entries_[i].ByteSize();
  }
  buffer_.reserve(totalByteSize);
  for (int i = 0; i < count_; ++i) {
    LogEntryPB& entry = phys_entries_[i];
    if (!pb_util::AppendToString(entry, &buffer_)) {
      return Status::IOError(strings::Substitute(
          "unable to serialize entry pb $0 out of $1; entry contents: $3",
          i, count_, entry.DebugString()));
    }
  }
  state_ = kEntrySerialized;
  return Status::OK();
}

void LogEntry::MarkReady() {
  DCHECK_EQ(state_, kEntrySerialized);
  state_ = kEntryReady;
  ready_lock_.Unlock();
}

void LogEntry::WaitForReady() {
  ready_lock_.Lock();
  DCHECK_EQ(state_, kEntryReady);
  ready_lock_.Unlock();
}

}  // namespace log
}  // namespace kudu

