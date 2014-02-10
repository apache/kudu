// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_CONSENSUS_LOG_H_
#define KUDU_CONSENSUS_LOG_H_

#include <string>
#include <vector>

#include "gutil/spinlock.h"
#include "consensus/log_util.h"
#include "util/task_executor.h"
#include "util/blocking_queue.h"

namespace kudu {

class FsManager;

namespace log {

class LogReader;
class LogEntry;

// Log interface, inspired by Raft's (logcabin) Log. Provides durability to
// Kudu as a normal Write Ahead Log and also plays the role of persistent
// storage for the consensus state machine.
//
// Note: This class is not thread safe, the caller is expected to synchronize
// Log::Reserve() and Log::Append() calls.
//
// Log uses group commit to improve write throughput and latency
// without compromising ordering and durability guarantees.
//
// To add operations to the log, the caller must obtain the lock and
// call Reserve() with the collection of operations to be added. Then,
// the caller may release the lock and call AsyncAppend(). Reserve()
// reserves a slot on a queue for the log entry; AsyncAppend()
// indicates that the entry in the slot is safe to write to disk and
// adds a callback that will be invoked once the entry is written and
// synchronized to disk.
//
// For sample usage see local_consensus.cc and mt-log-test.cc
class Log {
 public:
  static const Status kLogShutdownStatus;

  // Opens or continues a log and sets 'log' to the newly built Log.
  // After Open() the Log is ready to receive entries.
  // The passed 'super_block' and 'current_id' are copied.
  static Status Open(const LogOptions &options,
                     FsManager *fs_manager,
                     const metadata::TabletSuperBlockPB& super_block,
                     const consensus::OpId& current_id,
                     gscoped_ptr<Log> *log);

  // Reserves a spot in the log for operations in 'ops';
  // 'reserved_entry' is initialized by this method and any resources
  // associated with it will be released in AsyncAppend().  In order
  // to ensure correct ordering of operations across multiple threads,
  // calls to this method must be externally synchronized.
  //
  // WARNING: the caller _must_ call AsyncAppend() or else the log
  // will "stall" and will never be able to make forward progress.
  Status Reserve(const vector<consensus::OperationPB*>& ops,
                 LogEntry** reserved_entry);

  // Asynchronously appends 'entry' to the log. Once the append
  // completes and is synced, 'callback' will be invoked.
  Status AsyncAppend(LogEntry* entry,
                     const std::tr1::shared_ptr<FutureCallback>& callback);

  // Synchronously append a new entry to the log.
  // Log does not take ownership of the passed 'entry'.
  // TODO get rid of this method, transition to the asynchronous API
  Status Append(LogEntryPB* entry);

  // Make segments roll over
  Status RollOver();

  // Syncs all state and closes the log.
  Status Close();

  void SetMaxSegmentSizeForTests(uint64_t max_segment_size) {
    max_segment_size_ = max_segment_size;
  }

  // Returns the current header so that the log can be queried for the most
  // current config/metadata/<term,index>.
  const LogSegmentHeaderPB* current_header() const {
    return next_segment_header_.get();
  }

  const consensus::OpId& last_entry_id() const {
    return next_segment_header_->initial_id();
  }

  // Runs the garbage collector on the set of previous segments. Segments that
  // only refer to in-mem state that has been flushed are candidates for
  // garbage collection.
  Status GC();

  const WritableLogSegment* current_segment() const {
    return current_.get();
  }

  const vector<std::tr1::shared_ptr<ReadableLogSegment> > &previous_segments() const {
    return previous_;
  }

  ~Log();

 private:
  DISALLOW_COPY_AND_ASSIGN(Log);

  class AppendThread;

  Log(const LogOptions &options,
      FsManager *fs_manager,
      const string& log_path,
      gscoped_ptr<LogSegmentHeaderPB> header);

  // Initializes a new one or continues an existing log.
  Status Init();

  // Creates the name for a new segment as log-<term>-<index>
  string CreateSegmentFileName(const consensus::OpId& id);

  // Creates a new segment and sets current_ to the new one.
  Status CreateNewSegment(const LogSegmentHeaderPB& header);

  // Writes serialized contents of 'entry' to the log. Called inside
  // AppenderThread. If 'caller_owns_operation' is true, then the
  // 'operation' field of the entry will be released after the entry
  // is appended.
  // TODO once Append() is removed, 'caller_owns_operation' and
  // associated logic will no longer be needed.
  Status DoAppend(LogEntry* entry, bool caller_owns_operation = true);

  Status Sync();

  BlockingQueue<LogEntry*>* entry_queue() {
    return &entry_queue_;
  }

  LogOptions options_;
  FsManager *fs_manager_;
  string log_dir_;

  // the next segment's header
  gscoped_ptr<LogSegmentHeaderPB> next_segment_header_;
  // the current segment being written
  gscoped_ptr<WritableLogSegment> current_;
  // all previous, un-GC'd, segments
  vector<std::tr1::shared_ptr<ReadableLogSegment> > previous_;

  // the size of the header of the current segment
  uint64_t header_size_;
  // the maximum segment size, in bytes
  uint64_t max_segment_size_;

  // Reader for previous segments
  gscoped_ptr<LogReader> reader_;

  // The queue used to communicate between the thread calling
  // Reserve() and the Log Appender thread
  BlockingQueue<LogEntry*> entry_queue_;

  // Thread writing to the log
  gscoped_ptr<AppendThread> append_thread_;

  enum State {
    kLogInitialized,
    kLogWriting,
    kLogClosed
  };
  State state_;
};

// This class represents a batch of operations to be written and
// synced to the log. It is opaque to the user and is managed by the
// Log class.
class LogEntry {
 public:
  ~LogEntry();

 private:
  friend class Log;

  LogEntry(gscoped_ptr<LogEntryPB[]> phys_entries, size_t count);

  // Serializes contents of the entry to an internal buffer.
  Status Serialize();

  // Sets the callback that will be invoked after the entry is
  // appended and synced to disk
  void set_callback(const std::tr1::shared_ptr<FutureCallback>& cb) {
    callback_ = cb;
  }

  // Returns the callback that will be invoked after the entry is
  // appended and synced to disk.
  const std::tr1::shared_ptr<FutureCallback>& callback() {
    return callback_;
  }

  bool failed_to_append() const {
    return state_ == kEntryFailedToAppend;
  }

  void set_failed_to_append() {
    state_ = kEntryFailedToAppend;
  }

  // Mark the entry as reserved, but not yet ready to write to the log.
  void MarkReserved();

  // Mark the entry as ready to write to log.
  void MarkReady();

  // Wait (currently, by spinning on ready_lock_) until ready.
  void WaitForReady();

  // Returns a Slice representing the serialized contents of the
  // entry.
  Slice data() const {
    DCHECK_EQ(state_, kEntryReady);
    return Slice(buffer_);
  }

  size_t count() const { return count_; }

  // Contents of the log entries that will be written to disk.
  gscoped_ptr<LogEntryPB[]> phys_entries_;

  // Number of entries in phys_entries_
  size_t count_;

  // Callback to be invoked upon the entries being written and
  // synced to disk.
  std::tr1::shared_ptr<FutureCallback> callback_;

  // Used to coordinate the synchronizer thread and the caller
  // thread: this lock starts out locked, and is unlocked by the
  // caller thread (i.e., inside AppendThread()) once the entry is
  // fully initialized (once the callback is set and data is
  // serialized)
  base::SpinLock ready_lock_;

  // Buffer to which 'phys_entries_' are serialized by call to
  // 'Serialize()'
  faststring buffer_;

  enum State {
    kEntryInitialized,
    kEntryReserved,
    kEntrySerialized,
    kEntryReady,
    kEntryFailedToAppend
  };
  State state_;

  DISALLOW_COPY_AND_ASSIGN(LogEntry);
};

}  // namespace log
}  // namespace kudu
#endif /* KUDU_CONSENSUS_LOG_H_ */
