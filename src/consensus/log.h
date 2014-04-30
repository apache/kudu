// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_CONSENSUS_LOG_H_
#define KUDU_CONSENSUS_LOG_H_

#include <boost/thread/shared_mutex.hpp>
#include <string>
#include <vector>

#include "consensus/log_util.h"
#include "gutil/ref_counted.h"
#include "gutil/spinlock.h"
#include "util/async_util.h"
#include "util/locks.h"
#include "util/task_executor.h"
#include "util/blocking_queue.h"
#include "util/status.h"

namespace kudu {

class FsManager;
class MetricContext;

namespace log {

class LogReader;
class LogEntryBatch;
struct LogMetrics;
struct LogEntryBatchLogicalSize;

typedef BlockingQueue<LogEntryBatch*, LogEntryBatchLogicalSize> LogEntryBatchQueue;

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
//
// Methods on this class are _not_ thread-safe and must be externally
// synchronized unless otherwise noted.
//
// Note: The Log needs to be Close()d before any log-writing class is
// destroyed, otherwise the Log might hold references to these classes
// to execute the callbacks after each write.
class Log {
 public:
  static const Status kLogShutdownStatus;
  static const uint64_t kInitialLogSegmentSequenceNumber;

  // Opens or continues a log and sets 'log' to the newly built Log.
  // After a successful Open() the Log is ready to receive entries.
  static Status Open(const LogOptions &options,
                     FsManager *fs_manager,
                     const std::string& tablet_id,
                     MetricContext* parent_metric_context,
                     gscoped_ptr<Log> *log);

  ~Log();

  // Reserves a spot in the log for operations in 'ops';
  // 'reserved_entry' is initialized by this method and any resources
  // associated with it will be released in AsyncAppend().  In order
  // to ensure correct ordering of operations across multiple threads,
  // calls to this method must be externally synchronized.
  //
  // WARNING: the caller _must_ call AsyncAppend() or else the log
  // will "stall" and will never be able to make forward progress.
  Status Reserve(const vector<consensus::OperationPB*>& ops,
                 LogEntryBatch** reserved_entry);

  // Asynchronously appends 'entry' to the log. Once the append
  // completes and is synced, 'callback' will be invoked.
  Status AsyncAppend(LogEntryBatch* entry,
                     const StatusCallback& callback);

  // Like the method above, but without a callback.
  Status AsyncAppend(LogEntryBatch* entry);

  // Synchronously append a new entry to the log.
  // Log does not take ownership of the passed 'entry'.
  // TODO get rid of this method, transition to the asynchronous API
  Status Append(LogEntryPB* entry);

  // Blocks the current thread until all the entries in the log queue
  // are flushed.
  Status WaitUntilAllFlushed();

  // Kick off an asynchronous task that pre-allocates a new
  // log-segment, initializing 'allocation_future_'. To check whether
  // or not the task has finished, we can either use
  // allocation_future_ or check if allocation_state() returns
  // 'kAllocationFinished'.
  Status AsyncAllocateSegment();

  // Syncs all state and closes the log.
  Status Close();

  void SetMaxSegmentSizeForTests(uint64_t max_segment_size) {
    max_segment_size_ = max_segment_size;
  }

  void DisableAsyncAllocationForTests() {
    options_.async_preallocate_segments = false;
  }

  void DisableSync() {
    force_sync_all_ = false;
  }

  // If we previous called DisableSync(), we should restore the
  // default behavior and then call Sync() which will perform the
  // actual syncing if required.
  Status ReEnableSyncIfRequired() {
    force_sync_all_ = options_.force_fsync_all;
    return Sync();
  }

  // Get ID of tablet.
  const std::string& tablet_id() const {
    return tablet_id_;
  }

  // Returns the last-used OpId.
  // Will return Status::OK() iff there is any previous OpId in the log.
  // Otherwise, will return Status::NotFound().
  // This method will never return any other Status type.
  Status GetLastEntryOpId(consensus::OpId* op_id) const;

  // Runs the garbage collector on the set of previous segments. Segments that
  // only refer to in-mem state that has been flushed are candidates for
  // garbage collection.
  //
  // min_op_id is the minimum OpId required to be retained.
  // If successful, num_gced is set to the number of deleted log segments.
  //
  // This method is thread-safe.
  Status GC(const consensus::OpId& min_op_id, int* num_gced);

  // Returns the file system location of the currently active WAL segment.
  const std::string& ActiveSegmentPathForTests() const {
    return active_segment_->path();
  }

  // Forces the write of a header to the active segment.
  // For testing recovery when only the header has been written, but no records.
  Status WriteHeaderForTests();

  // Note: accessing this internal structure is _not_ thread-safe while
  // concurrently running GC().
  const vector<std::tr1::shared_ptr<ReadableLogSegment> >& PreviousSegmentsForTests() const {
    return previous_segments_;
  }

  // Synchronously allocate a new segment and roll the log.
  Status RollOverForTests();

 private:
  friend class LogTest;
  friend class LogTestBase;

  class AppendThread;
  class SegmentAllocationTask;

  // Log state.
  enum LogState {
    kLogInitialized,
    kLogWriting,
    kLogClosed
  };

  // State of segment (pre-) allocation.
  enum SegmentAllocationState {
    kAllocationNotStarted, // No segment allocation requested
    kAllocationInProgress, // Next segment allocation started
    kAllocationFinished // Next segment ready
  };

  Log(const LogOptions &options,
      FsManager *fs_manager,
      const string& log_path,
      const string& tablet_id,
      MetricContext* parent_metric_context);

  // Initializes a new one or continues an existing log.
  Status Init();

  // Make segments roll over.
  Status RollOver();

  // Actually makes the reservation in the queue, after entries have
  // beed added to a batch.
  Status DoReserve(gscoped_ptr<LogEntryBatchPB> entry_batch,
                   LogEntryBatch** reserved_entry);

  // Creates the name for a new segment as log-<seqno>
  string CreateSegmentFileName(uint64_t sequence_number);

  // Sets 'out' to a newly created temporary file (see
  // Env::NewTempWritableFile()) for a placeholder segment. Sets
  // 'result_path' to the fully qualified path to the unique filename
  // created for the segment.
  Status CreatePlaceholderSegment(const WritableFileOptions& opts,
                                  std::string* result_path,
                                  std::tr1::shared_ptr<WritableFile>* out);

  // Creates a new WAL segment on disk, writes the next_segment_header_ to
  // disk as the header, and sets active_segment_ to point to this new segment.
  Status SwitchToAllocatedSegment();

  // Preallocates the space for a new segment.
  Status PreAllocateNewSegment();

  // Writes serialized contents of 'entry' to the log. Called inside
  // AppenderThread. If 'caller_owns_operation' is true, then the
  // 'operation' field of the entry will be released after the entry
  // is appended.
  // TODO once Append() is removed, 'caller_owns_operation' and
  // associated logic will no longer be needed.
  Status DoAppend(LogEntryBatch* entry, bool caller_owns_operation = true);

  // Write the log header. Does not call Sync().
  Status WriteHeader(const consensus::OpId& initial_op_id);

  Status Sync();

  LogEntryBatchQueue* entry_queue() {
    return &entry_batch_queue_;
  }

  const SegmentAllocationState allocation_state() {
    boost::shared_lock<boost::shared_mutex> shared_lock(allocation_lock_);
    return allocation_state_;
  }

  LogOptions options_;
  FsManager *fs_manager_;
  string log_dir_;

  // The ID of the tablet this log is dedicated to.
  std::string tablet_id_;

  // The currently active segment being written.
  gscoped_ptr<WritableLogSegment> active_segment_;

  // The current (active) segment sequence number.
  uint64_t active_segment_sequence_number_;

  // The writable file for the next allocated segment
  std::tr1::shared_ptr<WritableFile> next_segment_file_;

  // The path for the next allocated segment.
  string next_segment_path_;

  // Lock to protect modifications to previous_segments_ and log_state_.
  mutable percpu_rwlock state_lock_;

  LogState log_state_;

  // All previous (inactive) un-GC'd segments.
  vector<std::tr1::shared_ptr<ReadableLogSegment> > previous_segments_;

  // Lock to protect last_entry_op_id_, which is constantly written but
  // read occasionally by things like consensus and log GC.
  mutable rw_spinlock last_entry_op_id_lock_;

  // The last known OpId written to this log (any segment).
  consensus::OpId last_entry_op_id_;

  // The maximum segment size, in bytes.
  uint64_t max_segment_size_;

  // The queue used to communicate between the thread calling
  // Reserve() and the Log Appender thread
  LogEntryBatchQueue entry_batch_queue_;

  // Thread writing to the log
  gscoped_ptr<AppendThread> append_thread_;

  gscoped_ptr<TaskExecutor> allocation_executor_;

  // If true, sync on all appends.
  bool force_sync_all_;

  // The future for an asynchronous log segment preallocation task.
  std::tr1::shared_ptr<kudu::Future> allocation_future_;

  // Read-write lock to protect 'allocation_state_'.
  mutable boost::shared_mutex allocation_lock_;

  SegmentAllocationState allocation_state_;

  gscoped_ptr<MetricContext> metric_context_;
  gscoped_ptr<LogMetrics> metrics_;

  DISALLOW_COPY_AND_ASSIGN(Log);
};

// This class represents a batch of operations to be written and
// synced to the log. It is opaque to the user and is managed by the
// Log class.
class LogEntryBatch {
 public:
  ~LogEntryBatch();

 private:
  friend class Log;
  friend struct LogEntryBatchLogicalSize;

  LogEntryBatch(gscoped_ptr<LogEntryBatchPB> entry_batch_pb, size_t count);

  // Serializes contents of the entry to an internal buffer.
  Status Serialize();

  // Sets the callback that will be invoked after the entry is
  // appended and synced to disk
  void set_callback(const StatusCallback& cb) {
    callback_ = cb;
  }

  // Returns the callback that will be invoked after the entry is
  // appended and synced to disk.
  const StatusCallback& callback() {
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

  // Returns the total size in bytes of the object.
  size_t total_size_bytes() const {
    return total_size_bytes_;
  }

  // Contents of the log entries that will be written to disk.
  gscoped_ptr<LogEntryBatchPB> entry_batch_pb_;

   // Total size in bytes of all entries
  const uint32_t total_size_bytes_;

  // Number of entries in 'entry_batch_pb_'
  const size_t count_;

  // Callback to be invoked upon the entries being written and
  // synced to disk.
  StatusCallback callback_;

  // Used to coordinate the synchronizer thread and the caller
  // thread: this lock starts out locked, and is unlocked by the
  // caller thread (i.e., inside AppendThread()) once the entry is
  // fully initialized (once the callback is set and data is
  // serialized)
  base::SpinLock ready_lock_;

  // Buffer to which 'phys_entries_' are serialized by call to
  // 'Serialize()'
  faststring buffer_;

  enum LogEntryState {
    kEntryInitialized,
    kEntryReserved,
    kEntrySerialized,
    kEntryReady,
    kEntryFailedToAppend
  };
  LogEntryState state_;

  DISALLOW_COPY_AND_ASSIGN(LogEntryBatch);
};

// Used by 'Log::queue_' to determine logical size of a LogEntryBatch.
struct LogEntryBatchLogicalSize {
  static size_t logical_size(const LogEntryBatch* batch) {
    return batch->total_size_bytes();
  }
};

}  // namespace log
}  // namespace kudu
#endif /* KUDU_CONSENSUS_LOG_H_ */
