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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest_prod.h>

#include "kudu/common/schema.h"
#include "kudu/consensus/log.pb.h"
#include "kudu/consensus/log_metrics.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/ref_counted_replicate.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/blocking_queue.h"
#include "kudu/util/faststring.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/promise.h"
#include "kudu/util/rw_mutex.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"
#include "kudu/util/threadpool.h"

namespace kudu {

class CompressionCodec;
class FileCache;
class FsManager;
class RWFile;
namespace consensus {
class CommitMsg;
}  // namespace consensus

namespace log {

class LogEntryBatch;
class LogFaultHooks;
class LogIndex;
class LogReader;
struct LogEntryBatchLogicalSize;
struct RetentionIndexes;

// Context used by the various classes that operate on the Log.
struct LogContext {
  const std::string tablet_id;
  const std::string log_dir;

  scoped_refptr<MetricEntity> metric_entity;
  std::unique_ptr<LogMetrics> metrics;
  FsManager* fs_manager;
  FileCache* file_cache;

  std::string LogPrefix() const;
};

typedef BlockingQueue<std::unique_ptr<LogEntryBatch>, LogEntryBatchLogicalSize>
    LogEntryBatchQueue;

// State of segment allocation.
enum SegmentAllocationState {
  kAllocationNotStarted, // No segment allocation requested
  kAllocationInProgress, // Next segment allocation started
  kAllocationFinished // Next segment ready
};

// Encapsulates the logic around allocating log segments.
//
// Methods of this class are not threadsafe, unless otherwise mentioned.
// It is expected that segment allocation is driven through a single thread
// (presumably the append thread, as operations are written).
class SegmentAllocator {
 public:
  // Creates a new SegmentAllocator. The allocator isn't usable until Init() is
  // called.
  //
  // 'opts' and 'ctx' are options and various context variables that
  // are relevant for the Log for which we are allocating segments.
  //
  // 'schema' and 'schema_version' define the initial schema for the Log.
  SegmentAllocator(const LogOptions* opts,
                   const LogContext* ctx,
                   SchemaPtr schema,
                   uint32_t schema_version);

  // Initializes the SegmentAllocator using 'sequence_number' as the active
  // segment's sequence number.
  //
  // 'new_readable_segment' contains the newly active segment, reopened for reading.
  Status Init(uint64_t sequence_number,
              scoped_refptr<ReadableLogSegment>* new_readable_segment);

  // Checks whether it's time to allocate (e.g. the current segment is full)
  // and/or roll over (e.g. a previous pre-allocation has finished), and does
  // so as appropriate.
  //
  // 'write_size_bytes' is the expected size of the next write; if the active
  // segment would go above the max segment size, a new segment is allocated.
  //
  // In the event of a roll over, 'finished_segment' contains a new segment
  // reader for the just-finished segment (if there was one), and
  // 'new_readable_segment' contains the newly active segment, reopened for reading.
  Status AllocateOrRollOverIfNecessary(
      uint32_t write_size_bytes,
      scoped_refptr<ReadableLogSegment>* finished_segment,
      scoped_refptr<ReadableLogSegment>* new_readable_segment);


  // Fsyncs the currently active segment to disk.
  Status Sync();

  // Syncs the current segment and writes out the footer.
  //
  // If 'finished_segment' is not null, it will contain a new ReadableLogSegment
  // corresponding to the segment that was just finished.
  Status FinishCurrentSegment(scoped_refptr<ReadableLogSegment>* finished_segment);

  // Update the footer based on the written 'batch', e.g. to track the
  // last-written OpId.
  void UpdateFooterForBatch(const LogEntryBatch& batch);

  // Shuts down the allocator threadpool. Note that this _doesn't_ close the
  // current active segment.
  void StopAllocationThread();

  std::string LogPrefix() const { return ctx_->LogPrefix(); }

  uint64_t active_segment_sequence_number() const {
    return active_segment_sequence_number_;
  }

 private:
  friend class Log;
  friend class LogTest;
  FRIEND_TEST(LogTest, TestAutoStopIdleAppendThread);
  FRIEND_TEST(LogTest, TestWriteAndReadToAndFromInProgressSegment);
  SegmentAllocationState allocation_state() {
    shared_lock<RWMutex> l(allocation_lock_);
    return allocation_state_;
  }

  // This is not thread-safe. It is up to the caller to ensure this does not
  // interfere with the append thread's attempts to switch log segments.
  //
  // If there was a previous active segment, 'finished_segment' contains a new
  // segment reader built for that segment.
  //
  // 'new_readable_segment' contains the newly active segment, reopened for reading.
  Status AllocateSegmentAndRollOver(
      scoped_refptr<ReadableLogSegment>* finished_segment,
      scoped_refptr<ReadableLogSegment>* new_readable_segment);

  // Sets the schema and version to be used for the next allocated segment.
  void SetSchemaForNextSegment(SchemaPtr schema, uint32_t version);

  // Schedules a task to allocate a new log segment.
  // Must be called when the allocation_lock_ is held.
  Status AsyncAllocateSegmentUnlocked();

  // Task to be put onto the allocation_pool_ that allocates segments.
  void AllocationTask();

  // Creates a temporary file, populating 'next_segment_file_' and
  // 'next_segment_path_', and pre-allocating 'max_segment_size_' bytes if
  // pre-allocation is enabled.
  Status AllocateNewSegment();

  // Swaps in the next segment file as the new active segment.
  //
  // 'new_readable_segment' contains the newly active segment, reopened for reading.
  Status SwitchToAllocatedSegment(
      scoped_refptr<ReadableLogSegment>* new_readable_segment);

  // Waits for any on-going allocation to complete and rolls over onto the
  // allocated segment, swapping out the previous active segment if it existed.
  //
  // If there was a previous active segment, 'finished_segment' contains a new
  // segment reader built for that segment.
  //
  // 'new_readable_segment' contains the newly active segment, reopened for reading.
  Status RollOver(scoped_refptr<ReadableLogSegment>* finished_segment,
                  scoped_refptr<ReadableLogSegment>* new_readable_segment);

  // Hooks used to inject faults into the allocator.
  std::shared_ptr<LogFaultHooks> hooks_;

  // Descriptors for the segment file that should be used as the next active
  // segment.
  std::shared_ptr<RWFile> next_segment_file_;
  std::string next_segment_path_;

  // Contains state shared by various Log-related classs.
  const LogOptions* opts_;
  const LogContext* ctx_;

  // The maximum segment size, in bytes.
  uint64_t max_segment_size_;

  // The codec used to compress entries, or nullptr if not configured.
  const CompressionCodec* codec_ = nullptr;

  // The schema and schema version to be used for the next segment.
  mutable rw_spinlock schema_lock_;
  SchemaPtr schema_;
  uint32_t schema_version_;

  // Whether fsyncing has been disabled.
  // This is used to disable fsync during bootstrap.
  bool sync_disabled_;

  // A footer being prepared for the current segment.
  // When the segment is finished, it will be written.
  LogSegmentFooterPB footer_;

  // The currently active segment being written.
  std::unique_ptr<WritableLogSegment> active_segment_;

  // Protects allocation_state_;
  mutable RWMutex allocation_lock_;
  SegmentAllocationState allocation_state_ = kAllocationNotStarted;

  // Single-threaded threadpool on which to allocate segments.
  std::unique_ptr<ThreadPool> allocation_pool_;
  Promise<Status> allocation_status_;

  // The sequence number of the 'active' log segment.
  uint64_t active_segment_sequence_number_ = 0;
};

// Log interface, inspired by Raft's (logcabin) Log. Provides durability to
// Kudu as a normal Write Ahead Log and also plays the role of persistent
// storage for the consensus state machine.
//
// Log uses group commit to improve write throughput and latency without
// compromising ordering and durability guarantees. A single background thread
// (in AppendThread) per Log instance is responsible for accumulating pending
// writes and writing them to disk.
//
// A separate background thread (in SegmentAllocator) per Log instance is
// responsible for synchronously allocating or asynchronously pre-allocating
// segment files as written entries fill up segments.
//
// The public interface of this class is thread-safe unless otherwise noted.
//
// Note: The Log needs to be Close()d before any log-writing class is
// destroyed, otherwise the Log might hold references to these classes
// to execute the callbacks after each write.
class Log : public RefCountedThreadSafe<Log> {
 public:

  static const Status kLogShutdownStatus;
  static const uint64_t kInitialLogSegmentSequenceNumber;

  // Opens or continues a log and sets 'log' to the newly built Log.
  // After a successful Open() the Log is ready to receive entries.
  static Status Open(LogOptions options,
                     FsManager* fs_manager,
                     FileCache* file_cache,
                     const std::string& tablet_id,
                     SchemaPtr schema,
                     uint32_t schema_version,
                     const scoped_refptr<MetricEntity>& metric_entity,
                     scoped_refptr<Log> *log);

  ~Log();

  // Synchronously append a new entry to the log.
  // Log does not take ownership of the passed 'entry'.
  // This is not thread-safe.
  Status Append(LogEntryPB* entry);

  // Append the given set of replicate messages, asynchronously.
  // This requires that the replicates have already been assigned OpIds.
  Status AsyncAppendReplicates(std::vector<consensus::ReplicateRefPtr> replicates,
                               StatusCallback callback);

  // Append the given commit message, asynchronously.
  //
  // Returns a bad status if the log is already shut down.
  Status AsyncAppendCommit(const consensus::CommitMsg& commit_msg,
                           StatusCallback callback);

  // Blocks the current thread until all the entries in the log queue
  // are flushed and fsynced (if fsync of log entries is enabled).
  Status WaitUntilAllFlushed();

  // Syncs all state and closes the log.
  Status Close();

  // Return true if there is any on-disk data for the given tablet.
  static bool HasOnDiskData(FsManager* fs_manager, const std::string& tablet_id);

  // Delete all WAL data from the log associated with this tablet.
  // REQUIRES: The Log must be closed.
  static Status DeleteOnDiskData(FsManager* fs_manager, const std::string& tablet_id);

  // Removes the recovery directory and all files contained therein, if it exists.
  // Intended to be invoked after log replay successfully completes.
  static Status RemoveRecoveryDirIfExists(FsManager* fs_manager, const std::string& tablet_id);

  // Returns a reader that is able to read through the previous segments,
  // provided the log is initialized and not yet closed. After being closed,
  // this function will return NULL, but existing reader references will
  // remain live.
  std::shared_ptr<LogReader> reader() const { return reader_; }

  void SetMaxSegmentSizeForTests(uint64_t max_segment_size) {
    segment_allocator_.max_segment_size_ = max_segment_size;
  }

  // This is not thread-safe.
  void DisableSync() {
    segment_allocator_.sync_disabled_ = true;
  }

  // If we previous called DisableSync(), we should restore the
  // default behavior and then call Sync() which will perform the
  // actual syncing if required.
  // This is not thread-safe.
  Status ReEnableSyncIfRequired() {
    segment_allocator_.sync_disabled_ = false;
    return segment_allocator_.Sync();
  }

  // Get ID of tablet.
  const std::string& tablet_id() const {
    return ctx_.tablet_id;
  }

  // Runs the garbage collector on the set of previous segments. Segments that
  // only refer to in-mem state that has been flushed are candidates for
  // garbage collection.
  //
  // 'min_op_idx' is the minimum operation index required to be retained.
  // If successful, num_gced is set to the number of deleted log segments.
  //
  // This method is thread-safe.
  Status GC(RetentionIndexes retention_indexes, int* num_gced);

  // Computes the amount of bytes that would have been GC'd if Log::GC had been called.
  int64_t GetGCableDataSize(RetentionIndexes retention_indexes) const;

  // Returns a map which can be used to determine the cumulative size of log segments
  // containing entries at or above any given log index.
  //
  // For example, if the current log segments are:
  //
  //    Indexes    Size
  //    ------------------
  //    [1-100]    20MB
  //    [101-200]  15MB
  //    [201-300]  10MB
  //    [302-???]  <open>   (counts as 0MB)
  //
  // This function will return:
  //
  //    {100 => 45MB,
  //     200 => 25MB,
  //     300 => 10MB}
  //
  // In other words, an anchor on any index <= 100 would retain 45MB of logs,
  // and any anchor on 100 < index <= 200 would retain 25MB of logs, etc.
  //
  // Note that the returned values are in units of bytes, not MB.
  void GetReplaySizeMap(std::map<int64_t, int64_t>* replay_size) const;

  // Returns the total size of the current segments, in bytes.
  // Returns 0 if the log is shut down.
  int64_t OnDiskSize();

  // Returns the file system location of the currently active WAL segment.
  const std::string& ActiveSegmentPathForTests() const {
    return segment_allocator_.active_segment_->path();
  }

  // Return true if the append thread is currently active.
  bool append_thread_active_for_tests() const;

  // Forces the Log to allocate a new segment and roll over.
  // This can be used to make sure all entries appended up to this point are
  // available in closed, readable segments.
  //
  // This is not thread-safe. Used in test only.
  Status AllocateSegmentAndRollOverForTests();

  void SetLogFaultHooksForTests(const std::shared_ptr<LogFaultHooks>& hooks) {
    segment_allocator_.hooks_ = hooks;
  }

  // Set the schema for the _next_ log segment.
  //
  // This method is thread-safe.
  void SetSchemaForNextLogSegment(const Schema& schema, uint32_t version);
 private:
  friend class LogTest;
  friend class LogTestBase;
  friend class SegmentAllocator;
  FRIEND_TEST(LogTestOptionalCompression, TestMultipleEntriesInABatch);
  FRIEND_TEST(LogTestOptionalCompression, TestReadLogWithReplacedReplicates);
  FRIEND_TEST(LogTest, TestWriteAndReadToAndFromInProgressSegment);
  FRIEND_TEST(LogTest, TestAutoStopIdleAppendThread);

  class AppendThread;

  // Log state.
  enum LogState {
    kLogInitialized,
    kLogWriting,
    kLogClosed
  };

  Log(LogOptions options, LogContext ctx, SchemaPtr schema, uint32_t schema_version);

  // Initializes a new one or continues an existing log.
  Status Init();

  // Sets that the current active segment is idle.
  void SetActiveSegmentIdle();

  // Creates a new LogEntryBatch from 'entry_batch_pb'; all entries must be of
  // type 'type'.
  //
  // After the batch is appended to the log, 'cb' will be invoked with the
  // result status of the append.
  static std::unique_ptr<LogEntryBatch> CreateBatchFromPB(
      LogEntryTypePB type, const LogEntryBatchPB& entry_batch_pb,
      StatusCallback cb);

  // Asynchronously appends 'entry_batch' to the log.
  Status AsyncAppend(std::unique_ptr<LogEntryBatch> entry_batch);

  // Writes serialized contents of 'entry' to the log. This is not thread-safe.
  Status WriteBatch(LogEntryBatch* entry_batch);

  // Update footer_builder_ to reflect the log indexes seen in 'batch'.
  void UpdateFooterForBatch(LogEntryBatch* batch);

  // Update the LogIndex to include entries for the replicate messages found in
  // 'batch'. The index entry points to the offset 'start_offset' in the current
  // log segment.
  Status UpdateIndexForBatch(const LogEntryBatch& batch,
                             int64_t start_offset);

  Status Sync();

  // Helper method to get the segment sequence to GC based on the provided 'retention' struct.
  void GetSegmentsToGCUnlocked(RetentionIndexes retention_indexes,
                               SegmentSequence* segments_to_gc) const;

  LogEntryBatchQueue* entry_queue() {
    return &entry_batch_queue_;
  }

  std::string LogPrefix() const;

  LogOptions options_;
  LogContext ctx_;
  std::string log_dir_;

  // Lock to protect mutations to log_state_ and other shared state variables.
  // Generally this is used to ensure adding and removing segments from the log
  // reader is threadsafe.
  mutable percpu_rwlock state_lock_;
  LogState log_state_;

  // A reader for the previous segments that were not yet GC'd.
  //
  // Will be NULL after the log is Closed().
  std::shared_ptr<LogReader> reader_;

  // Index which translates between operation indexes and the position
  // of the operation in the log.
  scoped_refptr<LogIndex> log_index_;

  // The maximum segment size, in bytes.
  uint64_t max_segment_size_;

  // The queue used to communicate between the threads appending operations to
  // the log and the thread which actually writing the operations them to disk.
  LogEntryBatchQueue entry_batch_queue_;

  // Thread writing to the log
  std::unique_ptr<AppendThread> append_thread_;

  SegmentAllocator segment_allocator_;

  // Protects the active segment as it is going idle, in case other threads
  // attempt to switch segments concurrently. This shouldn't happen in
  // production, but may happen if AllocateSegmentAndRollOver() is called.
  mutable rw_spinlock segment_idle_lock_;

  // The cached on-disk size of the log, used to track its size even if it has been closed.
  std::atomic<int64_t> on_disk_size_;

  DISALLOW_COPY_AND_ASSIGN(Log);
};

// Indicates which log indexes should be retained for different purposes.
//
// When default-constructed, starts with maximum indexes, indicating no
// logs need to be retained for either purposes.
struct RetentionIndexes {
  explicit RetentionIndexes(int64_t durability = std::numeric_limits<int64_t>::max(),
                            int64_t peers = std::numeric_limits<int64_t>::max())
      : for_durability(durability),
        for_peers(peers) {}

  // The minimum log entry index which *must* be retained in order to
  // preserve durability and the ability to restart the local node
  // from its WAL.
  int64_t for_durability;

  // The minimum log entry index which *should* be retained in order to
  // catch up other peers hosting this same tablet. These entries may
  // still be GCed in the case that they are from very old log segments
  // or the log has become too large.
  int64_t for_peers;
};


// This class represents a batch of operations to be written and
// synced to the log. It is opaque to the user and is managed by the
// Log class.
//
// A single batch must have only one type of entries in it (eg only
// REPLICATEs or only COMMITs).
//
// The ReplicateMsg sub-elements of each LogEntryPB within the LogEntryBatchPB
// 'entry_batch_pb_' are not owned by the LogEntryPBs, and at LogEntryBatch
// destruction time they are released.
class LogEntryBatch {
 public:
  ~LogEntryBatch();

 private:
  friend class Log;
  friend struct LogEntryBatchLogicalSize;
  friend class MultiThreadedLogTest;
  friend class SegmentAllocator;

  LogEntryBatch(LogEntryTypePB type, const LogEntryBatchPB& entry_batch_pb,
                StatusCallback cb);

  // Serializes contents of the entry to an internal buffer.
  void Serialize();

  // Returns a Slice representing the serialized contents of the
  // entry.
  Slice data() const {
    return Slice(buffer_);
  }

  size_t count() const { return count_; }

  // Returns the total size in bytes of the object.
  size_t total_size_bytes() const {
    return total_size_bytes_;
  }

  void SetAppendError(const Status& s) {
    DCHECK(!s.ok());
    if (append_status_.ok()) {
      append_status_ = s;
    }
  }

  void RunCallback() {
    callback_(append_status_);
  }

  // The type of entries in this batch.
  const LogEntryTypePB type_;

   // Total size in bytes of all entries
  const size_t total_size_bytes_;

  // Number of entries in the serialized batch.
  const size_t count_;

  // Used only when type is REPLICATE, the opids of the serialized
  // ReplicateMsgs.
  std::vector<consensus::OpId> replicate_op_ids_;

  // Callback to be invoked upon the entries being written and
  // synced to disk.
  StatusCallback callback_;

  // Buffer to which 'phys_entries_' are serialized by call to
  // 'Serialize()'
  faststring buffer_;

  // Tracks whether this batch was successfully append to the log.
  Status append_status_;

  DISALLOW_COPY_AND_ASSIGN(LogEntryBatch);
};

// Used by 'Log::queue_' to determine logical size of a LogEntryBatch.
struct LogEntryBatchLogicalSize {
  static size_t logical_size(const std::unique_ptr<LogEntryBatch>& batch) {
    return batch->total_size_bytes();
  }
};

class LogFaultHooks {
 public:

  // Executed immediately before returning from Log::Sync() at *ALL*
  // times.
  virtual Status PostSync() { return Status::OK(); }

  // Iff fsync is enabled, executed immediately after call to fsync.
  virtual Status PostSyncIfFsyncEnabled() { return Status::OK(); }

  // Emulate a slow disk where the filesystem has decided to synchronously
  // flush a full buffer.
  virtual Status PostAppend() { return Status::OK(); }

  virtual Status PreClose() { return Status::OK(); }
  virtual Status PostClose() { return Status::OK(); }

  virtual ~LogFaultHooks() {}
};

}  // namespace log
}  // namespace kudu
