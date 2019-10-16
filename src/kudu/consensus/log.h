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
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest_prod.h>

#include "kudu/common/schema.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log.pb.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/ref_counted_replicate.h"
#include "kudu/gutil/callback.h"  // IWYU pragma: keep
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/blocking_queue.h"
#include "kudu/util/faststring.h"
#include "kudu/util/locks.h"
#include "kudu/util/promise.h"
#include "kudu/util/rw_mutex.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"

namespace kudu {

class CompressionCodec;
class FsManager;
class MetricEntity;
class ThreadPool;
class WritableFile;
struct WritableFileOptions;

namespace log {

struct LogEntryBatchLogicalSize;
struct LogMetrics;
struct RetentionIndexes;
class LogEntryBatch;
class LogIndex;
class LogReader;

typedef BlockingQueue<LogEntryBatch*, LogEntryBatchLogicalSize> LogEntryBatchQueue;

// Log interface, inspired by Raft's (logcabin) Log. Provides durability to
// Kudu as a normal Write Ahead Log and also plays the role of persistent
// storage for the consensus state machine.
//
// Log uses group commit to improve write throughput and latency
// without compromising ordering and durability guarantees. A single background
// thread per Log instance is responsible for accumulating pending writes
// and flushing them to the log.
//
// This class is thread-safe unless otherwise noted.
//
// Note: The Log needs to be Close()d before any log-writing class is
// destroyed, otherwise the Log might hold references to these classes
// to execute the callbacks after each write.
class Log : public RefCountedThreadSafe<Log> {
 public:
  class LogFaultHooks;

  static const Status kLogShutdownStatus;
  static const uint64_t kInitialLogSegmentSequenceNumber;

  // Opens or continues a log and sets 'log' to the newly built Log.
  // After a successful Open() the Log is ready to receive entries.
  static Status Open(const LogOptions &options,
                     FsManager *fs_manager,
                     const std::string& tablet_id,
                     const Schema& schema,
                     uint32_t schema_version,
                     const scoped_refptr<MetricEntity>& metric_entity,
                     scoped_refptr<Log> *log);

  ~Log();

  // Synchronously append a new entry to the log.
  // Log does not take ownership of the passed 'entry'.
  Status Append(LogEntryPB* entry);

  // Append the given set of replicate messages, asynchronously.
  // This requires that the replicates have already been assigned OpIds.
  Status AsyncAppendReplicates(const std::vector<consensus::ReplicateRefPtr>& replicates,
                               const StatusCallback& callback);

  // Append the given commit message, asynchronously.
  //
  // Returns a bad status if the log is already shut down.
  Status AsyncAppendCommit(gscoped_ptr<consensus::CommitMsg> commit_msg,
                           const StatusCallback& callback);


  // Blocks the current thread until all the entries in the log queue
  // are flushed and fsynced (if fsync of log entries is enabled).
  Status WaitUntilAllFlushed();

  // Kick off an asynchronous task that pre-allocates a new
  // log-segment, setting 'allocation_status_'. To wait for the
  // result of the task, use allocation_status_.Get().
  Status AsyncAllocateSegment();

  // The closure submitted to allocation_pool_ to allocate a new segment.
  void SegmentAllocationTask();

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
    max_segment_size_ = max_segment_size;
  }

  void DisableAsyncAllocationForTests() {
    options_.async_preallocate_segments = false;
  }

  void DisableSync() {
    sync_disabled_ = true;
  }

  // If we previous called DisableSync(), we should restore the
  // default behavior and then call Sync() which will perform the
  // actual syncing if required.
  Status ReEnableSyncIfRequired() {
    sync_disabled_ = false;
    return Sync();
  }

  // Get ID of tablet.
  const std::string& tablet_id() const {
    return tablet_id_;
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
    return active_segment_->path();
  }

  // Return true if the append thread is currently active.
  bool append_thread_active_for_tests() const;

  // Forces the Log to allocate a new segment and roll over.
  // This can be used to make sure all entries appended up to this point are
  // available in closed, readable segments.
  Status AllocateSegmentAndRollOver();

  // Returns this Log's FsManager.
  FsManager* GetFsManager();

  void SetLogFaultHooksForTests(const std::shared_ptr<LogFaultHooks> &hooks) {
    log_hooks_ = hooks;
  }

  // Set the schema for the _next_ log segment.
  //
  // This method is thread-safe.
  void SetSchemaForNextLogSegment(const Schema& schema, uint32_t version);
 private:
  friend class LogTest;
  friend class LogTestBase;
  FRIEND_TEST(LogTestOptionalCompression, TestMultipleEntriesInABatch);
  FRIEND_TEST(LogTestOptionalCompression, TestReadLogWithReplacedReplicates);
  FRIEND_TEST(LogTest, TestWriteAndReadToAndFromInProgressSegment);

  class AppendThread;

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

  Log(LogOptions options, FsManager* fs_manager, std::string log_path,
      std::string tablet_id, const Schema& schema, uint32_t schema_version,
      scoped_refptr<MetricEntity> metric_entity);

  // Initializes a new one or continues an existing log.
  Status Init();

  // Make segments roll over.
  Status RollOver();

  static Status CreateBatchFromPB(LogEntryTypePB type,
                                  std::unique_ptr<LogEntryBatchPB> entry_batch_pb,
                                  std::unique_ptr<LogEntryBatch>* entry_batch);

  // Asynchronously appends 'entry_batch' to the log. Once the append
  // completes and is synced, 'callback' will be invoked.
  Status AsyncAppend(std::unique_ptr<LogEntryBatch> entry_batch,
                     const StatusCallback& callback);

  // Writes the footer and closes the current segment.
  Status CloseCurrentSegment();

  // Sets 'out' to a newly created temporary file (see
  // Env::NewTempWritableFile()) for a placeholder segment. Sets
  // 'result_path' to the fully qualified path to the unique filename
  // created for the segment.
  Status CreatePlaceholderSegment(const WritableFileOptions& opts,
                                  std::string* result_path,
                                  std::shared_ptr<WritableFile>* out);

  // Creates a new WAL segment on disk, writes the next_segment_header_ to
  // disk as the header, and sets active_segment_ to point to this new segment.
  Status SwitchToAllocatedSegment();

  // Preallocates the space for a new segment.
  Status PreAllocateNewSegment();

  // Writes serialized contents of 'entry' to the log. Called inside
  // AppenderThread.
  Status WriteBatch(LogEntryBatch* entry_batch);

  // Update footer_builder_ to reflect the log indexes seen in 'batch'.
  void UpdateFooterForBatch(LogEntryBatch* batch);

  // Update the LogIndex to include entries for the replicate messages found in
  // 'batch'. The index entry points to the offset 'start_offset' in the current
  // log segment.
  Status UpdateIndexForBatch(const LogEntryBatch& batch,
                             int64_t start_offset);

  // Replaces the last "empty" segment in 'log_reader_', i.e. the one currently
  // being written to, by the same segment once properly closed.
  Status ReplaceSegmentInReaderUnlocked();

  Status Sync();

  // Helper method to get the segment sequence to GC based on the provided 'retention' struct.
  Status GetSegmentsToGCUnlocked(RetentionIndexes retention_indexes,
                                 SegmentSequence* segments_to_gc) const;

  LogEntryBatchQueue* entry_queue() {
    return &entry_batch_queue_;
  }

  const SegmentAllocationState allocation_state() {
    shared_lock<RWMutex> l(allocation_lock_);
    return allocation_state_;
  }

  std::string LogPrefix() const;

  LogOptions options_;
  FsManager *fs_manager_;
  std::string log_dir_;

  // The ID of the tablet this log is dedicated to.
  std::string tablet_id_;

  // Lock to protect modifications to schema_ and schema_version_.
  mutable rw_spinlock schema_lock_;

  // The current schema of the tablet this log is dedicated to.
  Schema schema_;
  // The schema version
  uint32_t schema_version_;

  // The currently active segment being written.
  std::unique_ptr<WritableLogSegment> active_segment_;

  // The current (active) segment sequence number.
  uint64_t active_segment_sequence_number_;

  // The writable file for the next allocated segment
  std::shared_ptr<WritableFile> next_segment_file_;

  // The path for the next allocated segment.
  std::string next_segment_path_;

  // Lock to protect mutations to log_state_ and other shared state variables.
  mutable percpu_rwlock state_lock_;

  LogState log_state_;

  // A reader for the previous segments that were not yet GC'd.
  //
  // Will be NULL after the log is Closed().
  std::shared_ptr<LogReader> reader_;

  // Index which translates between operation indexes and the position
  // of the operation in the log.
  scoped_refptr<LogIndex> log_index_;

  // A footer being prepared for the current segment.
  // When the segment is closed, it will be written.
  LogSegmentFooterPB footer_builder_;

  // The maximum segment size, in bytes.
  uint64_t max_segment_size_;

  // The queue used to communicate between the threads appending operations
  // and the thread which actually appends them to the log.
  LogEntryBatchQueue entry_batch_queue_;

  // Thread writing to the log
  gscoped_ptr<AppendThread> append_thread_;

  std::unique_ptr<ThreadPool> allocation_pool_;

  // If true, sync on all appends.
  bool force_sync_all_;

  // If true, ignore the 'force_sync_all_' flag above.
  // This is used to disable fsync during bootstrap.
  bool sync_disabled_;

  // The status of the most recent log-allocation action.
  Promise<Status> allocation_status_;

  // Read-write lock to protect 'allocation_state_'.
  mutable RWMutex allocation_lock_;
  SegmentAllocationState allocation_state_;

  // The codec used to compress entries, or nullptr if not configured.
  const CompressionCodec* codec_;

  scoped_refptr<MetricEntity> metric_entity_;
  gscoped_ptr<LogMetrics> metrics_;

  std::shared_ptr<LogFaultHooks> log_hooks_;

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

  LogEntryBatch(LogEntryTypePB type,
                std::unique_ptr<LogEntryBatchPB> entry_batch_pb,
                size_t count);

  // Serializes contents of the entry to an internal buffer.
  void Serialize();

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

  // The highest OpId of a REPLICATE message in this batch.
  // Requires that this be a REPLICATE batch.
  consensus::OpId MaxReplicateOpId() const {
    DCHECK_EQ(REPLICATE, type_);
    int idx = entry_batch_pb_->entry_size() - 1;
    DCHECK(entry_batch_pb_->entry(idx).replicate().IsInitialized());
    return entry_batch_pb_->entry(idx).replicate().id();
  }

  void SetReplicates(const std::vector<consensus::ReplicateRefPtr>& replicates) {
    replicates_ = replicates;
  }

  // The type of entries in this batch.
  const LogEntryTypePB type_;

  // Contents of the log entries that will be written to disk.
  std::unique_ptr<LogEntryBatchPB> entry_batch_pb_;

   // Total size in bytes of all entries
  const uint32_t total_size_bytes_;

  // Number of entries in 'entry_batch_pb_'
  const size_t count_;

  // The vector of refcounted replicates.
  // Used only when type is REPLICATE, this makes sure there's at
  // least a reference to each replicate message until we're finished
  // appending.
  std::vector<consensus::ReplicateRefPtr> replicates_;

  // Callback to be invoked upon the entries being written and
  // synced to disk.
  StatusCallback callback_;

  // Buffer to which 'phys_entries_' are serialized by call to
  // 'Serialize()'
  faststring buffer_;

  DISALLOW_COPY_AND_ASSIGN(LogEntryBatch);
};

// Used by 'Log::queue_' to determine logical size of a LogEntryBatch.
struct LogEntryBatchLogicalSize {
  static size_t logical_size(const LogEntryBatch* batch) {
    return batch->total_size_bytes();
  }
};

class Log::LogFaultHooks {
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
