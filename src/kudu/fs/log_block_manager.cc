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

#include "kudu/fs/log_block_manager.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <ostream>
#include <set>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <rocksdb/write_batch.h>

#include "kudu/fs/block_manager_metrics.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/fs/dir_manager.h"
#include "kudu/fs/dir_util.h"
#include "kudu/fs/error_manager.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/fs/fs_report.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/alignment.h"
#include "kudu/util/array_view.h"
#include "kudu/util/env.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/file_cache.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/flag_validators.h"
#include "kudu/util/locks.h"
#include "kudu/util/malloc.h"
#include "kudu/util/metrics.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/rw_mutex.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/slice.h"
#include "kudu/util/sorted_disjoint_interval_list.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util_prod.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/trace.h"

DECLARE_bool(enable_data_block_fsync);
DECLARE_string(block_manager_preflush_control);

// TODO(unknown): How should this be configured? Should provide some guidance.
DEFINE_uint64(log_container_max_size, 10LU * 1024 * 1024 * 1024,
              "Maximum size (soft) of a log container");
TAG_FLAG(log_container_max_size, advanced);

DEFINE_uint64(log_container_metadata_max_size, 0,
              "Maximum size (soft) of a log container's metadata. Use 0 for "
              "no limit.");
TAG_FLAG(log_container_metadata_max_size, advanced);
TAG_FLAG(log_container_metadata_max_size, experimental);
TAG_FLAG(log_container_metadata_max_size, runtime);

DEFINE_bool(log_container_metadata_runtime_compact, false,
            "Whether to enable metadata file compaction at runtime.");
TAG_FLAG(log_container_metadata_runtime_compact, advanced);
TAG_FLAG(log_container_metadata_runtime_compact, experimental);
TAG_FLAG(log_container_metadata_runtime_compact, runtime);

DEFINE_int64(log_container_max_blocks, -1,
             "Maximum number of blocks (soft) of a log container. Use 0 for "
             "no limit. Use -1 for no limit except in the case of a kernel "
             "bug with hole punching on ext4 (see KUDU-1508 for details).");
TAG_FLAG(log_container_max_blocks, advanced);

DEFINE_uint64(log_container_preallocate_bytes, 32LU * 1024 * 1024,
              "Number of bytes to preallocate in a log container when "
              "creating new blocks. Set to 0 to disable preallocation");
TAG_FLAG(log_container_preallocate_bytes, advanced);

DEFINE_uint64(log_container_delete_batch_count, 256,
              "");

DEFINE_double(log_container_excess_space_before_cleanup_fraction, 0.10,
              "Additional fraction of a log container's calculated size that "
              "must be consumed on disk before the container is considered to "
              "be inconsistent and subject to excess space cleanup at block "
              "manager startup.");
TAG_FLAG(log_container_excess_space_before_cleanup_fraction, advanced);

DEFINE_double(log_container_live_metadata_before_compact_ratio, 0.50,
              "Desired ratio of live block metadata in log containers. If a "
              "container's live to total block ratio dips below this value, "
              "the container's metadata file will be compacted at startup, "
              "or at runtime, based on the configuration of "
              "--log_container_metadata_size_before_compact_ratio.");
TAG_FLAG(log_container_live_metadata_before_compact_ratio, experimental);

DEFINE_double(log_container_metadata_size_before_compact_ratio, 0.80,
              "Desired portion of --log_container_metadata_max_size container metadata must "
              "consume before metadata is considered for compaction. If a container's metadata "
              "file exceeds --log_container_metadata_max_size * "
              "--log_container_metadata_size_before_compact_ratio, and the number of live blocks "
              "falls below --log_container_live_metadata_before_compact_ratio, the container's "
              "metadata will be compacted.");
TAG_FLAG(log_container_metadata_size_before_compact_ratio, advanced);
TAG_FLAG(log_container_metadata_size_before_compact_ratio, experimental);

DEFINE_bool(log_block_manager_test_hole_punching, true,
            "Ensure hole punching is supported by the underlying filesystem");
TAG_FLAG(log_block_manager_test_hole_punching, advanced);
TAG_FLAG(log_block_manager_test_hole_punching, unsafe);

DEFINE_bool(log_block_manager_delete_dead_container, true,
            "When enabled, full and dead log block containers will be deleted "
            "at runtime, which can potentially help improving log block manager "
            "startup time");
TAG_FLAG(log_block_manager_delete_dead_container, advanced);
TAG_FLAG(log_block_manager_delete_dead_container, experimental);

DEFINE_int32(log_container_metadata_rewrite_inject_latency_ms, 0,
             "Amount of latency in ms to inject when rewrite metadata file. "
             "Only for testing.");
TAG_FLAG(log_container_metadata_rewrite_inject_latency_ms, hidden);

METRIC_DEFINE_gauge_uint64(server, log_block_manager_bytes_under_management,
                           "Bytes Under Management",
                           kudu::MetricUnit::kBytes,
                           "Number of bytes of data blocks currently under management",
                           kudu::MetricLevel::kInfo);

METRIC_DEFINE_gauge_uint64(server, log_block_manager_blocks_under_management,
                           "Blocks Under Management",
                           kudu::MetricUnit::kBlocks,
                           "Number of data blocks currently under management",
                           kudu::MetricLevel::kInfo);

METRIC_DEFINE_gauge_uint64(server, log_block_manager_containers,
                           "Number of Block Containers",
                           kudu::MetricUnit::kLogBlockContainers,
                           "Number of log block containers",
                           kudu::MetricLevel::kInfo);

METRIC_DEFINE_gauge_uint64(server, log_block_manager_full_containers,
                           "Number of Full Block Containers",
                           kudu::MetricUnit::kLogBlockContainers,
                           "Number of full log block containers",
                           kudu::MetricLevel::kInfo);

METRIC_DEFINE_counter(server, log_block_manager_holes_punched,
                      "Number of Holes Punched",
                      kudu::MetricUnit::kHoles,
                      "Number of holes punched since service start",
                      kudu::MetricLevel::kDebug);

METRIC_DEFINE_counter(server, log_block_manager_dead_containers_deleted,
                      "Number of Dead Block Containers Deleted",
                      kudu::MetricUnit::kLogBlockContainers,
                      "Number of full (but dead) block containers that were deleted",
                      kudu::MetricLevel::kDebug);

METRIC_DEFINE_gauge_uint64(server, log_block_manager_total_containers_startup,
                           "Total number of Log Block Containers during startup",
                           kudu::MetricUnit::kLogBlockContainers,
                           "Number of log block containers which were present during the server "
                           "startup",
                           kudu::MetricLevel::kInfo);

METRIC_DEFINE_gauge_uint64(server, log_block_manager_processed_containers_startup,
                           "Number of Log Block Containers opened during startup",
                           kudu::MetricUnit::kLogBlockContainers,
                           "Number of log block containers which were opened/processed during "
                           "the server startup",
                           kudu::MetricLevel::kInfo);

namespace kudu {

namespace fs {

using internal::LogBlock;
using internal::LogBlockContainer;
using internal::LogfBlockContainer;
using internal::LogrBlockContainer;
using internal::LogBlockDeletionTransaction;
using internal::LogWritableBlock;
using pb_util::ReadablePBContainerFile;
using pb_util::WritablePBContainerFile;
using std::accumulate;
using std::map;
using std::optional;
using std::set;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::SkipEmpty;
using strings::Split;
using strings::Substitute;

bool ValidateMetadataCompactFlags() {
  if (FLAGS_log_container_metadata_runtime_compact &&
      FLAGS_log_container_metadata_size_before_compact_ratio <= 0) {
    LOG(ERROR) << Substitute(
        "--log_container_metadata_size_before_compact_ratio ($0) must be greater than 0 when "
        "enabling --log_container_metadata_runtime_compact",
        FLAGS_log_container_metadata_size_before_compact_ratio);
    return false;
  }
  return true;
}
GROUP_FLAG_VALIDATOR(metadata_compact_flags, ValidateMetadataCompactFlags);

namespace internal {

////////////////////////////////////////////////////////////
// LogBlockManagerMetrics
////////////////////////////////////////////////////////////

// Metrics container associated with the log block manager.
//
// Includes implementation-agnostic metrics as well as some that are
// specific to the log block manager.
struct LogBlockManagerMetrics {
  explicit LogBlockManagerMetrics(const scoped_refptr<MetricEntity>& metric_entity);

  // Implementation-agnostic metrics.
  BlockManagerMetrics generic_metrics;

  scoped_refptr<AtomicGauge<uint64_t>> bytes_under_management;
  scoped_refptr<AtomicGauge<uint64_t>> blocks_under_management;

  scoped_refptr<AtomicGauge<uint64_t>> containers;
  scoped_refptr<AtomicGauge<uint64_t>> full_containers;

  scoped_refptr<AtomicGauge<uint64_t>> total_containers_startup;
  scoped_refptr<AtomicGauge<uint64_t>> processed_containers_startup;

  scoped_refptr<Counter> holes_punched;
  scoped_refptr<Counter> dead_containers_deleted;
};

#define MINIT(x) x(METRIC_log_block_manager_##x.Instantiate(metric_entity))
#define GINIT(x) x(METRIC_log_block_manager_##x.Instantiate(metric_entity, 0))
LogBlockManagerMetrics::LogBlockManagerMetrics(const scoped_refptr<MetricEntity>& metric_entity)
  : generic_metrics(metric_entity),
    GINIT(bytes_under_management),
    GINIT(blocks_under_management),
    GINIT(containers),
    GINIT(full_containers),
    GINIT(total_containers_startup),
    GINIT(processed_containers_startup),
    MINIT(holes_punched),
    MINIT(dead_containers_deleted) {
}
#undef GINIT

////////////////////////////////////////////////////////////
// LogBlock (declaration)
////////////////////////////////////////////////////////////

// The persistent metadata that describes a logical block.
//
// A block grows a LogBlock when its data has been synchronized with
// the disk. That's when it's fully immutable (i.e. none of its metadata
// can change), and when it becomes readable and persistent.
//
// LogBlocks are reference counted to simplify support for deletion with
// outstanding readers. All refcount increments are performed with the
// block manager lock held, as are deletion-based decrements. However,
// no lock is held when ~LogReadableBlock decrements the refcount, thus it
// must be made thread safe (by extending RefCountedThreadSafe instead of
// the simpler RefCounted).
class LogBlock : public RefCountedThreadSafe<LogBlock> {
 public:
  LogBlock(LogBlockContainerRefPtr container, BlockId block_id, int64_t offset,
           int64_t length);
  ~LogBlock() = default;

  const BlockId& block_id() const { return block_id_; }
  LogBlockContainer* container() const { return container_.get(); }
  int64_t offset() const { return offset_; }
  int64_t length() const { return length_; }

  // Returns a block's length aligned to the nearest filesystem block size.
  int64_t fs_aligned_length() const;

  // Registers the deletion of the block with a deletion transaction. Actual
  // deletion will take place when the deletion transaction is destructed.
  void RegisterDeletion(const shared_ptr<LogBlockDeletionTransaction>& transaction);

 private:
  // The owning container.
  LogBlockContainerRefPtr container_;

  // The block identifier.
  const BlockId block_id_;

  // The block's offset in the container.
  const int64_t offset_;

  // The block's length.
  const int64_t length_;

  // The block deletion transaction with which this block has been registered.
  shared_ptr<LogBlockDeletionTransaction> transaction_;

  DISALLOW_COPY_AND_ASSIGN(LogBlock);
};

////////////////////////////////////////////////////////////
// LogWritableBlock (declaration)
////////////////////////////////////////////////////////////

// A log-backed block that has been opened for writing.
//
// There's no reference to a LogBlock as this block has yet to be
// persisted.
class LogWritableBlock : public WritableBlock {
 public:
  LogWritableBlock(LogBlockContainerRefPtr container, BlockId block_id,
                   int64_t block_offset);

  ~LogWritableBlock();

  Status Close() override;

  Status Abort() override;

  const BlockId& id() const override;

  BlockManager* block_manager() const override;

  Status Append(const Slice& data) override;

  Status AppendV(ArrayView<const Slice> data) override;

  Status Finalize() override;

  size_t BytesAppended() const override;

  State state() const override;

  // Actually close the block, finalizing it if it has not yet been
  // finalized. Also updates various metrics.
  //
  // This is called after synchronization of dirty data and metadata
  // to disk.
  void DoClose();

  // Starts an asynchronous flush of dirty block data to disk.
  Status FlushDataAsync();

  LogBlockContainer* container() const { return container_.get(); }

  int64_t block_offset() const { return block_offset_; }

  int64_t block_length() const { return block_length_; }

 private:
  // The owning container.
  LogBlockContainerRefPtr container_;

  // The block's identifier.
  const BlockId block_id_;

  // The block's offset within the container. Known from the moment the
  // block is created.
  const int64_t block_offset_;

  // The block's length. Changes with each Append().
  int64_t block_length_;

  // The state of the block describing where it is in the write lifecycle,
  // for example, has it been synchronized to disk?
  WritableBlock::State state_;

  DISALLOW_COPY_AND_ASSIGN(LogWritableBlock);
};

////////////////////////////////////////////////////////////
// LogBlockContainer
////////////////////////////////////////////////////////////

// A single block container belonging to the log-backed block manager.
//
// A container may only be used to write one WritableBlock at a given time.
// However, existing blocks may be deleted concurrently. As such, almost
// all container functions must be reentrant, even if the container itself
// is logically thread unsafe (i.e. multiple clients calling WriteData()
// concurrently will produce nonsensical container data). Thread unsafe
// functions are marked explicitly.
//
// Block containers are reference counted so that they can be safely removed
// despite concurrent access.
class LogBlockContainer: public RefCountedThreadSafe<LogBlockContainer> {
 public:
  enum SyncMode {
    SYNC,
    NO_SYNC
  };

  // The destructor will delete files of this container if it is dead.
  virtual ~LogBlockContainer() = default;

  // Closes a set of blocks belonging to this container, possibly synchronizing
  // the dirty data and metadata to disk.
  //
  // If successful, adds all blocks to the block manager's in-memory maps.
  Status DoCloseBlocks(const vector<LogWritableBlock*>& blocks, SyncMode mode);

  // Frees the space associated with a block or a group of blocks at 'offset'
  // and 'length'. This is a physical operation, not a logical one; a separate
  // AppendMetadataForBatchDelete() is required to record the deletion in
  // container metadata.
  //
  // The on-disk effects of this call are made durable only after SyncData().
  Status PunchHole(int64_t offset, int64_t length);

  // Executes a hole punching operation at 'offset' with the given 'length'.
  void ContainerDeletionAsync(int64_t offset, int64_t length);

  // Preallocate enough space to ensure that an append of 'next_append_length'
  // can be satisfied by this container. The offset of the beginning of this
  // block must be provided in 'block_start_offset' (since container
  // bookkeeping is only updated when a block is finished).
  //
  // Does nothing if preallocation is disabled.
  Status EnsurePreallocated(int64_t block_start_offset,
                            size_t next_append_length);

  // See RWFile::Write()
  Status WriteData(int64_t offset, const Slice& data);

  // See RWFile::WriteV()
  Status WriteVData(int64_t offset, ArrayView<const Slice> data);

  // See RWFile::Read().
  Status ReadData(int64_t offset, Slice result) const;

  // See RWFile::ReadV().
  Status ReadVData(int64_t offset, ArrayView<Slice> results) const;

  // Appends 'pb' to this container's metadata file.
  //
  // The on-disk effects of this call are made durable only after SyncMetadata().
  virtual Status AppendMetadataForBatchDelete(const vector<BlockId>& block_ids) = 0;

  virtual Status AppendMetadataForBatchCreate(const vector<LogWritableBlock*>& blocks) = 0;

  // Asynchronously flush this container's data file from 'offset' through
  // to 'length'.
  //
  // Does not guarantee data durability; use SyncData() for that.
  Status FlushData(int64_t offset, int64_t length);

  // Synchronize this container's data file with the disk. On success,
  // guarantees that the data is made durable.
  //
  // TODO(unknown): Add support to synchronize just a range.
  Status SyncData();

  // Synchronize this container's metadata file with the disk. On success,
  // guarantees that the metadata is made durable.
  //
  // TODO(unknown): Add support to synchronize just a range.
  virtual Status SyncMetadata() = 0;

  // Reopen the metadata file record writer. Should be called if the underlying
  // file was changed.
  virtual Status ReopenMetadataWriter() {
    return Status::OK();
  }

  // Truncates this container's data file to 'next_block_offset_' if it is
  // full. This effectively removes any preallocated but unused space.
  //
  // Should be called only when 'next_block_offset_' is up-to-date with
  // respect to the data on disk (i.e. after the container's records have
  // been loaded), otherwise data may be lost!
  //
  // This function is thread unsafe.
  Status TruncateDataToNextBlockOffset();

  // Whether to update internal counters based on processed records. This may be
  // useful to avoid recomputing container statistics during operations that
  // don't change them, e.g. compacting container metadata.
  enum class ProcessRecordType {
    kReadOnly,       // Read records only.
    kReadAndUpdate,  // Read records and update container's statistic.
  };
  // Reads the container's metadata from disk, sanity checking and processing
  // records along the way.
  //
  // Malformed records and other container inconsistencies are written to
  // 'report'. Healthy blocks are written either to 'live_blocks' or
  // 'dead_blocks'. Live records are written to 'live_block_records'. The
  // greatest block ID seen thus far in the container is written to 'max_block_id'.
  //
  // Returns an error only if there was a problem accessing the container from
  // disk; such errors are fatal and effectively halt processing immediately.
  virtual Status ProcessRecords(
      FsReport* report,
      LogBlockManager::UntrackedBlockMap* live_blocks,
      LogBlockManager::BlockRecordMap* live_block_records,
      vector<LogBlockRefPtr>* dead_blocks,
      uint64_t* max_block_id,
      ProcessRecordType type) = 0;

  // Updates internal bookkeeping state to reflect the creation of a block.
  void BlockCreated(const LogBlockRefPtr& block);

  // Updates internal bookkeeping state to reflect the deletion of a block.
  //
  // This function is thread safe because block deletions can happen concurrently
  // with creations.
  //
  // Note: the container is not made "unfull"; containers remain sparse until deleted.
  void BlockDeleted(const LogBlockRefPtr& block);

  // Finalizes a fully written block. It updates the container data file's position,
  // truncates the container if full and marks the container as available.
  void FinalizeBlock(int64_t block_offset, int64_t block_length);

  // Runs a task on this container's data directory thread pool.
  //
  // Normally the task is performed asynchronously. However, if submission to
  // the pool fails, it runs synchronously on the current thread.
  void ExecClosure(const std::function<void()>& task);

  // Produces a debug-friendly string representation of this container.
  string ToString() const;

  // Makes the container read-only and stores the responsible error.
  void SetReadOnly(const Status& error);

  // Handles errors if the input status is not OK.
  void HandleError(const Status& s) const;

  // Returns whether or not the container has been marked read-only.
  bool read_only() const {
    return !read_only_status().ok();
  }

  // Returns the error that caused the container to become read-only, or OK if
  // the container has not been marked read-only.
  Status read_only_status() const {
    std::lock_guard<simple_spinlock> l(read_only_lock_);
    return read_only_status_;
  }

  // Simple accessors.
  LogBlockManager* block_manager() const { return block_manager_; }
  const string& id() const { return id_; }
  int64_t next_block_offset() const { return next_block_offset_.Load(); }
  int64_t total_bytes() const { return total_bytes_.Load(); }
  int64_t total_blocks() const { return total_blocks_.Load(); }
  int64_t live_bytes() const { return live_bytes_.Load(); }
  int64_t live_bytes_aligned() const { return live_bytes_aligned_.Load(); }
  int64_t live_blocks() const { return live_blocks_.Load(); }
  int32_t blocks_being_written() const { return blocks_being_written_.Load(); }
  virtual bool full() const {
    return next_block_offset() >= FLAGS_log_container_max_size ||
           (max_num_blocks_ && total_blocks() >= max_num_blocks_);
  }

  bool dead() const { return dead_.Load(); }
  const LogBlockManagerMetrics* metrics() const { return metrics_; }
  Dir* data_dir() const { return data_dir_; }
  const DirInstanceMetadataPB* instance() const { return data_dir_->instance()->metadata(); }

  // Adjusts the number of blocks being written.
  // Positive means increase, negative means decrease.
  int32_t blocks_being_written_incr(int32_t value) {
    return blocks_being_written_.IncrementBy(value);
  }

  // Check that the container meets the death condition.
  //
  // Although this code looks like a TOCTOU violation, it is safe because of
  // some additional LBM invariants:
  // 1) When a container becomes full, it stays full for the process' lifetime.
  // 2) A full container will never accrue another live block. Meaning, losing
  //    its last live block is a terminal state for a full container.
  // 3) The only exception to #2 is if the container currently has a finalized
  //    but not-yet-closed WritableBlock. In this case the container became full
  //    when the WritableBlock was finalized, but the live block counter only
  //    reflects the new block when it is closed.
  bool check_death_condition() const {
    return (full() && live_blocks() == 0 && blocks_being_written() == 0 &&
            FLAGS_log_block_manager_delete_dead_container);
  }

  // Tries to mark the container as 'dead', which means it will be deleted
  // when it goes out of scope. Can only be set dead once.
  //
  // If successful, returns true; otherwise returns false.
  bool TrySetDead() {
    if (dead()) return false;
    return dead_.CompareAndSet(false, true);
  }

  virtual bool ShouldCompact() const = 0;

  virtual void CompactMetadata() {}

  static std::vector<BlockRecordPB> SortRecords(LogBlockManager::BlockRecordMap live_block_records);

 protected:
  LogBlockContainer(LogBlockManager* block_manager, Dir* data_dir, const string& id,
                    shared_ptr<RWFile> data_file, uint64_t data_file_size);

  // Processes a single block record, performing sanity checks on it and adding
  // it either to 'live_blocks' or 'dead_blocks'. If the record is live, it is
  // added to 'live_block_records'.
  //
  // Returns an error only if there was a problem accessing the container from
  // disk; such errors are fatal and effectively halt processing immediately.
  //
  // On success, 'report' is updated with any inconsistencies found in the
  // record, 'data_file_size' may be updated with the latest size of the
  // container's data file, and 'max_block_id' reflects the largest block ID
  // seen thus far in the container.
  //
  // Note: 'record' may be swapped into 'report'; do not use it after calling
  // this function.
  Status ProcessRecord(
      BlockRecordPB* record,
      FsReport* report,
      LogBlockManager::UntrackedBlockMap* live_blocks,
      LogBlockManager::BlockRecordMap* live_block_records,
      vector<LogBlockRefPtr>* dead_blocks,
      uint64_t* data_file_size,
      uint64_t* max_block_id,
      ProcessRecordType type);

  // Updates this container data file's position based on the offset and length
  // of a block, marking this container as full if needed. Should only be called
  // when a block is fully written, or after an encryption header is written, as
  // it will round up the container data file's position.
  //
  // This function is thread unsafe.
  void UpdateNextBlockOffset(int64_t block_offset, int64_t block_length);

  // The owning block manager. Must outlive the container itself.
  LogBlockManager* const block_manager_;

  // The data directory where the container lives.
  Dir* data_dir_;

  string id_;

  const optional<int64_t> max_num_blocks_;

  // Offset up to which we have preallocated bytes.
  int64_t preallocated_offset_ = 0;

  shared_ptr<RWFile> data_file_;

  // The offset of the next block to be written to the container.
  AtomicInt<int64_t> next_block_offset_;

  // The amount of data (post block alignment) written thus far to the container.
  AtomicInt<int64_t> total_bytes_;

  // The number of blocks written thus far in the container.
  AtomicInt<int64_t> total_blocks_;

  // The amount of data present in not-yet-deleted blocks of the container.
  AtomicInt<int64_t> live_bytes_;

  // The amount of data (post block alignment) present in not-yet-deleted
  // blocks of the container.
  AtomicInt<int64_t> live_bytes_aligned_;

  // The number of not-yet-deleted blocks in the container.
  AtomicInt<int64_t> live_blocks_;

  // The number of LogWritableBlocks currently open for this container.
  AtomicInt<int32_t> blocks_being_written_;

  // Whether or not this container has been marked as dead.
  AtomicBool dead_;

  // The metrics. Not owned by the log container; it has the same lifespan
  // as the block manager.
  const LogBlockManagerMetrics* metrics_;

  // If true, only read operations are allowed. Existing blocks may
  // not be deleted until the next restart, and new blocks may not
  // be added.
  mutable simple_spinlock read_only_lock_;
  Status read_only_status_;

  DISALLOW_COPY_AND_ASSIGN(LogBlockContainer);
};

////////////////////////////////////////////////////////////
// LogfBlockContainer
////////////////////////////////////////////////////////////

// A single block container belonging to the log-backed block manager.
//
// A container may only be used to write one WritableBlock at a given time.
// However, existing blocks may be deleted concurrently. As such, almost
// all container functions must be reentrant, even if the container itself
// is logically thread unsafe (i.e. multiple clients calling WriteData()
// concurrently will produce nonsensical container data). Thread unsafe
// functions are marked explicitly.
//
// Block containers are reference counted so that they can be safely removed
// despite concurrent access.
class LogfBlockContainer: public LogBlockContainer {
 public:

  // Creates a new block container in 'dir'.
  static Status Create(LogBlockManager* block_manager,
                       Dir* dir,
                       LogBlockContainerRefPtr* container);

  // Opens an existing block container in 'dir'.
  //
  // Every container is comprised of two files: "<dir>/<id>.data" and
  // "<dir>/<id>.metadata". Together, 'dir' and 'id' fully describe both
  // files.
  //
  // Returns Status::Aborted() in the case that the metadata and data files
  // both appear to have no data (e.g. due to a crash just after creating
  // one of them but before writing any records). This is recorded in 'report'.
  static Status Open(LogBlockManager* block_manager,
                     Dir* dir,
                     FsReport* report,
                     const string& id,
                     LogBlockContainerRefPtr* container);

  // The destructor will delete files of this container if it is dead.
  ~LogfBlockContainer();

  Status AppendMetadataForBatchDelete(const vector<BlockId>& block_ids) override;

  Status AppendMetadataForBatchCreate(const vector<LogWritableBlock*>& blocks) override;

  // Synchronize this container's metadata file with the disk. On success,
  // guarantees that the metadata is made durable.
  //
  // TODO(unknown): Add support to synchronize just a range.
  Status SyncMetadata() override;

  // Reopen the metadata file record writer. Should be called if the underlying
  // file was changed.
  Status ReopenMetadataWriter() override;

  // Reads the container's metadata from disk, sanity checking and processing
  // records along the way.
  //
  // Malformed records and other container inconsistencies are written to
  // 'report'. Healthy blocks are written either to 'live_blocks' or
  // 'dead_blocks'. Live records are written to 'live_block_records'. The
  // greatest block ID seen thus far in the container is written to 'max_block_id'.
  //
  // Returns an error only if there was a problem accessing the container from
  // disk; such errors are fatal and effectively halt processing immediately.
  Status ProcessRecords(
      FsReport* report,
      LogBlockManager::UntrackedBlockMap* live_blocks,
      LogBlockManager::BlockRecordMap* live_block_records,
      vector<LogBlockRefPtr>* dead_blocks,
      uint64_t* max_block_id,
      ProcessRecordType type) override;

  bool full() const override {
    if (LogBlockContainer::full()) {
      return true;
    }

    if (FLAGS_log_container_metadata_max_size <= 0) {
      return false;
    }

    // Try lock before reading metadata offset, consider it not full if lock failed.
    shared_lock<RWMutex> l(metadata_compact_lock_, std::try_to_lock);
    if (!l.owns_lock()) {
      return false;
    }
    return metadata_file_->Offset() >= FLAGS_log_container_metadata_max_size;
  }

  bool ShouldCompact() const override {
    shared_lock<RWMutex> l(metadata_compact_lock_);
    return ShouldCompactUnlocked();
  }

  void CompactMetadata() override;

 private:
  LogfBlockContainer(LogBlockManager* block_manager, Dir* data_dir, const string& id,
                     shared_ptr<RWFile> data_file, uint64_t data_file_size,
                     unique_ptr<WritablePBContainerFile> metadata_file);

  // Check the container whether it is fine.
  //
  // OK: both files of the container exist;
  // Aborted: the container will be repaired later;
  // NotFound: one file of the container has gone missing;
  // *-------------*------------*----------------*-------------------------*---------------------*
  // |DATA\METADATA| NONE EXIST | EXIST && < MIN | EXIST && NO LIVE BLOCKS | EXIST && LIVE BLOCKS|
  // *-------------*------------*----------------*-------------------------*---------------------*
  // | NONE EXIST  |            |    Aborted     |      Aborted            |      NotFound       |
  // *-------------*------------*----------------*-------------------------*---------------------*
  // | EXIST && 0  |  Aborted   |    Aborted     |      OK                 |      OK             |
  // *-------------*------------*----------------*-------------------------*---------------------*
  // | EXIST && >0 |  NotFound  |    OK          |      OK                 |      OK             |
  // *-------------*------------*----------------*-------------------------*---------------------*
  //
  // Note: the status here only represents the result of check.
  static Status CheckContainerFiles(LogBlockManager* block_manager,
                                    FsReport* report,
                                    const Dir* dir,
                                    const string& id,
                                    const string& common_path,
                                    const string& data_path,
                                    const string& metadata_path);

  bool ShouldCompactUnlocked() const {
    DCHECK_GT(FLAGS_log_container_metadata_max_size, 0);
    if (live_blocks() >=
        total_blocks() * FLAGS_log_container_live_metadata_before_compact_ratio) {
      return false;
    }

    return metadata_file_->Offset() >= FLAGS_log_container_metadata_max_size *
                                           FLAGS_log_container_metadata_size_before_compact_ratio;
  }

  // Protect 'metadata_file_', only rewriting should add write lock,
  // appending and syncing only need read lock, cause there is an
  // internal lock for these operations in WritablePBContainerFile.
  mutable RWMutex metadata_compact_lock_;

  // Opened file handles to the container's files.
  unique_ptr<WritablePBContainerFile> metadata_file_;

  // TODO(yingchun): add metadata bytes for metadata.
  //  AtomicInt<int64_t> metadata_bytes_;

  DISALLOW_COPY_AND_ASSIGN(LogfBlockContainer);
};

////////////////////////////////////////////////////////////
// LogrBlockContainer
////////////////////////////////////////////////////////////

// A single block container belonging to the log-backed block manager.
//
// A container may only be used to write one WritableBlock at a given time.
// However, existing blocks may be deleted concurrently. As such, almost
// all container functions must be reentrant, even if the container itself
// is logically thread unsafe (i.e. multiple clients calling WriteData()
// concurrently will produce nonsensical container data). Thread unsafe
// functions are marked explicitly.
//
// Block containers are reference counted so that they can be safely removed
// despite concurrent access.
class LogrBlockContainer: public LogBlockContainer {
 public:

  // Creates a new block container in 'dir'.
  static Status Create(LogBlockManager* block_manager,
                       Dir* dir,
                       LogBlockContainerRefPtr* container);

  static Status Open(LogBlockManager* block_manager,
                     Dir* dir,
                     FsReport* report,
                     const string& id,
                     LogBlockContainerRefPtr* container);

  // The destructor will delete files of this container if it is dead.
  ~LogrBlockContainer();

  Status AppendMetadataForBatchDelete(const vector<BlockId>& block_ids) override;

  Status AppendMetadataForBatchCreate(const vector<LogWritableBlock*>& blocks) override;

  // Synchronize this container's metadata file with the disk. On success,
  // guarantees that the metadata is made durable.
  //
  // TODO(unknown): Add support to synchronize just a range.
  Status SyncMetadata() override {
    // TODO: rocksdb flush?
    return Status::OK();
  }

  // Reads the container's metadata from disk, sanity checking and processing
  // records along the way.
  //
  // Malformed records and other container inconsistencies are written to
  // 'report'. Healthy blocks are written either to 'live_blocks' or
  // 'dead_blocks'. Live records are written to 'live_block_records'. The
  // greatest block ID seen thus far in the container is written to 'max_block_id'.
  //
  // Returns an error only if there was a problem accessing the container from
  // disk; such errors are fatal and effectively halt processing immediately.
  Status ProcessRecords(
      FsReport* report,
      LogBlockManager::UntrackedBlockMap* live_blocks,
      LogBlockManager::BlockRecordMap* live_block_records,
      vector<LogBlockRefPtr>* dead_blocks,
      uint64_t* max_block_id,
      ProcessRecordType type) override;

  bool ShouldCompact() const override {
    return false;
  }

 private:
  LogrBlockContainer(LogBlockManager* block_manager, Dir* data_dir, const string& id,
                     shared_ptr<RWFile> data_file, uint64_t data_file_size);

  // TODO: update comments
  // Check the container whether it is fine.
  //
  // OK: both files of the container exist;
  // Aborted: the container will be repaired later;
  // NotFound: one file of the container has gone missing;
  // *-------------*------------*----------------*-------------------------*---------------------*
  // |DATA\METADATA| NONE EXIST | EXIST && < MIN | EXIST && NO LIVE BLOCKS | EXIST && LIVE BLOCKS|
  // *-------------*------------*----------------*-------------------------*---------------------*
  // | NONE EXIST  |            |    Aborted     |      Aborted            |      NotFound       |
  // *-------------*------------*----------------*-------------------------*---------------------*
  // | EXIST && 0  |  Aborted   |    Aborted     |      OK                 |      OK             |
  // *-------------*------------*----------------*-------------------------*---------------------*
  // | EXIST && >0 |  NotFound  |    OK          |      OK                 |      OK             |
  // *-------------*------------*----------------*-------------------------*---------------------*
  //
  // Note: the status here only represents the result of check.
  static Status CheckContainerFiles(LogBlockManager* block_manager,
                                    FsReport* report,
                                    const Dir* dir,
                                    const string& id,
                                    const string& common_path,
                                    const string& data_path,
                                    const string& metadata_path);

  DISALLOW_COPY_AND_ASSIGN(LogrBlockContainer);
};

#define CONTAINER_DISK_FAILURE(status_expr, msg) do { \
  Status s_ = (status_expr); \
  HANDLE_DISK_FAILURE(s_, block_manager_->error_manager_->RunErrorNotificationCb( \
      ErrorHandlerType::DISK_ERROR, data_dir_)); \
  WARN_NOT_OK(s_, msg); \
} while (0)

LogBlockContainer::LogBlockContainer(
    LogBlockManager* block_manager,
    Dir* data_dir,
    const string& id,
    shared_ptr<RWFile> data_file,
    uint64_t data_file_size)
    : block_manager_(block_manager),
      data_dir_(data_dir),
      id_(id),
      max_num_blocks_(FindOrDie(block_manager->block_limits_by_data_dir_,
                                data_dir)),
      preallocated_offset_(data_file_size),
      data_file_(std::move(data_file)),
      next_block_offset_(0),
      total_bytes_(0),
      total_blocks_(0),
      live_bytes_(0),
      live_bytes_aligned_(0),
      live_blocks_(0),
      blocks_being_written_(0),
      dead_(false),
      metrics_(block_manager->metrics()) {
  // If we have an encryption header, we need to align the next offset to the
  // next file system block.
  if (auto encryption_header_size = data_file_->GetEncryptionHeaderSize();
      encryption_header_size > 0) {
    UpdateNextBlockOffset(0, encryption_header_size);
    live_bytes_.Store(encryption_header_size);
    total_bytes_.Store(next_block_offset_.Load());
    live_bytes_aligned_.Store(next_block_offset_.Load());
  }
}

LogfBlockContainer::~LogfBlockContainer() {
  if (dead()) {
    CHECK(!block_manager_->opts_.read_only);
    string data_file_name = data_file_->filename();
    string metadata_file_name = metadata_file_->filename();
    string data_failure_msg =
        "Could not delete dead container data file " + data_file_name;
    string metadata_failure_msg =
        "Could not delete dead container metadata file " + metadata_file_name;
    if (PREDICT_TRUE(block_manager_->file_cache_)) {
      CONTAINER_DISK_FAILURE(block_manager_->file_cache_->DeleteFile(data_file_name),
                             data_failure_msg);
      CONTAINER_DISK_FAILURE(block_manager_->file_cache_->DeleteFile(metadata_file_name),
                             metadata_failure_msg);
    } else {
      CONTAINER_DISK_FAILURE(block_manager_->env_->DeleteFile(data_file_name),
                             data_failure_msg);
      CONTAINER_DISK_FAILURE(block_manager_->env_->DeleteFile(metadata_file_name),
                             metadata_failure_msg);
    }
  }
  data_file_.reset();
  metadata_file_.reset();
}

void LogBlockContainer::HandleError(const Status& s) const {
  HANDLE_DISK_FAILURE(s,
      block_manager()->error_manager()->RunErrorNotificationCb(ErrorHandlerType::DISK_ERROR,
                                                               data_dir_));
}

void LogfBlockContainer::CompactMetadata() {
  SCOPED_LOG_SLOW_EXECUTION(WARNING, 5, Substitute("CompactMetadata $0", ToString()));
  // Skip compacting if lock failed to reduce overhead, metadata is on compacting or will be
  // compacted next time.
  std::unique_lock<RWMutex> l(metadata_compact_lock_, std::try_to_lock);
  if (!l.owns_lock()) {
    return;
  }
  // Check again, in case the metadata was compacted while we were waiting.
  if (!ShouldCompactUnlocked()) {
    return;
  }

  FsReport report;
  report.full_container_space_check.emplace();
  report.incomplete_container_check.emplace();
  report.malformed_record_check.emplace();
  report.misaligned_block_check.emplace();
  report.partial_record_check.emplace();
  report.partial_rdb_record_check.emplace();

  LogBlockManager::UntrackedBlockMap live_blocks;
  LogBlockManager::BlockRecordMap live_block_records;
  vector<LogBlockRefPtr> dead_blocks;
  uint64_t max_block_id = 0;

  Status s;
  SCOPED_CLEANUP({
    if (!s.ok()) {
      // Make container read-only to forbid further writes in case of failure.
      // Because the on-disk state may contain partial/incomplete data/metadata at
      // this point, it is not safe to either overwrite it or append to it.
      SetReadOnly(s);
    }
  });
  s = ProcessRecords(
      &report, &live_blocks, &live_block_records, &dead_blocks, &max_block_id,
      ProcessRecordType::kReadOnly);  // Container statistic is updated, not need to update again.
  if (!s.ok()) {
    WARN_NOT_OK(s, Substitute("Could not process records in container $0", ToString()));
    return;
  }

  vector<BlockRecordPB> records = SortRecords(std::move(live_block_records));
  int64_t file_bytes_delta;
  s = block_manager_->RewriteMetadataFile(*this, records, &file_bytes_delta);
  if (!s.ok()) {
    WARN_NOT_OK(s, Substitute("Could not rewrite container metadata file $0", ToString()));
    return;
  }

  // However, we're hosed if we can't open the new metadata file.
  s = ReopenMetadataWriter();
  if (!s.ok()) {
    WARN_NOT_OK(s, Substitute("Could not reopen new metadata file $0", ToString()));
    return;
  }
  VLOG(1) << "Compacted metadata file " << ToString()
          << " (saved " << file_bytes_delta << " bytes)";

  total_blocks_.Store(live_blocks.size());
  live_blocks_.Store(live_blocks.size());
}

#define RETURN_NOT_OK_CONTAINER_DISK_FAILURE(status_expr) do { \
  RETURN_NOT_OK_HANDLE_DISK_FAILURE((status_expr), \
    block_manager->error_manager()->RunErrorNotificationCb(ErrorHandlerType::DISK_ERROR, dir)); \
} while (0)

Status LogfBlockContainer::Create(LogBlockManager* block_manager,
                                  Dir* dir,
                                  LogBlockContainerRefPtr* container) {
  string id;
  string common_path;
  string metadata_path;
  string data_path;
  Status metadata_status;
  Status data_status;
  shared_ptr<RWFile> metadata_writer;
  shared_ptr<RWFile> data_file;

  // Repeat in the event of a container id collision (unlikely).
  //
  // When looping, we delete any created-and-orphaned files.
  do {
    id = block_manager->oid_generator()->Next();
    common_path = JoinPathSegments(dir->dir(), id);
    metadata_path = StrCat(common_path, LogBlockManager::kContainerMetadataFileSuffix);
    data_path = StrCat(common_path, LogBlockManager::kContainerDataFileSuffix);
    if (PREDICT_TRUE(block_manager->file_cache_)) {
      if (metadata_writer) {
        WARN_NOT_OK(block_manager->file_cache_->DeleteFile(metadata_path),
                    "could not delete orphaned metadata file thru file cache");
      }
      if (data_file) {
        WARN_NOT_OK(block_manager->file_cache_->DeleteFile(data_path),
                    "could not delete orphaned data file thru file cache");
      }
      metadata_status = block_manager->file_cache_->OpenFile<Env::MUST_CREATE>(
          metadata_path, &metadata_writer);
      data_status = block_manager->file_cache_->OpenFile<Env::MUST_CREATE>(
          data_path, &data_file);
    } else {
      if (metadata_writer) {
        WARN_NOT_OK(block_manager->env()->DeleteFile(metadata_path),
                    "could not delete orphaned metadata file");
      }
      if (data_file) {
        WARN_NOT_OK(block_manager->env()->DeleteFile(data_path),
                    "could not delete orphaned data file");
      }
      unique_ptr<RWFile> rwf;
      RWFileOptions rw_opts;

      rw_opts.mode = Env::MUST_CREATE;
      rw_opts.is_sensitive = true;
      metadata_status = block_manager->env()->NewRWFile(
          rw_opts, metadata_path, &rwf);
      metadata_writer.reset(rwf.release());
      data_status = block_manager->env()->NewRWFile(
          rw_opts, data_path, &rwf);
      data_file.reset(rwf.release());
    }
  } while (PREDICT_FALSE(metadata_status.IsAlreadyPresent() ||
                         data_status.IsAlreadyPresent()));
  if (metadata_status.ok() && data_status.ok()) {
    unique_ptr<WritablePBContainerFile> metadata_file(new WritablePBContainerFile(
        std::move(metadata_writer)));
    RETURN_NOT_OK_CONTAINER_DISK_FAILURE(metadata_file->CreateNew(BlockRecordPB()));
    container->reset(new LogfBlockContainer(block_manager,
                                            dir,
                                            id,
                                            std::move(data_file),
                                            0 /*data_file_size*/,
                                            std::move(metadata_file)));
    VLOG(1) << "Created log block container " << (*container)->ToString();
  }

  // Prefer metadata status (arbitrarily).
  FsErrorManager* em = block_manager->error_manager();
  HANDLE_DISK_FAILURE(metadata_status, em->RunErrorNotificationCb(
                                           ErrorHandlerType::DISK_ERROR, dir));
  HANDLE_DISK_FAILURE(data_status, em->RunErrorNotificationCb(ErrorHandlerType::DISK_ERROR, dir));
  return !metadata_status.ok() ? metadata_status : data_status;
}

Status LogfBlockContainer::Open(LogBlockManager* block_manager,
                                Dir* dir,
                                FsReport* report,
                                const string& id,
                                LogBlockContainerRefPtr* container) {
  string common_path = JoinPathSegments(dir->dir(), id);
  string data_path = StrCat(common_path, LogBlockManager::kContainerDataFileSuffix);
  string metadata_path = StrCat(common_path, LogBlockManager::kContainerMetadataFileSuffix);
  RETURN_NOT_OK(LogfBlockContainer::CheckContainerFiles(block_manager, report, dir, id,
                                                        common_path, data_path, metadata_path));

  // Open the existing metadata and data files for writing.
  shared_ptr<RWFile> metadata_file;
  shared_ptr<RWFile> data_file;
  if (PREDICT_TRUE(block_manager->file_cache_)) {
    RETURN_NOT_OK_CONTAINER_DISK_FAILURE(
        block_manager->file_cache_->OpenFile<Env::MUST_EXIST>(metadata_path, &metadata_file));
    RETURN_NOT_OK_CONTAINER_DISK_FAILURE(
        block_manager->file_cache_->OpenFile<Env::MUST_EXIST>(data_path, &data_file));
  } else {
    RWFileOptions opts;
    opts.mode = Env::MUST_EXIST;
    opts.is_sensitive = true;
    unique_ptr<RWFile> rwf;
    RETURN_NOT_OK_CONTAINER_DISK_FAILURE(block_manager->env()->NewRWFile(opts,
                                                                         metadata_path, &rwf));
    metadata_file.reset(rwf.release());
    RETURN_NOT_OK_CONTAINER_DISK_FAILURE(block_manager->env()->NewRWFile(opts,
                                                                         data_path, &rwf));
    data_file.reset(rwf.release());
  }

  unique_ptr<WritablePBContainerFile> metadata_pb_writer;
  metadata_pb_writer.reset(new WritablePBContainerFile(std::move(metadata_file)));
  RETURN_NOT_OK_CONTAINER_DISK_FAILURE(metadata_pb_writer->OpenExisting());

  uint64_t data_file_size;
  RETURN_NOT_OK_CONTAINER_DISK_FAILURE(data_file->Size(&data_file_size));

  // Create the in-memory container and populate it.
  LogBlockContainerRefPtr open_container(new LogfBlockContainer(block_manager,
                                                                dir,
                                                                id,
                                                                std::move(data_file),
                                                                data_file_size,
                                                                std::move(metadata_pb_writer)));
  VLOG(1) << "Opened log block container " << open_container->ToString();
  container->swap(open_container);
  return Status::OK();
}

Status LogfBlockContainer::CheckContainerFiles(LogBlockManager* block_manager,
                                               FsReport* report,
                                               const Dir* dir,
                                               const string& id,
                                               const string& common_path,
                                               const string& data_path,
                                               const string& metadata_path) {
  Env* env = block_manager->env();
  uint64_t data_size = 0;
  Status s_data = env->GetFileSize(data_path, &data_size);
  if (!s_data.ok() && !s_data.IsNotFound()) {
    s_data = s_data.CloneAndPrepend("unable to determine data file size");
    RETURN_NOT_OK_CONTAINER_DISK_FAILURE(s_data);
  }
  uint64_t metadata_size = 0;
  Status s_meta = env->GetFileSize(metadata_path, &metadata_size);
  if (!s_meta.ok() && !s_meta.IsNotFound()) {
    s_meta = s_meta.CloneAndPrepend("unable to determine metadata file size");
    RETURN_NOT_OK_CONTAINER_DISK_FAILURE(s_meta);
  }

  const auto kEncryptionHeaderSize = env->GetEncryptionHeaderSize();
  const auto kMinimumValidLength = pb_util::kPBContainerMinimumValidLength + kEncryptionHeaderSize;

  // Check that both the metadata and data files exist and have valid lengths.
  // This covers a commonly seen case at startup, where the previous incarnation
  // of the server crashed due to "too many open files" just as it was trying
  // to create a file. This orphans an empty or invalid length file, which we can
  // safely delete. And another case is that the metadata and data files exist,
  // but the lengths are invalid.
  if (PREDICT_FALSE(metadata_size < kMinimumValidLength &&
                    data_size <= kEncryptionHeaderSize)) {
    report->incomplete_container_check->entries.emplace_back(common_path);
    return Status::Aborted(Substitute("orphaned empty or invalid length file $0", common_path));
  }

  // Handle a half-present container whose data file has gone missing and
  // the metadata file has no live blocks. If that's true, the (orphaned)
  // metadata file will be deleted when repairing.
  //
  // Open the metadata file and quickly check whether or not there is any live blocks.
  if (PREDICT_FALSE(metadata_size >= kMinimumValidLength &&
                    s_data.IsNotFound())) {
    Status read_status;
    BlockIdSet live_blocks;
    unique_ptr<RandomAccessFile> reader;
    RandomAccessFileOptions opts;
    opts.is_sensitive = true;
    RETURN_NOT_OK_CONTAINER_DISK_FAILURE(env->NewRandomAccessFile(opts, metadata_path, &reader));
    ReadablePBContainerFile pb_reader(std::move(reader));
    RETURN_NOT_OK_CONTAINER_DISK_FAILURE(pb_reader.Open());
    while (true) {
      BlockRecordPB record;
      read_status = pb_reader.ReadNextPB(&record);
      if (!read_status.ok()) break;
      switch (record.op_type()) {
        case CREATE:
          live_blocks.emplace(BlockId::FromPB(record.block_id()));
          break;
        case DELETE:
          live_blocks.erase(BlockId::FromPB(record.block_id()));
          break;
        default:
          LOG(WARNING) << Substitute("Found a record with unknown type $0", record.op_type());
          break;
      }
    }
    if (read_status.IsEndOfFile() && live_blocks.empty()) {
      report->incomplete_container_check->entries.emplace_back(common_path);
      return Status::Aborted(Substitute("orphaned metadata file with no live blocks $0",
                                        common_path));
    }
    // If the read failed for some unexpected reason, propagate the error.
    if (!read_status.IsEndOfFile() && !read_status.IsIncomplete()) {
      RETURN_NOT_OK_CONTAINER_DISK_FAILURE(read_status);
    }
  }

  // Except the special cases above, returns error status if any.
  if (s_data.IsNotFound()) RETURN_NOT_OK_CONTAINER_DISK_FAILURE(s_data);
  if (s_meta.IsNotFound()) RETURN_NOT_OK_CONTAINER_DISK_FAILURE(s_meta);

  return Status::OK();
}

Status LogBlockContainer::TruncateDataToNextBlockOffset() {
  RETURN_NOT_OK_HANDLE_ERROR(read_only_status());

  if (full()) {
    VLOG(2) << Substitute("Truncating container $0 to offset $1",
                          ToString(), next_block_offset());
    RETURN_NOT_OK_HANDLE_ERROR(data_file_->Truncate(next_block_offset()));
  }
  return Status::OK();
}

Status LogfBlockContainer::ProcessRecords(
    FsReport* report,
    LogBlockManager::UntrackedBlockMap* live_blocks,
    LogBlockManager::BlockRecordMap* live_block_records,
    vector<LogBlockRefPtr>* dead_blocks,
    uint64_t* max_block_id,
    ProcessRecordType type) {
  string metadata_path = metadata_file_->filename();
  unique_ptr<RandomAccessFile> metadata_reader;
  RandomAccessFileOptions opts;
  opts.is_sensitive = true;
  RETURN_NOT_OK_HANDLE_ERROR(block_manager()->env()->NewRandomAccessFile(
      opts, metadata_path, &metadata_reader));
  ReadablePBContainerFile pb_reader(std::move(metadata_reader));
  RETURN_NOT_OK_HANDLE_ERROR(pb_reader.Open());

  uint64_t data_file_size = 0;
  Status read_status;
  while (true) {
    BlockRecordPB record;
    read_status = pb_reader.ReadNextPB(&record);
    if (!read_status.ok()) {
      break;
    }
    RETURN_NOT_OK(ProcessRecord(&record, report,
                                live_blocks, live_block_records, dead_blocks,
                                &data_file_size, max_block_id, type));
  }

  // NOTE: 'read_status' will never be OK here.
  if (PREDICT_TRUE(read_status.IsEndOfFile())) {
    // We've reached the end of the file without any problems.
    return Status::OK();
  }
  if (read_status.IsIncomplete()) {
    // We found a partial trailing record in a version of the pb container file
    // format that can reliably detect this. Consider this a failed partial
    // write and truncate the metadata file to remove this partial record.
    report->partial_record_check->entries.emplace_back(ToString(),
                                                       pb_reader.offset());
    return Status::OK();
  }
  // If we've made it here, we've found (and are returning) an unrecoverable error.
  // Handle any errors we can, e.g. disk failures.
  HandleError(read_status);
  return read_status;
}

Status LogBlockContainer::ProcessRecord(
    BlockRecordPB* record,
    FsReport* report,
    LogBlockManager::UntrackedBlockMap* live_blocks,
    LogBlockManager::BlockRecordMap* live_block_records,
    vector<LogBlockRefPtr>* dead_blocks,
    uint64_t* data_file_size,
    uint64_t* max_block_id,
    ProcessRecordType type) {
  const BlockId block_id(BlockId::FromPB(record->block_id()));
  LogBlockRefPtr lb;
  switch (record->op_type()) {
    case CREATE:
      // First verify that the record's offset/length aren't wildly incorrect.
      if (PREDICT_FALSE(!record->has_offset() ||
                        !record->has_length() ||
                        record->offset() < 0  ||
                        record->length() < 0)) {
        report->malformed_record_check->entries.emplace_back(ToString(), record);
        break;
      }

      // Now it should be safe to access the record's offset/length.
      //
      // KUDU-1657: When opening a container in read-only mode which is actively
      // being written to by another lbm, we must reinspect the data file's size
      // frequently in order to account for the latest appends. Inspecting the
      // file size is expensive, so we only do it when the metadata indicates
      // that additional data has been written to the file.
      if (PREDICT_FALSE(record->offset() + record->length() > *data_file_size)) {
        RETURN_NOT_OK_HANDLE_ERROR(data_file_->Size(data_file_size));
      }

      // If the record still extends beyond the end of the file, it is
      // malformed, except if the length is 0, because in this case nothing has
      // actually been written.
      if (PREDICT_FALSE(record->offset() + record->length() > *data_file_size &&
                        record->length() > 0)) {
        // TODO(adar): treat as a different kind of inconsistency?
        report->malformed_record_check->entries.emplace_back(ToString(), record);
        break;
      }

      lb = new LogBlock(this, block_id, record->offset(), record->length());
      if (!InsertIfNotPresent(live_blocks, block_id, lb)) {
        // We found a record whose ID matches that of an already created block.
        //
        // TODO(adar): treat as a different kind of inconsistency?
        report->malformed_record_check->entries.emplace_back(
            ToString(), record);
        break;
      }

      VLOG(2) << Substitute("Found CREATE block $0 at offset $1 with length $2",
                            block_id.ToString(),
                            record->offset(), record->length());

      if (type == ProcessRecordType::kReadAndUpdate) {
        // This block must be included in the container's logical size, even if
        // it has since been deleted. This helps satisfy one of our invariants:
        // once a container byte range has been used, it may never be reused in
        // the future.
        //
        // If we ignored deleted blocks, we would end up reusing the space
        // belonging to the last deleted block in the container.
        UpdateNextBlockOffset(lb->offset(), lb->length());
        BlockCreated(lb);
      }

      (*live_block_records)[block_id].Swap(record);
      *max_block_id = std::max(*max_block_id, block_id.id());
      break;
    case DELETE:
      lb = EraseKeyReturnValuePtr(live_blocks, block_id);
      if (!lb) {
        // We found a record for which there is no already created block.
        //
        // TODO(adar): treat as a different kind of inconsistency?
        report->malformed_record_check->entries.emplace_back(ToString(), record);
        break;
      }
      VLOG(2) << Substitute("Found DELETE block $0", block_id.ToString());
      if (type == ProcessRecordType::kReadAndUpdate) {
        BlockDeleted(lb);
      }

      CHECK_EQ(1, live_block_records->erase(block_id));
      dead_blocks->emplace_back(std::move(lb));
      break;
    default:
      // We found a record with an unknown type.
      //
      // TODO(adar): treat as a different kind of inconsistency?
      report->malformed_record_check->entries.emplace_back(ToString(), record);
      break;
  }
  return Status::OK();
}

Status LogBlockContainer::DoCloseBlocks(const vector<LogWritableBlock*>& blocks,
                                        SyncMode mode) {
  auto sync_blocks = [&]() {
    if (mode == SYNC) {
      VLOG(3) << "Syncing data file " << data_file_->filename();
      RETURN_NOT_OK(SyncData());
    }

    // Append metadata only after data is synced so that there's
    // no chance of metadata landing on the disk before the data.
    RETURN_NOT_OK_PREPEND(AppendMetadataForBatchCreate(blocks),
                          "unable to append block's metadata during close");

    if (mode == SYNC) {
      RETURN_NOT_OK(SyncMetadata());
    }

    RETURN_NOT_OK(block_manager()->SyncContainer(*this));

    for (LogWritableBlock* block : blocks) {
      if (blocks.size() > 1) DCHECK_EQ(block->state(), WritableBlock::State::FINALIZED);
      block->DoClose();
    }
    return Status::OK();
  };

  Status s = sync_blocks();
  if (!s.ok()) {
    // Make container read-only to forbid further writes in case of failure.
    // Because the on-disk state may contain partial/incomplete data/metadata at
    // this point, it is not safe to either overwrite it or append to it.
    SetReadOnly(s);
  }
  return s;
}

Status LogBlockContainer::PunchHole(int64_t offset, int64_t length) {
  RETURN_NOT_OK_HANDLE_ERROR(read_only_status());
  DCHECK_GE(offset, 0);
  DCHECK_GE(length, 0);

  // It is invalid to punch a zero-size hole.
  if (length) {
    // It's OK if we exceed the file's total size; the kernel will truncate
    // our request.
    RETURN_NOT_OK_HANDLE_ERROR(data_file_->PunchHole(offset, length));
  }
  return Status::OK();
}

Status LogBlockContainer::WriteData(int64_t offset, const Slice& data) {
  return WriteVData(offset, ArrayView<const Slice>(&data, 1));
}

Status LogBlockContainer::WriteVData(int64_t offset, ArrayView<const Slice> data) {
  RETURN_NOT_OK_HANDLE_ERROR(read_only_status());
  DCHECK_GE(offset, next_block_offset());

  RETURN_NOT_OK_HANDLE_ERROR(data_file_->WriteV(offset, data));

  // This append may have changed the container size if:
  // 1. It was large enough that it blew out the preallocated space.
  // 2. Preallocation was disabled.
  size_t data_size = accumulate(data.begin(), data.end(), static_cast<size_t>(0),
                                [&](int sum, const Slice& curr) {
                                  return sum + curr.size();
                                });
  if (offset + data_size > preallocated_offset_) {
    RETURN_NOT_OK_HANDLE_ERROR(data_dir_->RefreshAvailableSpace(Dir::RefreshMode::ALWAYS));
  }
  return Status::OK();
}

Status LogBlockContainer::ReadData(int64_t offset, Slice result) const {
  DCHECK_GE(offset, 0);
  RETURN_NOT_OK_HANDLE_ERROR(data_file_->Read(offset, result));
  return Status::OK();
}

Status LogBlockContainer::ReadVData(int64_t offset, ArrayView<Slice> results) const {
  DCHECK_GE(offset, 0);
  RETURN_NOT_OK_HANDLE_ERROR(data_file_->ReadV(offset, results));
  return Status::OK();
}

Status LogBlockContainer::FlushData(int64_t offset, int64_t length) {
  RETURN_NOT_OK_HANDLE_ERROR(read_only_status());
  DCHECK_GE(offset, 0);
  DCHECK_GE(length, 0);
  RETURN_NOT_OK_HANDLE_ERROR(data_file_->Flush(RWFile::FLUSH_ASYNC, offset, length));
  return Status::OK();
}

Status LogBlockContainer::SyncData() {
  RETURN_NOT_OK_HANDLE_ERROR(read_only_status());
  if (FLAGS_enable_data_block_fsync) {
    if (metrics_) metrics_->generic_metrics.total_disk_sync->Increment();
    RETURN_NOT_OK_HANDLE_ERROR(data_file_->Sync());
  }
  return Status::OK();
}

LogfBlockContainer::LogfBlockContainer(
    LogBlockManager* block_manager,
    Dir* data_dir,
    const string& id,
    shared_ptr<RWFile> data_file,
    uint64_t data_file_size,
    unique_ptr<WritablePBContainerFile> metadata_file)
    : LogBlockContainer(block_manager, data_dir, id, std::move(data_file), data_file_size),
      metadata_compact_lock_(RWMutex::Priority::PREFER_READING),
      metadata_file_(std::move(metadata_file)) {
}

Status LogfBlockContainer::AppendMetadataForBatchDelete(const vector<BlockId>& block_ids) {
  RETURN_NOT_OK_HANDLE_ERROR(read_only_status());
  BlockRecordPB record;
  record.set_op_type(DELETE);
  record.set_timestamp_us(GetCurrentTimeMicros());
  for (const auto& block_id : block_ids) {
    block_id.CopyToPB(record.mutable_block_id());
    shared_lock<RWMutex> l(metadata_compact_lock_);
    RETURN_NOT_OK_HANDLE_ERROR(metadata_file_->Append(record));
  }
  return Status::OK();
}

Status LogfBlockContainer::AppendMetadataForBatchCreate(const vector<LogWritableBlock*>& blocks) {
  RETURN_NOT_OK_HANDLE_ERROR(read_only_status());
  // Note: We don't check for sufficient disk space for metadata writes in
  // order to allow for block deletion on full disks.
  BlockRecordPB record;
  record.set_op_type(CREATE);
  record.set_timestamp_us(GetCurrentTimeMicros());
  for (const auto& block : blocks) {
    block->id().CopyToPB(record.mutable_block_id());
    record.set_offset(block->block_offset());
    record.set_length(block->block_length());
    shared_lock<RWMutex> l(metadata_compact_lock_);
    RETURN_NOT_OK_HANDLE_ERROR(metadata_file_->Append(record));
  }
  return Status::OK();
}

Status LogfBlockContainer::SyncMetadata() {
  VLOG(3) << "Syncing metadata file " << metadata_file_->filename();
  RETURN_NOT_OK_HANDLE_ERROR(read_only_status());
  if (FLAGS_enable_data_block_fsync) {
    if (metrics_) metrics_->generic_metrics.total_disk_sync->Increment();
    shared_lock<RWMutex> l(metadata_compact_lock_);
    RETURN_NOT_OK_HANDLE_ERROR(metadata_file_->Sync());
  }
  return Status::OK();
}

Status LogfBlockContainer::ReopenMetadataWriter() {
  shared_ptr<RWFile> f;
  if (PREDICT_TRUE(block_manager_->file_cache_)) {
    RETURN_NOT_OK_HANDLE_ERROR(block_manager_->file_cache_->OpenFile<Env::MUST_EXIST>(
        metadata_file_->filename(), &f));
  } else {
    unique_ptr<RWFile> f_uniq;
    RWFileOptions opts;
    opts.mode = Env::MUST_EXIST;
    opts.is_sensitive = true;
    RETURN_NOT_OK_HANDLE_ERROR(
        block_manager_->env_->NewRWFile(opts, metadata_file_->filename(), &f_uniq));
    f.reset(f_uniq.release());
  }
  unique_ptr<WritablePBContainerFile> w;
  w.reset(new WritablePBContainerFile(std::move(f)));
  RETURN_NOT_OK_HANDLE_ERROR(w->OpenExisting());

  RETURN_NOT_OK_HANDLE_ERROR(metadata_file_->Close());
  metadata_file_.swap(w);
  return Status::OK();
}

Status LogBlockContainer::EnsurePreallocated(int64_t block_start_offset,
                                             size_t next_append_length) {
  RETURN_NOT_OK_HANDLE_ERROR(read_only_status());
  DCHECK_GE(block_start_offset, 0);

  if (!FLAGS_log_container_preallocate_bytes) {
    return Status::OK();
  }

  // If the last write blew out the preallocation window, or if the next write
  // exceeds it, we need to preallocate another chunk.
  if (block_start_offset > preallocated_offset_ ||
      next_append_length > preallocated_offset_ - block_start_offset) {
    int64_t off = std::max(preallocated_offset_, block_start_offset);
    int64_t len = FLAGS_log_container_preallocate_bytes;
    // If encryption is enabled, preallocation happens after the encryption
    // header is written. This keeps the file size a multiple of
    // log_container_preallocate_bytes even in this case.
    if (block_start_offset < FLAGS_log_container_preallocate_bytes) {
      len -= block_start_offset;
    }
    RETURN_NOT_OK_HANDLE_ERROR(data_file_->PreAllocate(off, len, RWFile::CHANGE_FILE_SIZE));
    RETURN_NOT_OK_HANDLE_ERROR(data_dir_->RefreshAvailableSpace(Dir::RefreshMode::ALWAYS));
    VLOG(2) << Substitute("Preallocated $0 bytes at offset $1 in container $2",
                          len, off, ToString());

    preallocated_offset_ = off + len;
  }

  return Status::OK();
}

void LogBlockContainer::FinalizeBlock(int64_t block_offset, int64_t block_length) {
  // Updates this container's next block offset before marking it as available
  // to ensure thread safety while updating internal bookkeeping state.
  UpdateNextBlockOffset(block_offset, block_length);

  // Truncate the container if it's now full; any left over preallocated space
  // is no longer needed.
  //
  // Note that depending on when FinalizeBlock() is called, this can take place
  // _after_ the container has been synced to disk. That's OK; truncation isn't
  // needed for correctness, and in the event of a crash or error, it will be
  // retried at startup.
  WARN_NOT_OK(TruncateDataToNextBlockOffset(),
              "could not truncate excess preallocated space");
  if (full() && block_manager_->metrics()) {
    block_manager_->metrics()->full_containers->Increment();
  }
  block_manager_->MakeContainerAvailable(this);
}

void LogBlockContainer::UpdateNextBlockOffset(int64_t block_offset, int64_t block_length) {
  DCHECK_GE(block_offset, 0);

  // The log block manager maintains block contiguity as an invariant, which
  // means accounting for the new block should be as simple as adding its
  // aligned length to 'next_block_offset_'. However, due to KUDU-1793, some
  // containers may have developed gaps between blocks. We'll account for them
  // by considering both the block's offset and its length.
  //
  // The number of bytes is rounded up to the nearest filesystem block so
  // that each Kudu block is guaranteed to be on a filesystem block
  // boundary. This guarantees that the disk space can be reclaimed when
  // the block is deleted.
  int64_t new_next_block_offset = KUDU_ALIGN_UP(
      block_offset + block_length,
      instance()->filesystem_block_size_bytes());
  next_block_offset_.StoreMax(new_next_block_offset);

  if (full()) {
    VLOG(1) << Substitute(
        "Container $0 with size $1 is now full, max size is $2",
        ToString(), next_block_offset(), FLAGS_log_container_max_size);
  }
}

vector<BlockRecordPB> LogBlockContainer::SortRecords(
    LogBlockManager::BlockRecordMap live_block_records) {
  // TODO(adar): this should be reported as an inconsistency once
  // container metadata compaction is also done in realtime. Until then,
  // it would be confusing to report it as such since it'll be a natural
  // event at startup.
  vector<BlockRecordPB> records(live_block_records.size());
  int i = 0;
  for (auto& e : live_block_records) {
    records[i].Swap(&e.second);
    i++;
  }

  // Sort the records such that their ordering reflects the ordering in
  // the pre-compacted metadata file.
  //
  // This is preferred to storing the records in an order-preserving
  // container (such as std::map) because while records are temporarily
  // retained for every container, only some containers will actually
  // undergo metadata compaction.
  std::sort(records.begin(), records.end(), [](const BlockRecordPB& a, const BlockRecordPB& b) {
    // Sort by timestamp.
    if (a.timestamp_us() != b.timestamp_us()) {
      return a.timestamp_us() < b.timestamp_us();
    }

    // If the timestamps match, sort by offset.
    //
    // If the offsets also match (i.e. both blocks are of zero length),
    // it doesn't matter which of the two records comes first.
    return a.offset() < b.offset();
  });

  return records;
}

void LogBlockContainer::BlockCreated(const LogBlockRefPtr& block) {
  DCHECK_GE(block->offset(), 0);

  total_bytes_.IncrementBy(block->fs_aligned_length());
  total_blocks_.Increment();
  live_bytes_.IncrementBy(block->length());
  live_bytes_aligned_.IncrementBy(block->fs_aligned_length());
  live_blocks_.Increment();
}

void LogBlockContainer::BlockDeleted(const LogBlockRefPtr& block) {
  DCHECK_GE(block->offset(), 0);

  live_bytes_.IncrementBy(-block->length());
  live_bytes_aligned_.IncrementBy(-block->fs_aligned_length());
  live_blocks_.IncrementBy(-1);
}

void LogBlockContainer::ExecClosure(const std::function<void()>& task) {
  data_dir_->ExecClosure(task);
}

string LogBlockContainer::ToString() const {
  string s;
  CHECK(TryStripSuffixString(data_file_->filename(),
                             LogBlockManager::kContainerDataFileSuffix, &s));
  return s;
}

void LogBlockContainer::SetReadOnly(const Status& error) {
  DCHECK(!error.ok());
  LOG(WARNING) << Substitute("Container $0 being marked read-only: $1",
                             ToString(), error.ToString());
  std::lock_guard<simple_spinlock> l(read_only_lock_);
  read_only_status_ = error;
}

void LogBlockContainer::ContainerDeletionAsync(int64_t offset, int64_t length) {
  if (dead()) {
    // Don't bother punching holes; the container's destructor will delete the
    // container's files outright.
    return;
  }

  VLOG(3) << "Freeing space belonging to container " << ToString();
  Status s = PunchHole(offset, length);
  if (s.ok() && metrics_) metrics_->holes_punched->Increment();
  WARN_NOT_OK(s, Substitute("could not delete blocks in container $0",
                            data_dir()->dir()));
}

Status LogrBlockContainer::Create(LogBlockManager* block_manager,
                                  Dir* dir,
                                  LogBlockContainerRefPtr* container) {
  string id;
  string common_path;
  string data_path;
  Status data_status;
  shared_ptr<RWFile> data_file;

  // Repeat in the event of a container id collision (unlikely).
  //
  // When looping, we delete any created-and-orphaned files.
  do {
    id = block_manager->oid_generator()->Next();
    common_path = JoinPathSegments(dir->dir(), id);
    data_path = StrCat(common_path, LogBlockManager::kContainerDataFileSuffix);
    rocksdb::WriteOptions del_opt;
    rocksdb::Slice begin_key = id;
    string next_id = ObjectIdGenerator::NextOf(id);
    rocksdb::Slice end_key = next_id;
    auto s = dir->rdb()->DeleteRange(del_opt,
                                     dir->rdb()->DefaultColumnFamily(),
                                     begin_key,
                                     end_key);
    CHECK_OK(FromRdbStatus(s));
    if (PREDICT_TRUE(block_manager->file_cache_)) {
      if (data_file) {
        WARN_NOT_OK(block_manager->file_cache_->DeleteFile(data_path),
                    "could not delete orphaned data file thru file cache");
      }
      data_status = block_manager->file_cache_->OpenFile<Env::MUST_CREATE>(
          data_path, &data_file);
    } else {
      if (data_file) {
        WARN_NOT_OK(block_manager->env()->DeleteFile(data_path),
                    "could not delete orphaned data file");
      }
      unique_ptr<RWFile> rwf;
      RWFileOptions rw_opts;

      rw_opts.mode = Env::MUST_CREATE;
      rw_opts.is_sensitive = true;
      data_status = block_manager->env()->NewRWFile(
          rw_opts, data_path, &rwf);
      data_file.reset(rwf.release());
    }
  } while (PREDICT_FALSE(data_status.IsAlreadyPresent()));
  if (data_status.ok()) {
    container->reset(new LogrBlockContainer(block_manager,
                                            dir,
                                            id,
                                            std::move(data_file),
                                            0 /* data_file_size */));
    VLOG(1) << "Created log block container " << (*container)->ToString();
  }

  // Prefer metadata status (arbitrarily).
  FsErrorManager* em = block_manager->error_manager();
  HANDLE_DISK_FAILURE(data_status, em->RunErrorNotificationCb(ErrorHandlerType::DISK_ERROR, dir));
  return data_status;
}

Status LogrBlockContainer::Open(LogBlockManager* block_manager,
                               Dir* dir,
                               FsReport* report,
                               const string& id,
                               LogBlockContainerRefPtr* container) {
  string common_path = JoinPathSegments(dir->dir(), id);
  string data_path = StrCat(common_path, LogBlockManager::kContainerDataFileSuffix);
  RETURN_NOT_OK(LogrBlockContainer::CheckContainerFiles(block_manager, report, dir, id,
                                                        common_path, data_path, ""));

  // Open the existing metadata and data files for writing.
  shared_ptr<RWFile> data_file;
  if (PREDICT_TRUE(block_manager->file_cache_)) {
    RETURN_NOT_OK_CONTAINER_DISK_FAILURE(
        block_manager->file_cache_->OpenFile<Env::MUST_EXIST>(data_path, &data_file));
  } else {
    RWFileOptions opts;
    opts.mode = Env::MUST_EXIST;
    opts.is_sensitive = true;
    unique_ptr<RWFile> rwf;
    RETURN_NOT_OK_CONTAINER_DISK_FAILURE(block_manager->env()->NewRWFile(opts,
                                                                         data_path, &rwf));
    data_file.reset(rwf.release());
  }
  uint64_t data_file_size;
  RETURN_NOT_OK_CONTAINER_DISK_FAILURE(data_file->Size(&data_file_size));

  // Create the in-memory container and populate it.
  LogBlockContainerRefPtr open_container(new LogrBlockContainer(block_manager,
                                                                dir,
                                                                id,
                                                                std::move(data_file),
                                                                data_file_size));
  VLOG(1) << "Opened log block container " << open_container->ToString();
  container->swap(open_container);
  return Status::OK();
}

LogrBlockContainer::LogrBlockContainer(
    LogBlockManager* block_manager,
    Dir* data_dir,
    const string& id,
    shared_ptr<RWFile> data_file,
    uint64_t data_file_size)
    : LogBlockContainer(block_manager, data_dir, id, std::move(data_file), data_file_size) {
}

LogrBlockContainer::~LogrBlockContainer() {
  if (dead()) {
    CHECK(!block_manager_->opts_.read_only);
    string data_file_name = data_file_->filename();
    string data_failure_msg =
        "Could not delete dead container data file " + data_file_name;

    rocksdb::WriteOptions del_opt;
    rocksdb::Slice begin_key = id_;
    string next_id = ObjectIdGenerator::NextOf(id_);
    rocksdb::Slice end_key = next_id;
    auto s = data_dir_->rdb()->DeleteRange(del_opt,
                                           data_dir_->rdb()->DefaultColumnFamily(),
                                           begin_key,
                                           end_key);
    CHECK_OK(FromRdbStatus(s));
    if (PREDICT_TRUE(block_manager_->file_cache_)) {
      CONTAINER_DISK_FAILURE(block_manager_->file_cache_->DeleteFile(data_file_name),
                             data_failure_msg);
    } else {
      CONTAINER_DISK_FAILURE(block_manager_->env_->DeleteFile(data_file_name),
                             data_failure_msg);
    }
  }
  data_file_.reset();
}

Status LogrBlockContainer::CheckContainerFiles(LogBlockManager* block_manager,
                                              FsReport* report,
                                              const Dir* dir,
                                              const string& id,
                                              const string& common_path,
                                              const string& data_path,
                                              const string& metadata_path) {
  Env* env = block_manager->env();
  uint64_t data_size = 0;
  Status s_data = env->GetFileSize(data_path, &data_size);
  if (!s_data.ok() && !s_data.IsNotFound()) {
    s_data = s_data.CloneAndPrepend("unable to determine data file size");
    RETURN_NOT_OK_CONTAINER_DISK_FAILURE(s_data);
  }

  const auto kEncryptionHeaderSize = env->GetEncryptionHeaderSize();

  // Check data files exist and have valid lengths.
  if (PREDICT_FALSE(data_size <= kEncryptionHeaderSize)) {
    report->incomplete_container_check->entries.emplace_back(common_path);
    return Status::Aborted(Substitute("orphaned empty or invalid length file $0", common_path));
  }

  return Status::OK();
}

Status LogrBlockContainer::ProcessRecords(
    FsReport* report,
    LogBlockManager::UntrackedBlockMap* live_blocks,
    LogBlockManager::BlockRecordMap* live_block_records,
    vector<LogBlockRefPtr>* dead_blocks,
    uint64_t* max_block_id,
    ProcessRecordType type) {
  int count = 0;
  SCOPED_LOG_TIMING(INFO, Substitute("LogrBlockContainer::ProcessRecords $0", count));
  rocksdb::Slice begin_key = id_;
  string next_id = ObjectIdGenerator::NextOf(id_);
  rocksdb::Slice end_key = next_id;
  rocksdb::ReadOptions options;
  options.total_order_seek = false;
  options.auto_prefix_mode = false;
  options.readahead_size = 2 << 20;
  options.iterate_upper_bound = &end_key;
  options.prefix_same_as_start = true;

  uint64_t data_file_size = 0;
  BlockRecordPB record;
  unique_ptr<rocksdb::Iterator> it(
      data_dir_->rdb()->NewIterator(options, data_dir_->rdb()->DefaultColumnFamily()));
  it->Seek(begin_key);
  while (it->Valid() && it->key().starts_with(begin_key)) {
    if (PREDICT_FALSE(!record.ParseFromArray(it->value().data(), it->value().size()))) {
      report->partial_rdb_record_check->entries.emplace_back(ToString(), it->key().ToString());
      LOG(WARNING) << Substitute("Parse metadata in rocksdb failed, path: $0, key: $1",
                                 ToString(),
                                 it->key().ToString());
      it->Next();
      continue;
    }

    RETURN_NOT_OK(ProcessRecord(&record, report,
                                live_blocks, live_block_records, dead_blocks,
                                &data_file_size, max_block_id, type));
    it->Next();
    count++;
  }
  Status s = FromRdbStatus(it->status());
  if (PREDICT_FALSE(!s.ok())) {
    LOG(WARNING) << "Read metadata from rocksdb failed, instance: " << s.ToString();
    HandleError(s);
    return s;
  }

  return Status::OK();
}

Status LogrBlockContainer::AppendMetadataForBatchDelete(const vector<BlockId>& block_ids) {
  rocksdb::WriteOptions options;
  rocksdb::WriteBatch batch;
  string tmp_key;
  for (const auto& block_id : block_ids) {
    tmp_key = id_ + "." + block_id.ToString();
    rocksdb::Slice key(tmp_key);
    CHECK_OK(FromRdbStatus(batch.Delete(key)));

    // TODO(yingchun): increase FLAGS_log_container_delete_batch_count seems can get better perform.
    if (batch.Count() == FLAGS_log_container_delete_batch_count) {
      rocksdb::Status s = data_dir_->rdb()->Write(options, &batch);
      CHECK_OK(FromRdbStatus(s));
      batch.Clear();
    }
  }

  if (batch.Count() > 0) {
    rocksdb::Status s = data_dir_->rdb()->Write(options, &batch);
    CHECK_OK(FromRdbStatus(s));
  }

  return Status::OK();
}

Status LogrBlockContainer::AppendMetadataForBatchCreate(const vector<LogWritableBlock*>& blocks) {
  BlockRecordPB record;
  record.set_op_type(CREATE);
  record.set_timestamp_us(GetCurrentTimeMicros());

  rocksdb::WriteBatch batch;
  string tmp_key;
  for (const auto& block : blocks) {
    block->id().CopyToPB(record.mutable_block_id());
    record.set_offset(block->block_offset());
    record.set_length(block->block_length());

    tmp_key = id_ + "." + block->id().ToString();
    rocksdb::Slice key(tmp_key);
    string buf;
    record.SerializeToString(&buf);
    rocksdb::Slice value(buf);
    CHECK_OK(FromRdbStatus(batch.Put(key, value)));
  }

  rocksdb::WriteOptions options;
  rocksdb::Status s = data_dir_->rdb()->Write(options, &batch);
  CHECK_OK(FromRdbStatus(s));

  return Status::OK();
}

///////////////////////////////////////////////////////////
// LogBlockCreationTransaction
////////////////////////////////////////////////////////////

class LogBlockCreationTransaction : public BlockCreationTransaction {
 public:
  LogBlockCreationTransaction() = default;

  ~LogBlockCreationTransaction() = default;

  void AddCreatedBlock(unique_ptr<WritableBlock> block) override;

  Status CommitCreatedBlocks() override;

 private:
  vector<unique_ptr<LogWritableBlock>> created_blocks_;
};

void LogBlockCreationTransaction::AddCreatedBlock(
    unique_ptr<WritableBlock> block) {
  LogWritableBlock* lwb = down_cast<LogWritableBlock*>(block.release());
  created_blocks_.emplace_back(unique_ptr<LogWritableBlock>(lwb));
}

Status LogBlockCreationTransaction::CommitCreatedBlocks() {
  if (created_blocks_.empty()) {
    return Status::OK();
  }

  VLOG(3) << "Closing " << created_blocks_.size() << " blocks";
  unordered_map<LogBlockContainer*, vector<LogWritableBlock*>> created_block_map;
  for (const auto& block : created_blocks_) {
    if (FLAGS_block_manager_preflush_control == "close") {
      // Ask the kernel to begin writing out each block's dirty data. This is
      // done up-front to give the kernel opportunities to coalesce contiguous
      // dirty pages.
      RETURN_NOT_OK(block->FlushDataAsync());
    }
    created_block_map[block->container()].emplace_back(block.get());
  }

  // Close all blocks and sync the blocks belonging to the same
  // container together to reduce fsync() usage, waiting for them
  // to become durable.
  for (const auto& entry : created_block_map) {
    RETURN_NOT_OK(entry.first->DoCloseBlocks(entry.second,
                                             LogBlockContainer::SyncMode::SYNC));
  }
  created_blocks_.clear();
  return Status::OK();
}

///////////////////////////////////////////////////////////
// LogBlockDeletionTransaction
////////////////////////////////////////////////////////////

class LogBlockDeletionTransaction : public BlockDeletionTransaction,
    public std::enable_shared_from_this<LogBlockDeletionTransaction> {
 public:
  explicit LogBlockDeletionTransaction(LogBlockManager* lbm)
      : lbm_(lbm) {
  }

  // Due to the shared ownership of LogBlockDeletionTransaction,
  // the destructor is responsible for destroying all registered
  // blocks. This includes:
  // 1. Punching holes in deleted blocks, and
  // 2. Deleting dead containers outright.
  ~LogBlockDeletionTransaction();

  void AddDeletedBlock(BlockId block) override;

  Status CommitDeletedBlocks(vector<BlockId>* deleted) override;

  // Add the given block that needs to be deleted to 'deleted_interval_map_',
  // which keeps track of container and the range to be hole punched.
  void AddBlock(const LogBlockRefPtr& lb);

 private:
  // Block <offset, offset + length> pair.
  typedef std::pair<int64_t, int64_t> BlockInterval;

  // Map used to aggregate BlockInterval instances across containers.
  unordered_map<LogBlockContainerRefPtr,
      vector<BlockInterval>,
      ScopedRefPtrHashFunctor<LogBlockContainer>,
      ScopedRefPtrEqualToFunctor<LogBlockContainer>> deleted_interval_map_;

  // The owning LogBlockManager. Must outlive the LogBlockDeletionTransaction.
  LogBlockManager* lbm_;
  vector<BlockId> deleted_blocks_;
  DISALLOW_COPY_AND_ASSIGN(LogBlockDeletionTransaction);
};

void LogBlockDeletionTransaction::AddDeletedBlock(BlockId block) {
  deleted_blocks_.emplace_back(block);
}

LogBlockDeletionTransaction::~LogBlockDeletionTransaction() {
  for (auto& entry : deleted_interval_map_) {
    LogBlockContainer* container = entry.first.get();

    // For the full and dead containers, it is much cheaper
    // to delete the container files outright, rather than
    // punching holes.
    if (container->check_death_condition()) {
      // Mark the container as deleted and remove it from the global map.
      //
      // It's possible for multiple deletion transactions to end up here. For
      // example, if one transaction deletes the second to last block in a
      // container and the second deletes its last block, the destructors of
      // both transactions could run at a time that container's internal
      // bookkeeping reflects the deadness of the container.
      //
      // The function 'TrySetDead()' can only be called successfully once,
      // because there is a compare-and-swap operation inside.
      if (container->TrySetDead()) {
        lbm_->RemoveDeadContainer(container->ToString());
      }
      continue;
    }

    CHECK_OK_PREPEND(CoalesceIntervals<int64_t>(&entry.second),
                     Substitute("could not coalesce hole punching for container: $0",
                                container->ToString()));

    scoped_refptr<LogBlockContainer> self(container);
    for (const auto& interval : entry.second) {
      container->ExecClosure([self, interval]() {
        self->ContainerDeletionAsync(interval.first, interval.second - interval.first);
      });
    }
  }
}

Status LogBlockDeletionTransaction::CommitDeletedBlocks(vector<BlockId>* deleted) {
  if (deleted) {
    deleted->clear();
  }
  shared_ptr<LogBlockDeletionTransaction> transaction = shared_from_this();

  vector<LogBlockRefPtr> log_blocks;
  Status first_failure = lbm_->RemoveLogBlocks(deleted_blocks_, &log_blocks, deleted);
  for (const auto& lb : log_blocks) {
    // Register the block to be hole punched if metadata recording
    // is successful.
    lb->RegisterDeletion(transaction);
    AddBlock(lb);

    if (lbm_->metrics_) {
      lbm_->metrics_->generic_metrics.total_blocks_deleted->Increment();
    }
  }

  if (!first_failure.ok()) {
    first_failure = first_failure.CloneAndPrepend(
        Substitute("only deleted $0 blocks, first failure",
                   deleted ? std::to_string(deleted->size()) : "partial"));
  }
  deleted_blocks_.clear();
  return first_failure;
}

void LogBlockDeletionTransaction::AddBlock(const LogBlockRefPtr& lb) {
  DCHECK_GE(lb->fs_aligned_length(), 0);

  BlockInterval block_interval(lb->offset(),
                               lb->offset() + lb->fs_aligned_length());
  deleted_interval_map_[lb->container()].emplace_back(block_interval);
}

////////////////////////////////////////////////////////////
// LogBlockContainerLoadResult
////////////////////////////////////////////////////////////

struct LogBlockContainerLoadResult {
  Status status;
  FsReport report;

  // Keep track of containers that have nothing but dead blocks; they will be
  // deleted during repair.
  vector<LogBlockContainerRefPtr> dead_containers;
  // Keep track of containers whose live block ratio is low; their metadata
  // files will be compacted during repair.
  unordered_map<string, vector<BlockRecordPB>> low_live_block_containers;
  // Keep track of deleted blocks whose space hasn't been punched; they will
  // be repunched during repair.
  vector<LogBlockRefPtr> need_repunching_blocks;

  LogBlockContainerLoadResult() {
    // We are going to perform these checks.
    //
    // Note: this isn't necessarily the complete set of FsReport checks; there
    // may be checks that the LBM cannot perform.
    report.full_container_space_check.emplace();
    report.incomplete_container_check.emplace();
    report.malformed_record_check.emplace();
    report.misaligned_block_check.emplace();
    report.partial_record_check.emplace();
    report.partial_rdb_record_check.emplace();
  }
};

////////////////////////////////////////////////////////////
// LogBlock (definition)
////////////////////////////////////////////////////////////

LogBlock::LogBlock(LogBlockContainerRefPtr container, BlockId block_id,
                   int64_t offset, int64_t length)
    : container_(std::move(container)),
      block_id_(block_id),
      offset_(offset),
      length_(length) {
  DCHECK_GE(offset, 0);
  DCHECK_GE(length, 0);
}

int64_t LogBlock::fs_aligned_length() const {
  uint64_t fs_block_size = container_->instance()->filesystem_block_size_bytes();

  // Nearly all blocks are placed on a filesystem block boundary, which means
  // their length post-alignment is simply their length aligned up to the
  // nearest fs block size.
  //
  // However, due to KUDU-1793, some blocks may start or end at misaligned
  // offsets. We don't maintain enough state to precisely pinpoint such a
  // block's (aligned) end offset in this case, so we'll just undercount it.
  // This should be safe, although it may mean unreclaimed disk space (i.e.
  // when fs_aligned_length() is used in hole punching).
  if (PREDICT_TRUE(offset_ % fs_block_size == 0)) {
    return KUDU_ALIGN_UP(length_, fs_block_size);
  }
  return length_;
}

void LogBlock::RegisterDeletion(
    const shared_ptr<LogBlockDeletionTransaction>& transaction) {
  DCHECK(!transaction_);
  DCHECK(transaction);

  transaction_ = transaction;
}

////////////////////////////////////////////////////////////
// LogWritableBlock (definition)
////////////////////////////////////////////////////////////

LogWritableBlock::LogWritableBlock(LogBlockContainerRefPtr container,
                                   BlockId block_id, int64_t block_offset)
    : container_(std::move(container)),
      block_id_(block_id),
      block_offset_(block_offset),
      block_length_(0),
      state_(CLEAN) {
  DCHECK_GE(block_offset, 0);
  DCHECK_EQ(0, block_offset % container_->instance()->filesystem_block_size_bytes());
  container_->blocks_being_written_incr(1);
  if (container_->metrics()) {
    container_->metrics()->generic_metrics.blocks_open_writing->Increment();
    container_->metrics()->generic_metrics.total_writable_blocks->Increment();
  }
}

LogWritableBlock::~LogWritableBlock() {
  // Put the decrement 'blocks_being_written_' at the beginning of this
  // function can help to avoid unnecessary hole punch.
  container_->blocks_being_written_incr(-1);
  if (state_ != CLOSED) {
    WARN_NOT_OK(Abort(), Substitute("Failed to abort block $0",
                                    id().ToString()));
  }
}

Status LogWritableBlock::Close() {
  return container_->DoCloseBlocks({ this }, LogBlockContainer::SyncMode::SYNC);
}

Status LogWritableBlock::Abort() {
  // Only updates metrics and block state for read-only container.
  if (container_->read_only()) {
    if (state_ != CLOSED) {
      state_ = CLOSED;
      if (container_->metrics()) {
        container_->metrics()->generic_metrics.blocks_open_writing->Decrement();
        container_->metrics()->generic_metrics.total_bytes_written->IncrementBy(
            block_length_);
      }
    }
    return container_->read_only_status().CloneAndPrepend(
        Substitute("container $0 is read-only", container_->ToString()));
  }

  // Close the block and then delete it. Theoretically, we could do nothing
  // for Abort() other than updating metrics and block state. Here is the
  // reasoning why it would be safe to do so. Currently, failures in block
  // creation can happen at various stages:
  // 1) before the block is finalized (either in CLEAN or DIRTY state). It is
  //    safe to do nothing, as the block is not yet finalized and next block
  //    will overwrite the dirty data.
  // 2) after the block is finalized but before the metadata is successfully
  //    appended. That means the container's internal bookkeeping has been
  //    updated and the data could be durable. Doing nothing can result in
  //    data-consuming gaps in containers, but these gaps can be cleaned up
  //    by hole repunching at start up.
  // 3) after metadata is appended. If we do nothing, this can result in
  //    orphaned blocks if the metadata is durable. But orphaned blocks can be
  //    garbage collected later.
  //
  // TODO(Hao): implement a fine-grained abort handling.
  // However it is better to provide a fine-grained abort handling to
  // avoid large chunks of data-consuming gaps and orphaned blocks. A
  // possible way to do so is,
  // 1) for per block abort, if the block state is FINALIZED, do hole
  //    punching.
  // 2) for BlockTransaction abort, DoCloseBlock() should append metadata
  //    for deletion and coalescing hole punching for blocks in the same
  //    container.

  RETURN_NOT_OK(container_->DoCloseBlocks({ this }, LogBlockContainer::SyncMode::NO_SYNC));

  // DoCloseBlocks() has unlocked the container; it may be locked by someone else.
  // But block_manager_ is immutable, so this is safe.
  shared_ptr<BlockDeletionTransaction> deletion_transaction =
      container_->block_manager()->NewDeletionTransaction();
  deletion_transaction->AddDeletedBlock(id());
  return deletion_transaction->CommitDeletedBlocks(nullptr);
}

const BlockId& LogWritableBlock::id() const {
  return block_id_;
}

BlockManager* LogWritableBlock::block_manager() const {
  return container_->block_manager();
}

Status LogWritableBlock::Append(const Slice& data) {
  return AppendV(ArrayView<const Slice>(&data, 1));
}

Status LogWritableBlock::AppendV(ArrayView<const Slice> data) {
  DCHECK(state_ == CLEAN || state_ == DIRTY)
      << "Invalid state: " << state_;

  // Calculate the amount of data to write
  size_t data_size = accumulate(data.begin(), data.end(), static_cast<size_t>(0),
                                [&](int sum, const Slice& curr) {
                                  return sum + curr.size();
                                });

  // The metadata change is deferred to Close(). We can't do
  // it now because the block's length is still in flux.
  int64_t cur_block_offset = block_offset_ + block_length_;
  RETURN_NOT_OK(container_->EnsurePreallocated(cur_block_offset, data_size));

  MicrosecondsInt64 start_time = GetMonoTimeMicros();
  RETURN_NOT_OK(container_->WriteVData(cur_block_offset, data));
  MicrosecondsInt64 end_time = GetMonoTimeMicros();

  int64_t dur = end_time - start_time;
  TRACE_COUNTER_INCREMENT("lbm_write_time_us", dur);
  const char* counter = BUCKETED_COUNTER_NAME("lbm_writes", dur);
  TRACE_COUNTER_INCREMENT(counter, 1);

  block_length_ += data_size;
  state_ = DIRTY;
  return Status::OK();
}

Status LogWritableBlock::FlushDataAsync() {
  VLOG(3) << "Flushing block " << id();
  RETURN_NOT_OK(container_->FlushData(block_offset_, block_length_));
  return Status::OK();
}

Status LogWritableBlock::Finalize() {
  DCHECK(state_ == CLEAN || state_ == DIRTY || state_ == FINALIZED)
      << "Invalid state: " << state_;

  if (state_ == FINALIZED) {
    return Status::OK();
  }

  SCOPED_CLEANUP({
    container_->FinalizeBlock(block_offset_, block_length_);
    state_ = FINALIZED;
  });

  VLOG(3) << "Finalizing block " << id();
  if (state_ == DIRTY &&
      FLAGS_block_manager_preflush_control == "finalize") {
    // We do not mark the container as read-only if FlushDataAsync() fails
    // since the corresponding metadata has not yet been appended.
    RETURN_NOT_OK(FlushDataAsync());
  }

  return Status::OK();
}

size_t LogWritableBlock::BytesAppended() const {
  return block_length_;
}

WritableBlock::State LogWritableBlock::state() const {
  return state_;
}

void LogWritableBlock::DoClose() {
  if (state_ == CLOSED) return;

  if (container_->metrics()) {
    container_->metrics()->generic_metrics.blocks_open_writing->Decrement();
    container_->metrics()->generic_metrics.total_bytes_written->IncrementBy(
        block_length_);
    container_->metrics()->generic_metrics.total_blocks_created->Increment();
  }

  // Finalize() was not called; this indicates we should
  // finalize the block.
  if (state_ == CLEAN || state_ == DIRTY) {
    container_->FinalizeBlock(block_offset_, block_length_);
  }

  LogBlockRefPtr lb = container_->block_manager()->CreateAndAddLogBlock(
      container_, block_id_, block_offset_, block_length_);
  CHECK(lb);
  container_->BlockCreated(lb);
  state_ = CLOSED;
}

////////////////////////////////////////////////////////////
// LogReadableBlock
////////////////////////////////////////////////////////////

// A log-backed block that has been opened for reading.
//
// Refers to a LogBlock representing the block's persisted metadata.
class LogReadableBlock : public ReadableBlock {
 public:
  explicit LogReadableBlock(LogBlockRefPtr log_block);

  ~LogReadableBlock();

  Status Close() override;

  const BlockId& id() const override;

  BlockManager* block_manager() const override;

  Status Size(uint64_t* sz) const override;

  Status Read(uint64_t offset, Slice result) const override;

  Status ReadV(uint64_t offset, ArrayView<Slice> results) const override;

  size_t memory_footprint() const override;

 private:
  // A reference to this block's metadata.
  LogBlockRefPtr log_block_;

  // Whether or not this block has been closed. Close() is thread-safe, so
  // this must be an atomic primitive.
  AtomicBool closed_;

  DISALLOW_COPY_AND_ASSIGN(LogReadableBlock);
};

LogReadableBlock::LogReadableBlock(LogBlockRefPtr log_block)
  : log_block_(std::move(log_block)),
    closed_(false) {
  if (log_block_->container()->metrics()) {
    log_block_->container()->metrics()->generic_metrics.blocks_open_reading->Increment();
    log_block_->container()->metrics()->generic_metrics.total_readable_blocks->Increment();
  }
}

LogReadableBlock::~LogReadableBlock() {
  WARN_NOT_OK(Close(), Substitute("Failed to close block $0",
                                  id().ToString()));
}

Status LogReadableBlock::Close() {
  if (closed_.CompareAndSet(false, true)) {
    if (log_block_->container()->metrics()) {
      log_block_->container()->metrics()->generic_metrics.blocks_open_reading->Decrement();
    }
    log_block_.reset();
  }

  return Status::OK();
}

BlockManager* LogReadableBlock::block_manager() const {
  return log_block_->container()->block_manager();
}

const BlockId& LogReadableBlock::id() const {
  return log_block_->block_id();
}

Status LogReadableBlock::Size(uint64_t* sz) const {
  DCHECK(!closed_.Load());

  *sz = log_block_->length();
  return Status::OK();
}

Status LogReadableBlock::Read(uint64_t offset, Slice result) const {
  return ReadV(offset, ArrayView<Slice>(&result, 1));
}

Status LogReadableBlock::ReadV(uint64_t offset, ArrayView<Slice> results) const {
  DCHECK(!closed_.Load());

  size_t read_length = accumulate(results.begin(), results.end(), static_cast<size_t>(0),
                                  [&](int sum, const Slice& curr) {
                                    return sum + curr.size();
                                  });

  uint64_t read_offset = log_block_->offset() + offset;
  if (log_block_->length() < offset + read_length) {
    return Status::IOError("Out-of-bounds read",
                           Substitute("read of [$0-$1) in block [$2-$3)",
                                      read_offset,
                                      read_offset + read_length,
                                      log_block_->offset(),
                                      log_block_->offset() + log_block_->length()));
  }

  MicrosecondsInt64 start_time = GetMonoTimeMicros();
  RETURN_NOT_OK(log_block_->container()->ReadVData(read_offset, results));
  MicrosecondsInt64 end_time = GetMonoTimeMicros();

  int64_t dur = end_time - start_time;
  TRACE_COUNTER_INCREMENT("lbm_read_time_us", dur);

  const char* counter = BUCKETED_COUNTER_NAME("lbm_reads", dur);
  TRACE_COUNTER_INCREMENT(counter, 1);

  if (log_block_->container()->metrics()) {
    log_block_->container()->metrics()->generic_metrics.total_bytes_read->IncrementBy(read_length);
  }
  return Status::OK();
}

size_t LogReadableBlock::memory_footprint() const {
  return kudu_malloc_usable_size(this);
}

} // namespace internal

////////////////////////////////////////////////////////////
// LogBlockManager
////////////////////////////////////////////////////////////

static const uint64_t kBlockMapChunk = 1 << 4;
static const uint64_t kBlockMapMask = kBlockMapChunk - 1;
const char* LogBlockManager::kContainerMetadataFileSuffix = ".metadata";
const char* LogBlockManager::kContainerDataFileSuffix = ".data";

// These values were arrived at via experimentation. See commit 4923a74 for
// more details.
const map<int64_t, int64_t> LogBlockManager::kPerFsBlockSizeBlockLimits({
  { 1024, 673 },
  { 2048, 1353 },
  { 4096, 2721 }});

LogBlockManager::LogBlockManager(Env* env,
                                 DataDirManager* dd_manager,
                                 FsErrorManager* error_manager,
                                 FileCache* file_cache,
                                 BlockManagerOptions opts)
  : env_(DCHECK_NOTNULL(env)),
    dd_manager_(DCHECK_NOTNULL(dd_manager)),
    error_manager_(DCHECK_NOTNULL(error_manager)),
    opts_(std::move(opts)),
    mem_tracker_(MemTracker::CreateTracker(-1,
                                           "log_block_manager",
                                           opts_.parent_mem_tracker)),
    file_cache_(file_cache),
    buggy_el6_kernel_(IsBuggyEl6Kernel(env->GetKernelRelease())),
    next_block_id_(1) {
  managed_block_shards_.resize(kBlockMapChunk);
  for (auto& mb : managed_block_shards_) {
    mb.lock = unique_ptr<simple_spinlock>(new simple_spinlock());
    mb.blocks_by_block_id
        = unique_ptr<BlockMap>(new BlockMap(10,
                                            BlockMap::hasher(),
                                            BlockMap::key_equal(),
                                            BlockAllocator(mem_tracker_)));
    mb.blocks_by_block_id->set_deleted_key(BlockId());
  }

  // HACK: when running in a test environment, we often instantiate many
  // LogBlockManagers in the same process, eg corresponding to different
  // tablet servers in a minicluster, or due to running many separate test
  // cases of some CFile-related code. In that case, we need to make it more
  // likely that the block IDs are not reused. So, instead of starting with
  // block ID 1, we'll start with a random block ID. A collision is still
  // possible, but exceedingly unlikely.
  if (IsGTest()) {
    Random r(GetRandomSeed32());
    next_block_id_.Store(r.Next64());
  }

  if (opts_.metric_entity) {
    metrics_.reset(new internal::LogBlockManagerMetrics(opts_.metric_entity));
  }
}

LogBlockManager::~LogBlockManager() {
  // Release all of the memory accounted by the blocks.
  int64_t mem = 0;
  for (const auto& mb : managed_block_shards_) {
    for (const auto& entry : *mb.blocks_by_block_id) {
      mem += kudu_malloc_usable_size(entry.second.get());
    }
  }
  mem_tracker_->Release(mem);

  // A LogBlock's destructor depends on its container, so all LogBlocks must be
  // destroyed before their containers.
  for (auto& mb : managed_block_shards_) {
    mb.blocks_by_block_id->clear();
  }

  // Containers may have outstanding tasks running on data directories; wait
  // for them to complete before destroying the containers.
  dd_manager_->WaitOnClosures();
}

// Ensure that no load task failed without being handled.
//
// Currently only disk failures are handled. Reports from failed disks are
// unusable.
#define RETURN_ON_NON_DISK_FAILURE(d, s)                        \
  do {                                                          \
    if (PREDICT_FALSE(!(s).ok())) {                             \
      if (!(s).IsDiskFailure()) {                               \
        return s;                                               \
      }                                                         \
      LOG(ERROR) << Substitute("Not using report from $0: $1",  \
          (d)->dir(), (s).ToString());                          \
    }                                                           \
  } while (false)

Status LogBlockManager::Open(FsReport* report, std::atomic<int>* containers_processed,
                             std::atomic<int>* containers_total) {
  // Establish (and log) block limits for each data directory using kernel,
  // filesystem, and gflags information.
  for (const auto& dd : dd_manager_->dirs()) {
    optional<int64_t> limit;
    if (FLAGS_log_container_max_blocks == -1) {
      // No limit, unless this is KUDU-1508.

      // The log block manager requires hole punching and, of the ext
      // filesystems, only ext4 supports it. Thus, if this is an ext
      // filesystem, it's ext4 by definition.
      if (buggy_el6_kernel_ && dd->fs_type() == FsType::EXT) {
        uint64_t fs_block_size =
            dd->instance()->metadata()->filesystem_block_size_bytes();
        bool untested_block_size =
            !ContainsKey(kPerFsBlockSizeBlockLimits, fs_block_size);
        string msg = Substitute(
            "Data dir $0 is on an ext4 filesystem vulnerable to KUDU-1508 "
            "with $1block size $2", dd->dir(),
            untested_block_size ? "untested " : "", fs_block_size);
        if (untested_block_size) {
          LOG(WARNING) << msg;
        } else {
          LOG(INFO) << msg;
        }
        limit = LookupBlockLimit(fs_block_size);
      }
    } else if (FLAGS_log_container_max_blocks > 0) {
      // Use the provided limit.
      limit = FLAGS_log_container_max_blocks;
    }

    if (limit) {
      LOG(INFO) << Substitute(
          "Limiting containers on data directory $0 to $1 blocks",
          dd->dir(), *limit);
    }
    InsertOrDie(&block_limits_by_data_dir_, dd.get(), limit);
  }

  // Open containers in each data dirs.
  vector<Status> statuses(dd_manager_->dirs().size());
  vector<vector<unique_ptr<internal::LogBlockContainerLoadResult>>> container_results(
      dd_manager_->dirs().size());
  int i = -1;
  for (const auto& dd : dd_manager_->dirs()) {
    i++;

    if (type_ == LogBlockManagerType::kRdb) {
      Status rdb_s = dd->InitRdb();
      if (rdb_s.IsIOError()) {
        statuses[i] = Status::IOError("Data directory failed", rdb_s.ToString(), EIO);
        continue;
      }
    }

    int uuid_idx;
    CHECK(dd_manager_->FindUuidIndexByDir(dd.get(), &uuid_idx));
    // TODO(awong): store Statuses for each directory in the directory manager
    // so we can avoid these artificial Statuses.
    if (dd_manager_->IsDirFailed(uuid_idx)) {
      statuses[i] = Status::IOError("Data directory failed", "", EIO);
      continue;
    }

    // Open the data dir asynchronously.
    auto* dd_raw = dd.get();
    auto* results = &container_results[i];
    auto* s = &statuses[i];
    dd->ExecClosure([this, dd_raw, results, s, containers_processed, containers_total]() {
      this->OpenDataDir(dd_raw, results, s, containers_processed, containers_total);
      WARN_NOT_OK(*s, Substitute("failed to open dir $0", dd_raw->dir()));
    });
  }

  // Wait for the opens to complete.
  dd_manager_->WaitOnClosures();

  // Check load errors and merge each data dir's container load results, then do repair tasks.
  unique_ptr<ThreadPool> repair_pool;
  RETURN_NOT_OK(ThreadPoolBuilder("repair_pool")
                    .set_max_threads(dd_manager_->dirs().size())
                    .Build(&repair_pool));
  vector<unique_ptr<internal::LogBlockContainerLoadResult>> dir_results(
      dd_manager_->dirs().size());
  for (int i = 0; i < dd_manager_->dirs().size(); ++i) {
    const auto& s = statuses[i];
    const auto& dd = dd_manager_->dirs()[i];
    RETURN_ON_NON_DISK_FAILURE(dd, s);
    // If open dir error, do not try to repair.
    if (PREDICT_FALSE(!s.ok())) {
      continue;
    }

    unique_ptr<internal::LogBlockContainerLoadResult> dir_result(
        new internal::LogBlockContainerLoadResult());
    dir_result->report.data_dirs.push_back(dd->dir());
    bool do_repair = true;
    for (const auto& container_result : container_results[i]) {
      RETURN_ON_NON_DISK_FAILURE(dd, container_result->status);
      if (PREDICT_FALSE(!s.ok())) {
        // If open container error, do not try to repair.
        do_repair = false;
        break;
      }

      dir_result->report.MergeFrom(container_result->report);
      dir_result->dead_containers.insert(
          dir_result->dead_containers.end(),
          container_result->dead_containers.begin(),
          container_result->dead_containers.end());
      container_result->dead_containers.clear();
      dir_result->low_live_block_containers.insert(
          container_result->low_live_block_containers.begin(),
          container_result->low_live_block_containers.end());
      container_result->low_live_block_containers.clear();
      dir_result->need_repunching_blocks.insert(
          dir_result->need_repunching_blocks.end(),
          container_result->need_repunching_blocks.begin(),
          container_result->need_repunching_blocks.end());
      container_result->need_repunching_blocks.clear();
    }
    if (do_repair) {
      dir_results[i] = std::move(dir_result);
      auto* dd_raw = dd.get();
      auto* dr = dir_results[i].get();
      CHECK_OK(repair_pool->Submit([this, dd_raw, dr]() { this->RepairTask(dd_raw, dr); }));
    }
  }

  // Wait for the repair tasks to complete.
  repair_pool->Wait();
  repair_pool->Shutdown();

  FsReport merged_report;
  for (int i = 0; i < dd_manager_->dirs().size(); ++i) {
    if (PREDICT_FALSE(!dir_results[i])) {
      continue;
    }
    if (PREDICT_TRUE(dir_results[i]->status.ok())) {
      merged_report.MergeFrom(dir_results[i]->report);
      continue;
    }
    RETURN_ON_NON_DISK_FAILURE(dd_manager_->dirs()[i], dir_results[i]->status);
  }

  if (dd_manager_->AreAllDirsFailed()) {
    return Status::IOError("All data dirs failed to open", "", EIO);
  }

  // Either return or log the report.
  if (report) {
    *report = std::move(merged_report);
  } else {
    RETURN_NOT_OK(merged_report.LogAndCheckForFatalErrors());
  }

  return Status::OK();
}

Status LogBlockManager::CreateBlock(const CreateBlockOptions& opts,
                                    unique_ptr<WritableBlock>* block) {
  CHECK(!opts_.read_only);

  // Find a free container. If one cannot be found, create a new one.
  //
  // TODO(unknown): should we cap the number of outstanding containers and
  // force callers to block if we've reached it?
  LogBlockContainerRefPtr container;
  RETURN_NOT_OK(GetOrCreateContainer(opts, &container));

  // Generate a free block ID.
  // We have to loop here because earlier versions used non-sequential block IDs,
  // and thus we may have to "skip over" some block IDs that are claimed.
  BlockId new_block_id;
  do {
    new_block_id.SetId(next_block_id_.Increment());
  } while (!TryUseBlockId(new_block_id));

  block->reset(new LogWritableBlock(container,
                                    new_block_id,
                                    container->next_block_offset()));
  VLOG(3) << "Created block " << (*block)->id() << " in container "
          << container->ToString();
  return Status::OK();
}

Status LogBlockManager::OpenBlock(const BlockId& block_id,
                                  unique_ptr<ReadableBlock>* block) {
  LogBlockRefPtr lb;
  {
    int index = block_id.id() & kBlockMapMask;
    std::lock_guard<simple_spinlock> l(*managed_block_shards_[index].lock);
    lb = FindPtrOrNull(*managed_block_shards_[index].blocks_by_block_id, block_id);
  }
  if (!lb) {
    return Status::NotFound("Can't find block when open log block", block_id.ToString());
  }

  VLOG(3) << "Opened block " << block_id
          << " from container " << lb->container()->ToString();
  block->reset(new internal::LogReadableBlock(std::move(lb)));
  return Status::OK();
}

unique_ptr<BlockCreationTransaction> LogBlockManager::NewCreationTransaction() {
  CHECK(!opts_.read_only);
  return std::make_unique<internal::LogBlockCreationTransaction>();
}

shared_ptr<BlockDeletionTransaction> LogBlockManager::NewDeletionTransaction() {
  CHECK(!opts_.read_only);
  return std::make_shared<internal::LogBlockDeletionTransaction>(this);
}

Status LogBlockManager::GetAllBlockIds(vector<BlockId>* block_ids) {
  block_ids->clear();
  for (const auto& mb : managed_block_shards_) {
    std::lock_guard<simple_spinlock> l(*mb.lock);
    AppendKeysFromMap(*mb.blocks_by_block_id, block_ids);
    block_ids->insert(block_ids->end(), mb.open_block_ids.begin(), mb.open_block_ids.end());
  }
  return Status::OK();
}

void LogBlockManager::NotifyBlockId(BlockId block_id) {
  next_block_id_.StoreMax(block_id.id() + 1);
}

void LogBlockManager::AddNewContainerUnlocked(const LogBlockContainerRefPtr& container) {
  DCHECK(lock_.is_locked());
  InsertOrDie(&all_containers_by_name_, container->ToString(), container);
  if (metrics()) {
    metrics()->containers->Increment();
    if (container->full()) {
      metrics()->full_containers->Increment();
    }
  }
}

void LogBlockManager::RemoveDeadContainerUnlocked(const string& container_name) {
  DCHECK(lock_.is_locked());
  LogBlockContainerRefPtr to_delete(EraseKeyReturnValuePtr(
      &all_containers_by_name_, container_name));
  CHECK(to_delete);
  CHECK(to_delete->full())
      << Substitute("Container $0 is not full", container_name);
  CHECK(to_delete->dead())
      << Substitute("Container $0 is not dead", container_name);
  if (metrics()) {
    metrics()->containers->Decrement();
    metrics()->full_containers->Decrement();
  }
}

void LogBlockManager::RemoveDeadContainer(const string& container_name) {
  // Remove the container from memory.
  {
    std::lock_guard<simple_spinlock> l(lock_);
    RemoveDeadContainerUnlocked(container_name);
  }

  // Update the metrics if necessary.
  if (metrics()) {
    metrics()->dead_containers_deleted->Increment();
  }
}

Status LogBlockManager::GetOrCreateContainer(const CreateBlockOptions& opts,
                                             LogBlockContainerRefPtr* container) {
  Dir* dir;
  RETURN_NOT_OK_EVAL(dd_manager_->GetDirAddIfNecessary(opts, &dir),
      error_manager_->RunErrorNotificationCb(ErrorHandlerType::NO_AVAILABLE_DISKS, opts.tablet_id));

  {
    std::lock_guard<simple_spinlock> l(lock_);
    auto& d = available_containers_by_data_dir_[DCHECK_NOTNULL(dir)];
    if (!d.empty()) {
      *container = d.front();
      d.pop_front();
      return Status::OK();
    }
  }

  // All containers are in use; create a new one.
  LogBlockContainerRefPtr new_container;
  Status s;
  if (type_ == LogBlockManagerType::kFile) {
    s = LogfBlockContainer::Create(this, dir, &new_container);
  } else {
    DCHECK_EQ(type_, LogBlockManagerType::kRdb);
    s = LogrBlockContainer::Create(this, dir, &new_container);
  }

  // We could create a container in a different directory, but there's
  // currently no point in doing so. On disk failure, the tablet specified by
  // 'opts' will be shut down, so the returned container would not be used.
  HANDLE_DISK_FAILURE(s, error_manager_->RunErrorNotificationCb(ErrorHandlerType::DISK_ERROR, dir));
  RETURN_NOT_OK_PREPEND(s, "Could not create new log block container at " + dir->dir());
  {
    std::lock_guard<simple_spinlock> l(lock_);
    dirty_dirs_.insert(dir->dir());
    AddNewContainerUnlocked(new_container);
  }
  *container = std::move(new_container);
  return Status::OK();
}

void LogBlockManager::MakeContainerAvailable(LogBlockContainerRefPtr container) {
  std::lock_guard<simple_spinlock> l(lock_);
  MakeContainerAvailableUnlocked(std::move(container));
}

void LogBlockManager::MakeContainerAvailableUnlocked(LogBlockContainerRefPtr container) {
  DCHECK(lock_.is_locked());
  if (container->full() || container->read_only()) {
    return;
  }
  VLOG(3) << Substitute("container $0 being made available", container->ToString());
  available_containers_by_data_dir_[container->data_dir()].push_front(std::move(container));
}

Status LogBlockManager::SyncContainer(const LogBlockContainer& container) {
  Status s;
  bool to_sync = false;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    to_sync = dirty_dirs_.erase(container.data_dir()->dir());
  }

  if (to_sync && FLAGS_enable_data_block_fsync) {
    if (metrics_) metrics_->generic_metrics.total_disk_sync->Increment();
    s = env_->SyncDir(container.data_dir()->dir());

    // If SyncDir fails, the container directory must be restored to
    // dirty_dirs_. Otherwise a future successful LogWritableBlock::Close()
    // on this container won't call SyncDir again, and the container might
    // be lost on crash.
    //
    // In the worst case (another block synced this container as we did),
    // we'll sync it again needlessly.
    if (!s.ok()) {
      container.HandleError(s);
      std::lock_guard<simple_spinlock> l(lock_);
      dirty_dirs_.insert(container.data_dir()->dir());
    }
  }
  return s;
}

bool LogBlockManager::TryUseBlockId(const BlockId& block_id) {
  if (block_id.IsNull()) {
    return false;
  }

  int index = block_id.id() & kBlockMapMask;
  std::lock_guard<simple_spinlock> l(*managed_block_shards_[index].lock);
  if (ContainsKey(*managed_block_shards_[index].blocks_by_block_id, block_id)) {
    return false;
  }

  return InsertIfNotPresent(&managed_block_shards_[index].open_block_ids, block_id);
}

LogBlockRefPtr LogBlockManager::CreateAndAddLogBlock(
    LogBlockContainerRefPtr container,
    const BlockId& block_id,
    int64_t offset,
    int64_t length) {
  LogBlockRefPtr lb(new LogBlock(std::move(container), block_id, offset, length));
  mem_tracker_->Consume(kudu_malloc_usable_size(lb.get()));

  if (AddLogBlock(lb)) {
    return lb;
  }
  return nullptr;
}

bool LogBlockManager::AddLogBlock(LogBlockRefPtr lb) {
  // InsertIfNotPresent doesn't use move semantics, so instead we just
  // insert an empty scoped_refptr and assign into it down below rather
  // than using the utility function.
  int index = lb->block_id().id() & kBlockMapMask;
  std::lock_guard<simple_spinlock> l(*managed_block_shards_[index].lock);
  auto& blocks_by_block_id = *managed_block_shards_[index].blocks_by_block_id;
  LogBlockRefPtr* entry_ptr = &blocks_by_block_id[lb->block_id()];
  if (*entry_ptr) {
    // Already have an entry for this block ID.
    return false;
  }

  VLOG(2) << Substitute("Added block: id $0, offset $1, length $2",
                        lb->block_id().ToString(), lb->offset(), lb->length());

  // There may already be an entry in open_block_ids_arr_ (e.g. we just finished
  // writing out a block).
  managed_block_shards_[index].open_block_ids.erase(lb->block_id());
  if (metrics()) {
    metrics()->blocks_under_management->Increment();
    metrics()->bytes_under_management->IncrementBy(lb->length());
  }

  *entry_ptr = std::move(lb);
  return true;
}

Status LogBlockManager::RemoveLogBlocks(const vector<BlockId>& block_ids,
                                        vector<LogBlockRefPtr>* log_blocks,
                                        vector<BlockId>* deleted) {
  Status first_failure;
  vector<LogBlockRefPtr> lbs;
  lbs.reserve(block_ids.size());
  log_blocks->reserve(block_ids.size());
  if (deleted) {
    deleted->reserve(block_ids.size());
  }
  int64_t malloc_space = 0, blocks_length = 0;
  for (const auto& block_id : block_ids) {
    LogBlockRefPtr lb;
    Status s = RemoveLogBlock(block_id, &lb);
    if (!s.ok() && !s.IsNotFound()) {
      if (first_failure.ok()) first_failure = s;
    } else if (s.ok()) {
      malloc_space += kudu_malloc_usable_size(lb.get());
      blocks_length += lb->length();
      lbs.emplace_back(std::move(lb));
    } else {
      // If we get NotFound, then the block was already deleted.
      DCHECK(s.IsNotFound());
      if (deleted) {
        deleted->emplace_back(block_id);
      }
    }
  }

  // Update various metrics.
  mem_tracker_->Release(malloc_space);
  if (metrics()) {
    metrics()->blocks_under_management->DecrementBy(lbs.size());
    metrics()->bytes_under_management->DecrementBy(blocks_length);
  }

  unordered_map<LogBlockContainer*, vector<LogBlockRefPtr>> lbs_by_containers;
  for (auto& lb : lbs) {
    auto& lbs_by_container = LookupOrInsert(&lbs_by_containers, lb->container(), {});
    lbs_by_container.emplace_back(std::move(lb));
  }

  for (auto& lbs_by_container : lbs_by_containers) {
    LogBlockContainer* container = lbs_by_container.first;
    vector<BlockId> to_delete_block_ids;
    to_delete_block_ids.reserve(lbs_by_container.second.size());
    for (const auto& lb : lbs_by_container.second) {
      VLOG(3) << "Deleting block " << lb->block_id();
      container->BlockDeleted(lb);

      to_delete_block_ids.emplace_back(lb->block_id());
    }
    // Record the on-disk deletion.
    //
    // TODO(unknown): what if this fails? Should we restore the in-memory block?
    Status s = container->AppendMetadataForBatchDelete(to_delete_block_ids);

    // We don't bother fsyncing the metadata append for deletes in order to avoid
    // the disk overhead. Even if we did fsync it, we'd still need to account for
    // garbage at startup time (in the event that we crashed just before the
    // fsync).
    //
    // TODO(KUDU-829): Implement GC of orphaned blocks.
    if (!s.ok()) {
      if (first_failure.ok()) {
        first_failure = s.CloneAndPrepend("Unable to append deletion record to block metadata");
      }
      continue;
    }

    if (type_ == LogBlockManagerType::kFile) {
      // Metadata files of containers with very few live blocks will be compacted.
      if (!container->read_only() && FLAGS_log_container_metadata_runtime_compact &&
          container->ShouldCompact()) {
        scoped_refptr<LogBlockContainer> self(container);
        container->ExecClosure([self]() { self->CompactMetadata(); });
      }
    }

    if (deleted) {
      deleted->insert(deleted->end(), to_delete_block_ids.begin(), to_delete_block_ids.end());
    }
    log_blocks->insert(log_blocks->end(),
                       lbs_by_container.second.begin(),
                       lbs_by_container.second.end());
  }

  return first_failure;
}

Status LogBlockManager::RemoveLogBlock(const BlockId& block_id,
                                       LogBlockRefPtr* lb) {
  int index = block_id.id() & kBlockMapMask;
  std::lock_guard<simple_spinlock> l(*managed_block_shards_[index].lock);
  auto& blocks_by_block_id = managed_block_shards_[index].blocks_by_block_id;

  auto it = blocks_by_block_id->find(block_id);
  if (it == blocks_by_block_id->end()) {
    return Status::NotFound("Can't find block when remove log block", block_id.ToString());
  }

  LogBlockContainer* container = it->second->container();
  HANDLE_DISK_FAILURE(container->read_only_status(),
      error_manager_->RunErrorNotificationCb(ErrorHandlerType::DISK_ERROR, container->data_dir()));

  // Return early if deleting a block in a failed directory.
  set<int> failed_dirs = dd_manager_->GetFailedDirs();
  if (PREDICT_FALSE(!failed_dirs.empty())) {
    int uuid_idx;
    CHECK(dd_manager_->FindUuidIndexByDir(container->data_dir(), &uuid_idx));
    if (ContainsKey(failed_dirs, uuid_idx)) {
      LOG_EVERY_N(INFO, 10) << Substitute("Block $0 is in a failed directory; not deleting",
                                          block_id.ToString());
      return Status::IOError("Block is in a failed directory");
    }
  }
  *lb = std::move(it->second);
  blocks_by_block_id->erase(it);

  VLOG(2) << Substitute("Removed block: id $0, offset $1, length $2",
                        (*lb)->block_id().ToString(), (*lb)->offset(), (*lb)->length());
  return Status::OK();
}

void LogBlockManager::OpenDataDir(
    Dir* dir,
    vector<unique_ptr<internal::LogBlockContainerLoadResult>>* results,
    Status* result_status,
    std::atomic<int>* containers_processed,
    std::atomic<int>* containers_total) {
  vector<string> children;
  Status s = env_->GetChildren(dir->dir(), &children);
  if (!s.ok()) {
    HANDLE_DISK_FAILURE(s, error_manager_->RunErrorNotificationCb(
        ErrorHandlerType::DISK_ERROR, dir));
    *result_status = s.CloneAndPrepend(Substitute(
        "Could not list children of $0", dir->dir()));
    return;
  }

  // TODO: if data file is not exist, but metadata in rdb exists, we should check this case.
  // Find all containers and open them.
  unordered_set<string> containers_seen;
  results->reserve(children.size() / 2);  // TODO: rdb and file
  for (const string& child : children) {
    string container_name;
    if (!TryStripSuffixString(
            child, LogBlockManager::kContainerDataFileSuffix, &container_name) &&
        !TryStripSuffixString(
            child, LogBlockManager::kContainerMetadataFileSuffix, &container_name)) {
      continue;
    }
    InsertIfNotPresent(&containers_seen, container_name);
  }

  if (containers_total) {
    *containers_total += containers_seen.size();
    if (metrics_) {
      metrics()->total_containers_startup->IncrementBy(containers_seen.size());
    }
  }

  for (const string& container_name : containers_seen) {
    // Add a new result for the container.
    results->emplace_back(new internal::LogBlockContainerLoadResult());
    LogBlockContainerRefPtr container;
    if (type_ == LogBlockManagerType::kFile) {
      s = LogfBlockContainer::Open(
          this, dir, &results->back()->report, container_name, &container);
    } else {
      s = LogrBlockContainer::Open(
          this, dir, &results->back()->report, container_name, &container);
    }
    if (containers_processed) {
      ++*containers_processed;
      if (metrics_) {
        metrics()->processed_containers_startup->Increment();
      }
    }
    if (!s.ok()) {
      if (s.IsAborted()) {
        // Skip the container. Open() added a record of it to 'results->back()->report' for us.
        continue;
      }
      if (opts_.read_only && s.IsNotFound()) {
        // Skip the container while the operation is read-only and the files are away,
        // especially for the kudu cli tool.
        continue;
      }
      *result_status = s.CloneAndPrepend(
          Substitute("Could not open container $0, directory: $1", container_name, dir->dir()));
      return;
    }

    // Load the container's records asynchronously.
    auto* r = results->back().get();
    dir->ExecClosure([this, dir, container, r]() {
      this->LoadContainer(dir, container, r);
    });
  }
}

void LogBlockManager::LoadContainer(Dir* dir,
                                    LogBlockContainerRefPtr container,
                                    internal::LogBlockContainerLoadResult* result) {
  // Process the records, building a container-local map for live blocks and
  // a list of dead blocks.
  //
  // It's important that we don't try to add these blocks to the global map
  // incrementally as we see each record, since it's possible that one container
  // has a "CREATE <b>" while another has a "CREATE <b> ; DELETE <b>" pair.
  // If we processed those two containers in this order, then upon processing
  // the second container, we'd think there was a duplicate block. Building
  // the container-local map first ensures that we discount deleted blocks
  // before checking for duplicate IDs.
  //
  // NOTE: Since KUDU-1538, we allocate sequential block IDs, which makes reuse
  // exceedingly unlikely. However, we might have old data which still exhibits
  // the above issue.
  UntrackedBlockMap live_blocks;
  BlockRecordMap live_block_records;
  vector<LogBlockRefPtr> dead_blocks;
  uint64_t max_block_id = 0;
  Status s = container->ProcessRecords(&result->report,
                                       &live_blocks,
                                       &live_block_records,
                                       &dead_blocks,
                                       &max_block_id,
                                       LogBlockContainer::ProcessRecordType::kReadAndUpdate);
  if (!s.ok()) {
    result->status = s.CloneAndPrepend(Substitute(
        "Could not process records in container $0", container->ToString()));
    return;
  }

  // With deleted blocks out of the way, check for misaligned blocks.
  //
  // We could also enforce that the record's offset is aligned with the
  // underlying filesystem's block size, an invariant maintained by the log
  // block manager. However, due to KUDU-1793, that invariant may have been
  // broken, so we'll note but otherwise allow it.
  for (const auto& e : live_blocks) {
    if (PREDICT_FALSE(e.second->offset() %
                      container->instance()->filesystem_block_size_bytes() != 0)) {
      result->report.misaligned_block_check->entries.emplace_back(
          container->ToString(), e.first);

    }
  }

  if (container->full()) {
    // Full containers without any live blocks can be deleted outright.
    //
    // TODO(adar): this should be reported as an inconsistency once dead
    // container deletion is also done in real time. Until then, it would be
    // confusing to report it as such since it'll be a natural event at startup.
    if (container->live_blocks() == 0) {
      DCHECK(live_blocks.empty());
      result->dead_containers.emplace_back(container);
    } else if (static_cast<double>(container->live_blocks()) /
        container->total_blocks() <= FLAGS_log_container_live_metadata_before_compact_ratio) {
      // Metadata files of containers with very few live blocks will be compacted.
      vector<BlockRecordPB> records = LogBlockContainer::SortRecords(std::move(live_block_records));
      result->low_live_block_containers[container->ToString()] = std::move(records);
    }

    // Having processed the block records, let's check whether any full
    // containers have any extra space (left behind after a crash or from an
    // older version of Kudu).
    //
    // Filesystems are unpredictable beasts and may misreport the amount of
    // space allocated to a file in various interesting ways. Some examples:
    // - XFS's speculative preallocation feature may artificially enlarge the
    //   container's data file without updating its file size. This makes the
    //   file size untrustworthy for the purposes of measuring allocated space.
    //   See KUDU-1856 for more details.
    // - On el6.6/ext4 a container data file that consumed ~32K according to
    //   its extent tree was actually reported as consuming an additional fs
    //   block (2k) of disk space. A similar container data file (generated
    //   via the same workload) on Ubuntu 16.04/ext4 did not exhibit this.
    //   The suspicion is that older versions of ext4 include interior nodes
    //   of the extent tree when reporting file block usage.
    //
    // To deal with these issues, our extra space cleanup code (deleted block
    // repunching and container truncation) is gated on an "actual disk space
    // consumed" heuristic. To prevent unnecessary triggering of the
    // heuristic, we allow for some slop in our size measurements. The exact
    // amount of slop is configurable via
    // log_container_excess_space_before_cleanup_fraction.
    //
    // Too little slop and we'll do unnecessary work at startup. Too much and
    // more unused space may go unreclaimed.
    string data_filename = StrCat(container->ToString(), kContainerDataFileSuffix);
    uint64_t reported_size;
    s = env_->GetFileSizeOnDisk(data_filename, &reported_size);
    if (!s.ok()) {
      HANDLE_DISK_FAILURE(s, error_manager_->RunErrorNotificationCb(
          ErrorHandlerType::DISK_ERROR, dir));
      result->status = s.CloneAndPrepend(Substitute(
          "Could not get on-disk file size of container $0", container->ToString()));
      return;
    }
    int64_t cleanup_threshold_size = container->live_bytes_aligned() *
        (1 + FLAGS_log_container_excess_space_before_cleanup_fraction);
    if (reported_size > cleanup_threshold_size) {
      result->report.full_container_space_check->entries.emplace_back(
          container->ToString(), reported_size - container->live_bytes_aligned());

      // If the container is to be deleted outright, don't bother repunching
      // its blocks. The report entry remains, however, so it's clear that
      // there was a space discrepancy.
      if (container->live_blocks()) {
        result->need_repunching_blocks.insert(result->need_repunching_blocks.end(),
                                              dead_blocks.begin(), dead_blocks.end());
      }
    }

    result->report.stats.lbm_full_container_count++;
  }
  result->report.stats.live_block_bytes += container->live_bytes();
  result->report.stats.live_block_bytes_aligned += container->live_bytes_aligned();
  result->report.stats.live_block_count += container->live_blocks();
  result->report.stats.lbm_container_count++;

  next_block_id_.StoreMax(max_block_id + 1);

  int64_t mem_usage = 0;
  for (UntrackedBlockMap::value_type& e : live_blocks) {
    int block_mem = kudu_malloc_usable_size(e.second.get());
    if (!AddLogBlock(std::move(e.second))) {
      // TODO(adar): track as an inconsistency?
      LOG(FATAL) << "Found duplicate CREATE record for block " << e.first
                 << " which already is alive from another container when "
                 << " processing container " << container->ToString();
    }
    mem_usage += block_mem;
  }

  mem_tracker_->Consume(mem_usage);

  int64_t container_count = 0;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    AddNewContainerUnlocked(container);
    MakeContainerAvailableUnlocked(std::move(container));
    container_count = all_containers_by_name_.size();
  }

  // Log every 200 number of log block containers
  if (container_count % 200 == 0) {
    LOG(INFO) << Substitute("Opened $0 log block containers", container_count);
  }
}

void LogBlockManager::RepairTask(Dir* dir, internal::LogBlockContainerLoadResult* result) {
  result->status = Repair(dir,
                          &result->report,
                          std::move(result->need_repunching_blocks),
                          std::move(result->dead_containers),
                          std::move(result->low_live_block_containers));
}

#define RETURN_NOT_OK_LBM_DISK_FAILURE_PREPEND(status_expr, msg) do { \
  Status s_ = (status_expr); \
  s_ = s_.CloneAndPrepend(msg); \
  RETURN_NOT_OK_HANDLE_DISK_FAILURE(s_, \
      error_manager_->RunErrorNotificationCb(ErrorHandlerType::DISK_ERROR, dir)); \
} while (0)

#define WARN_NOT_OK_LBM_DISK_FAILURE(status_expr, msg) do { \
  Status s_ = (status_expr); \
  HANDLE_DISK_FAILURE(s_, error_manager_->RunErrorNotificationCb( \
      ErrorHandlerType::DISK_ERROR, dir)); \
  WARN_NOT_OK(s_, msg); \
} while (0)

Status LogBlockManager::Repair(
    Dir* dir,
    FsReport* report,
    vector<LogBlockRefPtr> need_repunching,
    vector<LogBlockContainerRefPtr> dead_containers,
    unordered_map<string, vector<BlockRecordPB>> low_live_block_containers) {
  if (opts_.read_only) {
    LOG(INFO) << "Read-only block manager, skipping repair";
    return Status::OK();
  }
  if (report->HasFatalErrors()) {
    LOG(WARNING) << "Found fatal and irreparable errors, skipping repair";
    return Status::OK();
  }

  // From here on out we're committed to repairing.

  // Fetch all the containers we're going to need.
  unordered_map<string, LogBlockContainerRefPtr> containers_by_name;
  {
    std::lock_guard<simple_spinlock> l(lock_);

    // Remove all of the dead containers from the block manager. They will be
    // deleted from disk shortly thereafter, outside of the lock.
    for (const auto& d : dead_containers) {
      CHECK(d->TrySetDead());
      RemoveDeadContainerUnlocked(d->ToString());

      // This must be true if clearing dead_containers below is to delete the
      // container files.
      DCHECK(d->HasOneRef());
    }

    // Fetch all the containers we're going to need.
    if (report->partial_record_check) {
      for (const auto& pr : report->partial_record_check->entries) {
        LogBlockContainerRefPtr c = FindPtrOrNull(all_containers_by_name_,
                                                  pr.container);
        if (c) {
          containers_by_name[pr.container] = c;
        }
      }
    }
    if (report->full_container_space_check) {
      for (const auto& fcp : report->full_container_space_check->entries) {
        LogBlockContainerRefPtr c = FindPtrOrNull(all_containers_by_name_,
                                                  fcp.container);
        if (c) {
          containers_by_name[fcp.container] = c;
        }
      }
    }
    if (report->partial_rdb_record_check) {
      for (const auto& e : report->partial_rdb_record_check->entries) {
        LogBlockContainerRefPtr c = FindPtrOrNull(all_containers_by_name_,
                                                  e.container);
        if (c) {
          containers_by_name[e.container] = c;
        }
      }
    }
    for (const auto& e : low_live_block_containers) {
      LogBlockContainerRefPtr c = FindPtrOrNull(all_containers_by_name_,
                                                e.first);
      if (c) {
        containers_by_name[e.first] = c;
      }
    }
  }

  // Clearing this vector drops the last references to the containers within,
  // triggering their deletion.
  dead_containers.clear();

  // Truncate partial metadata records.
  //
  // This is a fatal inconsistency; if the repair fails, we cannot proceed.
  if (report->partial_record_check) {
    for (auto& pr : report->partial_record_check->entries) {
      LogBlockContainerRefPtr container = FindPtrOrNull(containers_by_name, pr.container);
      if (!container) {
        // The container was deleted outright.
        pr.repaired = true;
        continue;
      }

      unique_ptr<RWFile> file;
      RWFileOptions opts;
      opts.mode = Env::MUST_EXIST;
      opts.is_sensitive = true;
      RETURN_NOT_OK_LBM_DISK_FAILURE_PREPEND(
          env_->NewRWFile(opts,
                          StrCat(pr.container, kContainerMetadataFileSuffix),
                          &file),
          "could not reopen container to truncate partial metadata record");

      RETURN_NOT_OK_LBM_DISK_FAILURE_PREPEND(file->Truncate(pr.offset),
          "could not truncate partial metadata record");

      // Technically we've "repaired" the inconsistency if the truncation
      // succeeded, even if the following logic fails.
      pr.repaired = true;

      RETURN_NOT_OK_LBM_DISK_FAILURE_PREPEND(file->Close(),
          "could not close container after truncating partial metadata record");

      // Reopen the PB writer so that it will refresh its metadata about the
      // underlying file and resume appending to the new end of the file.
      RETURN_NOT_OK_LBM_DISK_FAILURE_PREPEND(container->ReopenMetadataWriter(),
          "could not reopen container metadata file");
    }
  }

  // Delete any incomplete container files.
  //
  // This is a non-fatal inconsistency; we can just as easily ignore the
  // leftover container files.
  if (report->incomplete_container_check) {
    for (auto& ic : report->incomplete_container_check->entries) {
      if (type_ == LogBlockManagerType::kFile) {
        Status s = env_->DeleteFile(StrCat(ic.container, kContainerMetadataFileSuffix));
        if (!s.ok() && !s.IsNotFound()) {
          WARN_NOT_OK_LBM_DISK_FAILURE(s, "could not delete incomplete container metadata file");
        }
      } else {
        DCHECK_EQ(type_, LogBlockManagerType::kRdb);
        vector<string> kv = Split(ic.container, "/", SkipEmpty());
        CHECK(!kv.empty());
        string id = kv[kv.size() - 1];
        rocksdb::WriteOptions del_opt;
        rocksdb::Slice begin_key = id;
        string next_id = ObjectIdGenerator::NextOf(id);
        rocksdb::Slice end_key = next_id;
        auto s =
            dir->rdb()->DeleteRange(del_opt, dir->rdb()->DefaultColumnFamily(), begin_key, end_key);
        CHECK_OK(FromRdbStatus(s));
      }

      Status s = env_->DeleteFile(StrCat(ic.container, kContainerDataFileSuffix));
      if (!s.ok() && !s.IsNotFound()) {
        WARN_NOT_OK_LBM_DISK_FAILURE(s, "could not delete incomplete container data file");
      }
      ic.repaired = true;
    }
  }

  // Truncate any excess preallocated space in full containers.
  //
  // This is a non-fatal inconsistency; we can just as easily ignore the extra
  // disk space consumption.
  if (report->full_container_space_check) {
    for (auto& fcp : report->full_container_space_check->entries) {
      LogBlockContainerRefPtr container = FindPtrOrNull(containers_by_name, fcp.container);
      if (!container) {
        // The container was deleted outright.
        fcp.repaired = true;
        continue;
      }

      Status s = container->TruncateDataToNextBlockOffset();
      if (s.ok()) {
        fcp.repaired = true;
      }
      WARN_NOT_OK(s, "could not truncate excess preallocated space");
    }
  }

  if (report->partial_rdb_record_check) {
    CHECK(type_ == LogBlockManagerType::kRdb || report->partial_rdb_record_check->entries.empty());
    for (auto& e : report->partial_rdb_record_check->entries) {
      // TODO: optimize to delete by batch
      rocksdb::WriteOptions del_opt;
      rocksdb::Slice key(e.block_id);
      auto s = dir->rdb()->Delete(del_opt, key);
      CHECK_OK(FromRdbStatus(s));
      e.repaired = true;
    }
  }

  // Repunch all requested holes. Any excess space reclaimed was already
  // tracked by LBMFullContainerSpaceCheck.
  //
  // Register deletions to a single BlockDeletionTransaction. So, the repunched
  // holes belonging to the same container can be coalesced.
  auto transaction = std::make_shared<LogBlockDeletionTransaction>(this);
  for (const auto& b : need_repunching) {
    b->RegisterDeletion(transaction);
    transaction->AddBlock(b);
  }

  // Clearing this vector drops the last references to the LogBlocks within,
  // triggering the repunching operations.
  need_repunching.clear();

  // "Compact" metadata files with few live blocks by rewriting them with only
  // the live block records.
  std::atomic<int64_t> metadata_files_compacted = 0;
  std::atomic<int64_t> metadata_bytes_delta = 0;
  std::atomic<bool> seen_fatal_error = false;
  Status first_fatal_error;
  SCOPED_LOG_TIMING(INFO, "loading block containers with low live blocks");
  for (const auto& e : low_live_block_containers) {
    if (seen_fatal_error) {
      break;
    }
    LogBlockContainerRefPtr container = FindPtrOrNull(containers_by_name, e.first);
    if (!container) {
      // The container was deleted outright.
      continue;
    }

    dir->ExecClosure([this, &metadata_files_compacted, &metadata_bytes_delta,
        &seen_fatal_error, &first_fatal_error, e, container]() {
      // Rewrite this metadata file.
      int64_t file_bytes_delta;
      const auto& meta_path = StrCat(e.first, kContainerMetadataFileSuffix);
      Status s = RewriteMetadataFile(*(container.get()), e.second, &file_bytes_delta);
      if (!s.ok()) {
        // Rewrite metadata file failure is non-fatal, just skip it.
        WARN_NOT_OK(s, Substitute("could not rewrite metadata file $0", meta_path));
        return;
      }

      // Reopen the new metadata file.
      s = container->ReopenMetadataWriter();
      if (!s.ok()) {
        // Open the new metadata file failure is fatal, stop processing other containers.
        bool current_seen_error = false;
        if (seen_fatal_error.compare_exchange_strong(current_seen_error, true)) {
          first_fatal_error = s.CloneAndPrepend(
              Substitute("could not reopen new metadata file", meta_path));
        }
        return;
      }

      metadata_files_compacted++;
      metadata_bytes_delta += file_bytes_delta;
      VLOG(1) << "Compacted metadata file " << meta_path << " (saved " << file_bytes_delta
              << " bytes)";
    });
  }

  dir->WaitOnClosures();
  if (seen_fatal_error) {
    LOG_AND_RETURN(WARNING, first_fatal_error);
  }

  // The data directory can be synchronized once for all of the new metadata files.
  //
  // Non-disk failures are fatal: if a new metadata file doesn't durably exist
  // in the data directory, it would be unsafe to append new block records to
  // it. This is because after a crash the old metadata file may appear
  // instead, and that file lacks the newly appended block records.
  //
  // TODO(awong): The below will only be true with persistent disk states.
  // Disk failures do not suffer from this issue because, on the next startup,
  // the entire directory will not be used.
  if (metadata_files_compacted.load() > 0) {
    Status s = env_->SyncDir(dir->dir());
    RETURN_NOT_OK_LBM_DISK_FAILURE_PREPEND(s, "Could not sync data directory");
    LOG(INFO) << Substitute("Compacted $0 metadata files ($1 metadata bytes)",
                            metadata_files_compacted.load(), metadata_bytes_delta.load());
  }

  return Status::OK();
}

Status LogBlockManager::RewriteMetadataFile(const LogBlockContainer& container,
                                            const vector<BlockRecordPB>& records,
                                            int64_t* file_bytes_delta) {
  MAYBE_INJECT_FIXED_LATENCY(FLAGS_log_container_metadata_rewrite_inject_latency_ms);

  const string metadata_file_name = StrCat(container.ToString(), kContainerMetadataFileSuffix);
  // Get the container's data directory's UUID for error handling.
  const string dir = container.data_dir()->dir();

  uint64_t old_metadata_size;
  RETURN_NOT_OK_LBM_DISK_FAILURE_PREPEND(env_->GetFileSize(metadata_file_name, &old_metadata_size),
                                         "could not get size of old metadata file");

  // Create a new container metadata file with only the live block records.
  //
  // By using a temporary file and renaming it over the original file at the
  // end, we ensure that we can recover from a failure at any point. Any
  // temporary files left behind are cleaned up by the FsManager at startup.
  string tmpl = metadata_file_name + kTmpInfix + ".XXXXXX";
  unique_ptr<RWFile> tmp_file;
  string tmp_file_name;
  RWFileOptions opts;
  opts.is_sensitive = true;
  RETURN_NOT_OK_LBM_DISK_FAILURE_PREPEND(env_->NewTempRWFile(opts, tmpl,
                                                             &tmp_file_name, &tmp_file),
                                         "could not create temporary metadata file");
  auto tmp_deleter = MakeScopedCleanup([&]() {
    WARN_NOT_OK(env_->DeleteFile(tmp_file_name),
                "Could not delete file " + tmp_file_name);

  });
  WritablePBContainerFile pb_file(std::move(tmp_file));
  RETURN_NOT_OK_LBM_DISK_FAILURE_PREPEND(pb_file.CreateNew(BlockRecordPB()),
                                         "could not initialize temporary metadata file");
  for (const auto& r : records) {
    RETURN_NOT_OK_LBM_DISK_FAILURE_PREPEND(pb_file.Append(r),
                                           "could not append to temporary metadata file");
  }
  RETURN_NOT_OK_LBM_DISK_FAILURE_PREPEND(pb_file.Sync(),
                                         "could not sync temporary metadata file");
  RETURN_NOT_OK_LBM_DISK_FAILURE_PREPEND(pb_file.Close(),
                                         "could not close temporary metadata file");
  uint64_t new_metadata_size;
  RETURN_NOT_OK_LBM_DISK_FAILURE_PREPEND(env_->GetFileSize(tmp_file_name, &new_metadata_size),
                                         "could not get file size of temporary metadata file");
  RETURN_NOT_OK_LBM_DISK_FAILURE_PREPEND(env_->RenameFile(tmp_file_name, metadata_file_name),
                                         "could not rename temporary metadata file");
  if (PREDICT_TRUE(file_cache_)) {
    // Evict the old path from the file cache, so that when we re-open the new
    // metadata file for write, we don't accidentally get a cache hit on the
    // old file descriptor pointing to the now-deleted old version.
    file_cache_->Invalidate(metadata_file_name);
  }

  tmp_deleter.cancel();
  *file_bytes_delta = (static_cast<int64_t>(old_metadata_size) - new_metadata_size);
  return Status::OK();
}

string LogBlockManager::ContainerPathForTests(internal::LogBlockContainer* container) {
  return container->ToString();
}

bool LogBlockManager::IsBuggyEl6Kernel(const string& kernel_release) {
  autodigit_less lt;

  // Only el6 is buggy.
  if (kernel_release.find("el6") == string::npos) return false;

  // Kernels in the 6.8 update stream (2.6.32-642.a.b) are fixed
  // for a >= 15.
  //
  // https://rhn.redhat.com/errata/RHSA-2017-0307.html
  if (MatchPattern(kernel_release, "2.6.32-642.*.el6.*") &&
      lt("2.6.32-642.15.0", kernel_release)) {
    return false;
  }

  // If the kernel older is than 2.6.32-674 (el6.9), it's buggy.
  return lt(kernel_release, "2.6.32-674");
}

int64_t LogBlockManager::LookupBlockLimit(int64_t fs_block_size) {
  const int64_t* limit = FindFloorOrNull(kPerFsBlockSizeBlockLimits,
                                         fs_block_size);
  if (limit) {
    return *limit;
  }

  // Block size must have been less than the very first key. Return the
  // first recorded entry and hope for the best.
  return kPerFsBlockSizeBlockLimits.begin()->second;
}

} // namespace fs
} // namespace kudu
