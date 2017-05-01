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
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "kudu/fs/block_manager_metrics.h"
#include "kudu/fs/block_manager_util.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/fs/fs_report.h"
#include "kudu/gutil/callback.h"
#include "kudu/gutil/integral_types.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/alignment.h"
#include "kudu/util/atomic.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/file_cache.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/locks.h"
#include "kudu/util/malloc.h"
#include "kudu/util/metrics.h"
#include "kudu/util/mutex.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util_prod.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/trace.h"

DECLARE_bool(enable_data_block_fsync);
DECLARE_bool(block_manager_lock_dirs);

// TODO(unknown): How should this be configured? Should provide some guidance.
DEFINE_uint64(log_container_max_size, 10LU * 1024 * 1024 * 1024,
              "Maximum size (soft) of a log container");
TAG_FLAG(log_container_max_size, advanced);

DEFINE_int64(log_container_max_blocks, -1,
             "Maximum number of blocks (soft) of a log container. Use 0 for "
             "no limit. Use -1 for no limit except in the case of a kernel "
             "bug with hole punching on ext4 (see KUDU-1508 for details).");
TAG_FLAG(log_container_max_blocks, advanced);

DEFINE_uint64(log_container_preallocate_bytes, 32LU * 1024 * 1024,
              "Number of bytes to preallocate in a log container when "
              "creating new blocks. Set to 0 to disable preallocation");
TAG_FLAG(log_container_preallocate_bytes, advanced);

DEFINE_double(log_container_excess_space_before_cleanup_fraction, 0.10,
              "Additional fraction of a log container's calculated size that "
              "must be consumed on disk before the container is considered to "
              "be inconsistent and subject to excess space cleanup at block "
              "manager startup.");
TAG_FLAG(log_container_excess_space_before_cleanup_fraction, advanced);

DEFINE_bool(log_block_manager_test_hole_punching, true,
            "Ensure hole punching is supported by the underlying filesystem");
TAG_FLAG(log_block_manager_test_hole_punching, advanced);
TAG_FLAG(log_block_manager_test_hole_punching, unsafe);

METRIC_DEFINE_gauge_uint64(server, log_block_manager_bytes_under_management,
                           "Bytes Under Management",
                           kudu::MetricUnit::kBytes,
                           "Number of bytes of data blocks currently under management");

METRIC_DEFINE_gauge_uint64(server, log_block_manager_blocks_under_management,
                           "Blocks Under Management",
                           kudu::MetricUnit::kBlocks,
                           "Number of data blocks currently under management");

METRIC_DEFINE_counter(server, log_block_manager_containers,
                      "Number of Block Containers",
                      kudu::MetricUnit::kLogBlockContainers,
                      "Number of log block containers");

METRIC_DEFINE_counter(server, log_block_manager_full_containers,
                      "Number of Full Block Counters",
                      kudu::MetricUnit::kLogBlockContainers,
                      "Number of full log block containers");

METRIC_DEFINE_counter(server, log_block_manager_unavailable_containers,
                      "Number of Unavailable Log Block Containers",
                      kudu::MetricUnit::kLogBlockContainers,
                      "Number of non-full log block containers that are under root paths "
                      "whose disks are full");

namespace kudu {

namespace fs {

using internal::LogBlock;
using internal::LogBlockContainer;
using pb_util::ReadablePBContainerFile;
using pb_util::WritablePBContainerFile;
using std::map;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::Substitute;

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

  scoped_refptr<AtomicGauge<uint64_t> > bytes_under_management;
  scoped_refptr<AtomicGauge<uint64_t> > blocks_under_management;

  scoped_refptr<Counter> containers;
  scoped_refptr<Counter> full_containers;
};

#define MINIT(x) x(METRIC_log_block_manager_##x.Instantiate(metric_entity))
#define GINIT(x) x(METRIC_log_block_manager_##x.Instantiate(metric_entity, 0))
LogBlockManagerMetrics::LogBlockManagerMetrics(const scoped_refptr<MetricEntity>& metric_entity)
  : generic_metrics(metric_entity),
    GINIT(bytes_under_management),
    GINIT(blocks_under_management),
    MINIT(containers),
    MINIT(full_containers) {
}
#undef GINIT
#undef MINIT

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
  LogBlock(LogBlockContainer* container, BlockId block_id, int64_t offset,
           int64_t length);
  ~LogBlock();

  const BlockId& block_id() const { return block_id_; }
  LogBlockContainer* container() const { return container_; }
  int64_t offset() const { return offset_; }
  int64_t length() const { return length_; }

  // Returns a block's length aligned to the nearest filesystem block size.
  int64_t fs_aligned_length() const;

  // Delete the block. Actual deletion takes place when the
  // block is destructed.
  void Delete();

 private:
  // The owning container. Must outlive the LogBlock.
  LogBlockContainer* container_;

  // The block identifier.
  const BlockId block_id_;

  // The block's offset in the container.
  const int64_t offset_;

  // The block's length.
  const int64_t length_;

  // Whether the block has been marked for deletion.
  bool deleted_;

  DISALLOW_COPY_AND_ASSIGN(LogBlock);
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
class LogBlockContainer {
 public:
  static const char* kMagic;

  // Creates a new block container in 'dir'.
  static Status Create(LogBlockManager* block_manager,
                       DataDir* dir,
                       unique_ptr<LogBlockContainer>* container);

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
                     DataDir* dir,
                     FsReport* report,
                     const std::string& id,
                     unique_ptr<LogBlockContainer>* container);

  // Indicates that the writing of 'block' is finished. If successful,
  // adds the block to the block manager's in-memory maps.
  //
  // Returns a status that is either the same as 's' (if !s.ok()) or
  // potentially different (if s.ok() and FinishBlock() failed).
  //
  // After returning, this container has been released to the block manager
  // and may no longer be used in the context of writing 'block'.
  Status FinishBlock(const Status& s, WritableBlock* block);

  // Frees the space associated with a block at 'offset' and 'length'. This
  // is a physical operation, not a logical one; a separate AppendMetadata()
  // is required to record the deletion in container metadata.
  //
  // The on-disk effects of this call are made durable only after SyncData().
  Status PunchHole(int64_t offset, int64_t length);

  // Preallocate enough space to ensure that an append of 'next_append_length'
  // can be satisfied by this container. The offset of the beginning of this
  // block must be provided in 'block_start_offset' (since container
  // bookkeeping is only updated when a block is finished).
  //
  // Does nothing if preallocation is disabled.
  Status EnsurePreallocated(int64_t block_start_offset,
                            size_t next_append_length);

  // Writes 'data' to this container's data file at offset 'offset'.
  //
  // The on-disk effects of this call are made durable only after SyncData().
  Status WriteData(int64_t offset, const Slice& data);

  // See RWFile::Read().
  Status ReadData(int64_t offset, size_t length,
                  Slice* result, uint8_t* scratch) const;

  // Appends 'pb' to this container's metadata file.
  //
  // The on-disk effects of this call are made durable only after SyncMetadata().
  Status AppendMetadata(const BlockRecordPB& pb);

  // Asynchronously flush this container's data file from 'offset' through
  // to 'length'.
  //
  // Does not guarantee data durability; use SyncData() for that.
  Status FlushData(int64_t offset, int64_t length);

  // Asynchronously flush this container's metadata file (all dirty bits).
  //
  // Does not guarantee metadata durability; use SyncMetadata() for that.
  //
  // TODO(unknown): Add support to just flush a range.
  Status FlushMetadata();

  // Synchronize this container's data file with the disk. On success,
  // guarantees that the data is made durable.
  //
  // TODO(unknown): Add support to synchronize just a range.
  Status SyncData();

  // Synchronize this container's metadata file with the disk. On success,
  // guarantees that the metadata is made durable.
  //
  // TODO(unknown): Add support to synchronize just a range.
  Status SyncMetadata();

  // Reopen the metadata file record writer. Should be called if the underlying
  // file was changed.
  Status ReopenMetadataWriter();

  // Truncates this container's data file to 'next_block_offset_' if it is
  // full. This effectively removes any preallocated but unused space.
  //
  // Should be called only when 'next_block_offset_' is up-to-date with
  // respect to the data on disk (i.e. after the container's records have
  // been loaded), otherwise data may be lost!
  //
  // This function is thread unsafe.
  Status TruncateDataToNextBlockOffset();

  // Reads the container's metadata from disk, sanity checking and processing
  // records along the way.
  //
  // Malformed records and other container inconsistencies are written to
  // 'report'. Healthy blocks are written either to 'live_blocks' or
  // 'dead_blocks'. The greatest block ID seen thus far in the container is
  // written to 'max_block_id'.
  //
  // Returns an error only if there was a problem accessing the container from
  // disk; such errors are fatal and effectively halt processing immediately.
  Status ProcessRecords(
      FsReport* report,
      LogBlockManager::UntrackedBlockMap* live_blocks,
      std::vector<scoped_refptr<internal::LogBlock>>* dead_blocks,
      uint64_t* max_block_id);

  // Updates internal bookkeeping state to reflect the creation of a block,
  // marking this container as full if needed. Should only be called when a
  // block is fully written, as it will round up the container data file's position.
  //
  // This function is thread unsafe.
  void BlockCreated(const scoped_refptr<LogBlock>& block);

  // Updates internal bookkeeping state to reflect the deletion of a block.
  //
  // Unlike BlockCreated(), this function is thread safe because block
  // deletions can happen concurrently with creations.
  //
  // Note: the container is not made "unfull"; containers remain sparse until deleted.
  void BlockDeleted(const scoped_refptr<LogBlock>& block);

  // Run a task on this container's data directory thread pool.
  //
  // Normally the task is performed asynchronously. However, if submission to
  // the pool fails, it runs synchronously on the current thread.
  void ExecClosure(const Closure& task);

  // Produces a debug-friendly string representation of this container.
  string ToString() const;

  // Simple accessors.
  LogBlockManager* block_manager() const { return block_manager_; }
  int64_t next_block_offset() const { return next_block_offset_; }
  int64_t total_bytes() const { return total_bytes_; }
  int64_t total_blocks() const { return total_blocks_; }
  int64_t live_bytes() const { return live_bytes_.Load(); }
  int64_t live_bytes_aligned() const { return live_bytes_aligned_.Load(); }
  int64_t live_blocks() const { return live_blocks_.Load(); }
  int64_t preallocated_window() const {
    return std::max<int64_t>(0, preallocated_offset_ - next_block_offset_);
  }
  bool full() const {
    return next_block_offset_ >= FLAGS_log_container_max_size ||
        (max_num_blocks_ && (total_blocks_ >= max_num_blocks_));
  }
  const LogBlockManagerMetrics* metrics() const { return metrics_; }
  const DataDir* data_dir() const { return data_dir_; }
  DataDir* mutable_data_dir() const { return data_dir_; }
  const PathInstanceMetadataPB* instance() const { return data_dir_->instance()->metadata(); }

 private:
  LogBlockContainer(LogBlockManager* block_manager, DataDir* data_dir,
                    unique_ptr<WritablePBContainerFile> metadata_file,
                    shared_ptr<RWFile> data_file);

  // Processes a single block record, performing sanity checks on it and adding
  // it either to 'live_blocks' or 'dead_blocks'.
  //
  // Returns an error only if there was a problem accessing the container from
  // disk; such errors are fatal and effectively halt processing immediately.
  //
  // On success, 'report' is updated with any inconsistencies found in the
  // record, 'data_file_size' may be updated with the latest size of the
  // container's data file, and 'max_block_id' reflects the largest block ID
  // seen thus far in the container.
  Status ProcessRecord(
      const BlockRecordPB& record,
      FsReport* report,
      LogBlockManager::UntrackedBlockMap* live_blocks,
      std::vector<scoped_refptr<internal::LogBlock>>* dead_blocks,
      uint64_t* data_file_size,
      uint64_t* max_block_id);

  // The owning block manager. Must outlive the container itself.
  LogBlockManager* const block_manager_;

  // The data directory where the container lives.
  DataDir* data_dir_;

  const boost::optional<int64_t> max_num_blocks_;

  // Offset up to which we have preallocated bytes.
  int64_t preallocated_offset_ = 0;

  // Opened file handles to the container's files.
  unique_ptr<WritablePBContainerFile> metadata_file_;
  shared_ptr<RWFile> data_file_;

  // The offset of the next block to be written to the container.
  int64_t next_block_offset_ = 0;

  // The amount of data (post block alignment) written thus far to the container.
  int64_t total_bytes_ = 0;

  // The number of blocks written thus far in the container.
  int64_t total_blocks_ = 0;

  // The amount of data present in not-yet-deleted blocks of the container.
  //
  // Declared atomic because it is mutated from BlockDeleted().
  AtomicInt<int64_t> live_bytes_;

  // The amount of data (post block alignment) present in not-yet-deleted
  // blocks of the container.
  //
  // Declared atomic because it is mutated from BlockDeleted().
  AtomicInt<int64_t> live_bytes_aligned_;

  // The number of not-yet-deleted blocks in the container.
  //
  // Declared atomic because it is mutated from BlockDeleted().
  AtomicInt<int64_t> live_blocks_;

  // The metrics. Not owned by the log container; it has the same lifespan
  // as the block manager.
  const LogBlockManagerMetrics* metrics_;

  DISALLOW_COPY_AND_ASSIGN(LogBlockContainer);
};

LogBlockContainer::LogBlockContainer(
    LogBlockManager* block_manager,
    DataDir* data_dir,
    unique_ptr<WritablePBContainerFile> metadata_file,
    shared_ptr<RWFile> data_file)
    : block_manager_(block_manager),
      data_dir_(data_dir),
      max_num_blocks_(FindOrDie(block_manager->block_limits_by_data_dir_,
                                data_dir)),
      metadata_file_(std::move(metadata_file)),
      data_file_(std::move(data_file)),
      live_bytes_(0),
      live_bytes_aligned_(0),
      live_blocks_(0),
      metrics_(block_manager->metrics()) {
}

Status LogBlockContainer::Create(LogBlockManager* block_manager,
                                 DataDir* dir,
                                 unique_ptr<LogBlockContainer>* container) {
  string common_path;
  string metadata_path;
  string data_path;
  Status metadata_status;
  Status data_status;
  unique_ptr<RWFile> metadata_writer;
  unique_ptr<RWFile> data_file;
  RWFileOptions wr_opts;
  wr_opts.mode = Env::CREATE_NON_EXISTING;

  // Repeat in the event of a container id collision (unlikely).
  //
  // When looping, we delete any created-and-orphaned files.
  do {
    if (metadata_writer) {
      block_manager->env()->DeleteFile(metadata_path);
    }
    common_path = JoinPathSegments(dir->dir(),
                                   block_manager->oid_generator()->Next());
    metadata_path = StrCat(common_path, LogBlockManager::kContainerMetadataFileSuffix);
    metadata_status = block_manager->env()->NewRWFile(wr_opts,
                                                      metadata_path,
                                                      &metadata_writer);
    if (data_file) {
      block_manager->env()->DeleteFile(data_path);
    }
    data_path = StrCat(common_path, LogBlockManager::kContainerDataFileSuffix);
    RWFileOptions rw_opts;
    rw_opts.mode = Env::CREATE_NON_EXISTING;
    data_status = block_manager->env()->NewRWFile(rw_opts,
                                                  data_path,
                                                  &data_file);
  } while (PREDICT_FALSE(metadata_status.IsAlreadyPresent() ||
                         data_status.IsAlreadyPresent()));
  if (metadata_status.ok() && data_status.ok()) {
    unique_ptr<WritablePBContainerFile> metadata_file;
    shared_ptr<RWFile> cached_data_file;

    if (block_manager->file_cache_) {
      metadata_writer.reset();
      shared_ptr<RWFile> cached_metadata_writer;
      RETURN_NOT_OK(block_manager->file_cache_->OpenExistingFile(
          metadata_path, &cached_metadata_writer));
      metadata_file.reset(new WritablePBContainerFile(
          std::move(cached_metadata_writer)));

      data_file.reset();
      RETURN_NOT_OK(block_manager->file_cache_->OpenExistingFile(
          data_path, &cached_data_file));
    } else {
      metadata_file.reset(new WritablePBContainerFile(
          std::move(metadata_writer)));
      cached_data_file = std::move(data_file);
    }
    RETURN_NOT_OK(metadata_file->Init(BlockRecordPB()));

    container->reset(new LogBlockContainer(block_manager,
                                           dir,
                                           std::move(metadata_file),
                                           std::move(cached_data_file)));
    VLOG(1) << "Created log block container " << (*container)->ToString();
  }

  // Prefer metadata status (arbitrarily).
  return !metadata_status.ok() ? metadata_status : data_status;
}

Status LogBlockContainer::Open(LogBlockManager* block_manager,
                               DataDir* dir,
                               FsReport* report,
                               const string& id,
                               unique_ptr<LogBlockContainer>* container) {
  Env* env = block_manager->env();
  string common_path = JoinPathSegments(dir->dir(), id);
  string metadata_path = StrCat(common_path, LogBlockManager::kContainerMetadataFileSuffix);
  string data_path = StrCat(common_path, LogBlockManager::kContainerDataFileSuffix);

  // Check that both the metadata and data files exist and have valid lengths.
  // This covers a commonly seen case at startup, where the previous incarnation
  // of the server crashed due to "too many open files" just as it was trying
  // to create a data file. This orphans an empty metadata file, which we can
  // safely delete.
  {
    uint64_t metadata_size = 0;
    uint64_t data_size = 0;
    Status s = env->GetFileSize(metadata_path, &metadata_size);
    if (!s.IsNotFound()) {
      RETURN_NOT_OK_PREPEND(s, "unable to determine metadata file size");
    }
    s = env->GetFileSize(data_path, &data_size);
    if (!s.IsNotFound()) {
      RETURN_NOT_OK_PREPEND(s, "unable to determine data file size");
    }

    if (metadata_size < pb_util::kPBContainerMinimumValidLength &&
        data_size == 0) {
      report->incomplete_container_check->entries.emplace_back(common_path);
      return Status::Aborted("orphaned empty metadata and data files $0");
    }
  }

  // Open the existing metadata and data files for writing.
  unique_ptr<WritablePBContainerFile> metadata_pb_writer;
  shared_ptr<RWFile> data_file;

  if (block_manager->file_cache_) {
    shared_ptr<RWFile> metadata_writer;
    RETURN_NOT_OK(block_manager->file_cache_->OpenExistingFile(
        metadata_path, &metadata_writer));
    metadata_pb_writer.reset(new WritablePBContainerFile(
        std::move(metadata_writer)));

    RETURN_NOT_OK(block_manager->file_cache_->OpenExistingFile(
        data_path, &data_file));
  } else {
    RWFileOptions wr_opts;
    wr_opts.mode = Env::OPEN_EXISTING;

    unique_ptr<RWFile> metadata_writer;
    RETURN_NOT_OK(block_manager->env_->NewRWFile(wr_opts,
                                                 metadata_path,
                                                 &metadata_writer));
    metadata_pb_writer.reset(new WritablePBContainerFile(
        std::move(metadata_writer)));

    unique_ptr<RWFile> uw;
    RETURN_NOT_OK(block_manager->env_->NewRWFile(wr_opts,
                                                 data_path,
                                                 &uw));
    data_file = std::move(uw);
  }
  RETURN_NOT_OK(metadata_pb_writer->Reopen());

  uint64_t data_file_size;
  RETURN_NOT_OK(data_file->Size(&data_file_size));

  // Create the in-memory container and populate it.
  unique_ptr<LogBlockContainer> open_container(new LogBlockContainer(block_manager,
                                                                     dir,
                                                                     std::move(metadata_pb_writer),
                                                                     std::move(data_file)));
  open_container->preallocated_offset_ = data_file_size;
  VLOG(1) << "Opened log block container " << open_container->ToString();
  container->swap(open_container);
  return Status::OK();
}

Status LogBlockContainer::TruncateDataToNextBlockOffset() {
  if (full()) {
    VLOG(2) << Substitute("Truncating container $0 to offset $1",
                          ToString(), next_block_offset_);
    RETURN_NOT_OK(data_file_->Truncate(next_block_offset_));
  }
  return Status::OK();
}

Status LogBlockContainer::ProcessRecords(
    FsReport* report,
    LogBlockManager::UntrackedBlockMap* live_blocks,
    vector<scoped_refptr<internal::LogBlock>>* dead_blocks,
    uint64_t* max_block_id) {
  string metadata_path = metadata_file_->filename();
  unique_ptr<RandomAccessFile> metadata_reader;
  RETURN_NOT_OK(block_manager()->env()->NewRandomAccessFile(metadata_path, &metadata_reader));
  ReadablePBContainerFile pb_reader(std::move(metadata_reader));
  RETURN_NOT_OK(pb_reader.Open());

  uint64_t data_file_size = 0;
  Status read_status;
  while (true) {
    BlockRecordPB record;
    read_status = pb_reader.ReadNextPB(&record);
    if (!read_status.ok()) {
      break;
    }
    RETURN_NOT_OK(ProcessRecord(record, report,
                                live_blocks, dead_blocks,
                                &data_file_size, max_block_id));
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
  return read_status;
}

Status LogBlockContainer::ProcessRecord(
    const BlockRecordPB& record,
    FsReport* report,
    LogBlockManager::UntrackedBlockMap* live_blocks,
    vector<scoped_refptr<internal::LogBlock>>* dead_blocks,
    uint64_t* data_file_size,
    uint64_t* max_block_id) {
  BlockId block_id(BlockId::FromPB(record.block_id()));
  scoped_refptr<LogBlock> lb;
  switch (record.op_type()) {
    case CREATE:
      // First verify that the record's offset/length aren't wildly incorrect.
      if (PREDICT_FALSE(!record.has_offset() ||
                        !record.has_length() ||
                        record.offset() < 0  ||
                        record.length() < 0)) {
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
      if (PREDICT_FALSE(record.offset() + record.length() > *data_file_size)) {
        RETURN_NOT_OK(data_file_->Size(data_file_size));
      }

      // If the record still extends beyond the end of the file, it is malformed.
      if (PREDICT_FALSE(record.offset() + record.length() > *data_file_size)) {
        // TODO(adar): treat as a different kind of inconsistency?
        report->malformed_record_check->entries.emplace_back(ToString(), record);
        break;
      }

      lb = new LogBlock(this, block_id, record.offset(), record.length());
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
                            record.offset(), record.length());

      // This block must be included in the container's logical size, even if
      // it has since been deleted. This helps satisfy one of our invariants:
      // once a container byte range has been used, it may never be reused in
      // the future.
      //
      // If we ignored deleted blocks, we would end up reusing the space
      // belonging to the last deleted block in the container.
      BlockCreated(lb);

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
      BlockDeleted(lb);
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

Status LogBlockContainer::FinishBlock(const Status& s, WritableBlock* block) {
  auto cleanup = MakeScopedCleanup([&]() {
    block_manager_->MakeContainerAvailable(this);
  });

  if (!s.ok()) {
    // Early return; 'cleanup' makes the container available again.
    return s;
  }

  // A failure when syncing the container means the container (and its new
  // blocks) may be missing the next time the on-disk state is reloaded.
  //
  // As such, it's not correct to add the block to in-memory state unless
  // synchronization succeeds. In the worst case, this means the data file
  // will have written some garbage that can be expunged during a GC.
  RETURN_NOT_OK(block_manager()->SyncContainer(*this));

  scoped_refptr<LogBlock> lb = block_manager()->AddLogBlock(
      this, block->id(), next_block_offset_, block->BytesAppended());
  CHECK(lb);
  BlockCreated(lb);

  // Truncate the container if it's now full; any left over preallocated space
  // is no longer needed.
  //
  // Note that this take places _after_ the container has been synced to disk.
  // That's OK; truncation isn't needed for correctness, and in the event of a
  // crash, it will be retried at startup.
  RETURN_NOT_OK(TruncateDataToNextBlockOffset());

  if (full() && block_manager()->metrics()) {
    block_manager()->metrics()->full_containers->Increment();
  }
  return Status::OK();
}

Status LogBlockContainer::PunchHole(int64_t offset, int64_t length) {
  DCHECK_GE(offset, 0);
  DCHECK_GE(length, 0);

  // It is invalid to punch a zero-size hole.
  if (length) {
    // It's OK if we exceed the file's total size; the kernel will truncate
    // our request.
    return data_file_->PunchHole(offset, length);
  }
  return Status::OK();
}

Status LogBlockContainer::WriteData(int64_t offset, const Slice& data) {
  DCHECK_GE(offset, next_block_offset_);

  RETURN_NOT_OK(data_file_->Write(offset, data));

  // This append may have changed the container size if:
  // 1. It was large enough that it blew out the preallocated space.
  // 2. Preallocation was disabled.
  if (offset + data.size() > preallocated_offset_) {
    RETURN_NOT_OK(data_dir_->RefreshIsFull(DataDir::RefreshMode::ALWAYS));
  }
  return Status::OK();
}

Status LogBlockContainer::ReadData(int64_t offset, size_t length,
                                   Slice* result, uint8_t* scratch) const {
  DCHECK_GE(offset, 0);

  return data_file_->Read(offset, length, result, scratch);
}

Status LogBlockContainer::AppendMetadata(const BlockRecordPB& pb) {
  // Note: We don't check for sufficient disk space for metadata writes in
  // order to allow for block deletion on full disks.
  return metadata_file_->Append(pb);
}

Status LogBlockContainer::FlushData(int64_t offset, int64_t length) {
  DCHECK_GE(offset, 0);
  DCHECK_GE(length, 0);
  return data_file_->Flush(RWFile::FLUSH_ASYNC, offset, length);
}

Status LogBlockContainer::FlushMetadata() {
  return metadata_file_->Flush();
}

Status LogBlockContainer::SyncData() {
  if (FLAGS_enable_data_block_fsync) {
    return data_file_->Sync();
  }
  return Status::OK();
}

Status LogBlockContainer::SyncMetadata() {
  if (FLAGS_enable_data_block_fsync) {
    return metadata_file_->Sync();
  }
  return Status::OK();
}

Status LogBlockContainer::ReopenMetadataWriter() {
  return metadata_file_->Reopen();
}

Status LogBlockContainer::EnsurePreallocated(int64_t block_start_offset,
                                             size_t next_append_length) {
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
    RETURN_NOT_OK(data_file_->PreAllocate(off, len, RWFile::CHANGE_FILE_SIZE));
    RETURN_NOT_OK(data_dir_->RefreshIsFull(DataDir::RefreshMode::ALWAYS));
    VLOG(2) << Substitute("Preallocated $0 bytes at offset $1 in container $2",
                          len, off, ToString());

    preallocated_offset_ = off + len;
  }

  return Status::OK();
}

void LogBlockContainer::BlockCreated(const scoped_refptr<LogBlock>& block) {
  DCHECK_GE(block->offset(), 0);

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
      block->offset() + block->length(),
      instance()->filesystem_block_size_bytes());
  if (PREDICT_FALSE(new_next_block_offset < next_block_offset_)) {
    LOG(WARNING) << Substitute(
        "Container $0 unexpectedly tried to lower its next block offset "
        "(from $1 to $2), ignoring",
        ToString(), next_block_offset_, new_next_block_offset);
  } else {
    int64_t aligned_block_length = new_next_block_offset - next_block_offset_;
    total_bytes_+= aligned_block_length;
    live_bytes_.IncrementBy(block->length());
    live_bytes_aligned_.IncrementBy(aligned_block_length);
    next_block_offset_ = new_next_block_offset;
  }
  total_blocks_++;
  live_blocks_.Increment();

  if (full()) {
    VLOG(1) << Substitute(
        "Container $0 with size $1 is now full, max size is $2",
        ToString(), next_block_offset_, FLAGS_log_container_max_size);
  }
}

void LogBlockContainer::BlockDeleted(const scoped_refptr<LogBlock>& block) {
  DCHECK_GE(block->offset(), 0);

  live_bytes_.IncrementBy(-block->length());
  live_bytes_aligned_.IncrementBy(-block->fs_aligned_length());
  live_blocks_.IncrementBy(-1);
}

void LogBlockContainer::ExecClosure(const Closure& task) {
  data_dir_->ExecClosure(task);
}

string LogBlockContainer::ToString() const {
  string s;
  CHECK(TryStripSuffixString(data_file_->filename(),
                             LogBlockManager::kContainerDataFileSuffix, &s));
  return s;
}

////////////////////////////////////////////////////////////
// LogBlock (definition)
////////////////////////////////////////////////////////////

LogBlock::LogBlock(LogBlockContainer* container, BlockId block_id,
                   int64_t offset, int64_t length)
    : container_(container),
      block_id_(block_id),
      offset_(offset),
      length_(length),
      deleted_(false) {
  DCHECK_GE(offset, 0);
  DCHECK_GE(length, 0);
}

static void DeleteBlockAsync(LogBlockContainer* container,
                             BlockId block_id,
                             int64_t offset,
                             int64_t fs_aligned_length) {
  // We don't call SyncData() to synchronize the deletion because it's
  // expensive, and in the worst case, we'll just leave orphaned data
  // behind to be cleaned up in the next GC.
  VLOG(3) << "Freeing space belonging to block " << block_id;
  WARN_NOT_OK(container->PunchHole(offset, fs_aligned_length),
              Substitute("Could not delete block $0", block_id.ToString()));
}

LogBlock::~LogBlock() {
  if (deleted_) {
    // Use the block's aligned length so that the filesystem can reclaim
    // maximal disk space.
    container_->ExecClosure(Bind(&DeleteBlockAsync, container_, block_id_,
                                 offset_, fs_aligned_length()));
  }
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

void LogBlock::Delete() {
  DCHECK(!deleted_);
  deleted_ = true;
}

////////////////////////////////////////////////////////////
// LogWritableBlock
////////////////////////////////////////////////////////////

// A log-backed block that has been opened for writing.
//
// There's no reference to a LogBlock as this block has yet to be
// persisted.
class LogWritableBlock : public WritableBlock {
 public:
  enum SyncMode {
    SYNC,
    NO_SYNC
  };

  LogWritableBlock(LogBlockContainer* container, BlockId block_id,
                   int64_t block_offset);

  virtual ~LogWritableBlock();

  virtual Status Close() OVERRIDE;

  virtual Status Abort() OVERRIDE;

  virtual const BlockId& id() const OVERRIDE;

  virtual BlockManager* block_manager() const OVERRIDE;

  virtual Status Append(const Slice& data) OVERRIDE;

  virtual Status FlushDataAsync() OVERRIDE;

  virtual size_t BytesAppended() const OVERRIDE;

  virtual State state() const OVERRIDE;

  // Actually close the block, possibly synchronizing its dirty data and
  // metadata to disk.
  Status DoClose(SyncMode mode);

  // Write this block's metadata to disk.
  //
  // Does not synchronize the written data; that takes place in Close().
  Status AppendMetadata();

 private:
  // The owning container. Must outlive the block.
  LogBlockContainer* container_;

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

LogWritableBlock::LogWritableBlock(LogBlockContainer* container,
                                   BlockId block_id, int64_t block_offset)
    : container_(container),
      block_id_(block_id),
      block_offset_(block_offset),
      block_length_(0),
      state_(CLEAN) {
  DCHECK_GE(block_offset, 0);
  DCHECK_EQ(0, block_offset % container->instance()->filesystem_block_size_bytes());
  if (container->metrics()) {
    container->metrics()->generic_metrics.blocks_open_writing->Increment();
    container->metrics()->generic_metrics.total_writable_blocks->Increment();
  }
}

LogWritableBlock::~LogWritableBlock() {
  if (state_ != CLOSED) {
    WARN_NOT_OK(Abort(), Substitute("Failed to abort block $0",
                                    id().ToString()));
  }
}

Status LogWritableBlock::Close() {
  return DoClose(SYNC);
}

Status LogWritableBlock::Abort() {
  RETURN_NOT_OK(DoClose(NO_SYNC));

  // DoClose() has unlocked the container; it may be locked by someone else.
  // But block_manager_ is immutable, so this is safe.
  return container_->block_manager()->DeleteBlock(id());
}

const BlockId& LogWritableBlock::id() const {
  return block_id_;
}

BlockManager* LogWritableBlock::block_manager() const {
  return container_->block_manager();
}

Status LogWritableBlock::Append(const Slice& data) {
  DCHECK(state_ == CLEAN || state_ == DIRTY)
      << "Invalid state: " << state_;

  // The metadata change is deferred to Close() or FlushDataAsync(),
  // whichever comes first. We can't do it now because the block's
  // length is still in flux.

  int64_t cur_block_offset = block_offset_ + block_length_;
  RETURN_NOT_OK(container_->EnsurePreallocated(cur_block_offset, data.size()));

  MicrosecondsInt64 start_time = GetMonoTimeMicros();
  RETURN_NOT_OK(container_->WriteData(cur_block_offset, data));
  MicrosecondsInt64 end_time = GetMonoTimeMicros();

  int64_t dur = end_time - start_time;
  TRACE_COUNTER_INCREMENT("lbm_write_time_us", dur);
  const char* counter = BUCKETED_COUNTER_NAME("lbm_writes", dur);
  TRACE_COUNTER_INCREMENT(counter, 1);

  block_length_ += data.size();
  state_ = DIRTY;
  return Status::OK();
}

Status LogWritableBlock::FlushDataAsync() {
  DCHECK(state_ == CLEAN || state_ == DIRTY || state_ == FLUSHING)
      << "Invalid state: " << state_;
  if (state_ == DIRTY) {
    VLOG(3) << "Flushing block " << id();
    RETURN_NOT_OK(container_->FlushData(block_offset_, block_length_));

    RETURN_NOT_OK_PREPEND(AppendMetadata(), "Unable to append block metadata");

    // TODO(unknown): Flush just the range we care about.
    RETURN_NOT_OK(container_->FlushMetadata());
  }

  state_ = FLUSHING;
  return Status::OK();
}

size_t LogWritableBlock::BytesAppended() const {
  return block_length_;
}


WritableBlock::State LogWritableBlock::state() const {
  return state_;
}

Status LogWritableBlock::DoClose(SyncMode mode) {
  if (state_ == CLOSED) {
    return Status::OK();
  }

  // Tracks the first failure (if any).
  //
  // It's important that any subsequent failures mutate 's' before
  // returning. Otherwise 'cleanup' won't properly provide the first
  // failure to LogBlockContainer::FinishBlock().
  //
  // Note also that when 'cleanup' goes out of scope it may mutate 's'.
  Status s;
  {
    auto cleanup = MakeScopedCleanup([&]() {
        if (container_->metrics()) {
          container_->metrics()->generic_metrics.blocks_open_writing->Decrement();
          container_->metrics()->generic_metrics.total_bytes_written->IncrementBy(
              BytesAppended());
        }

        state_ = CLOSED;
        s = container_->FinishBlock(s, this);
      });
    // FlushDataAsync() was not called; append the metadata now.
    if (state_ == CLEAN || state_ == DIRTY) {
      s = AppendMetadata();
      RETURN_NOT_OK_PREPEND(s, "Unable to flush block during close");
    }

    if (mode == SYNC &&
        (state_ == CLEAN || state_ == DIRTY || state_ == FLUSHING)) {
      VLOG(3) << "Syncing block " << id();

      // TODO(unknown): Sync just this block's dirty data.
      s = container_->SyncData();
      RETURN_NOT_OK(s);

      // TODO(unknown): Sync just this block's dirty metadata.
      s = container_->SyncMetadata();
      RETURN_NOT_OK(s);
    }
  }

  return s;
}

Status LogWritableBlock::AppendMetadata() {
  BlockRecordPB record;
  id().CopyToPB(record.mutable_block_id());
  record.set_op_type(CREATE);
  record.set_timestamp_us(GetCurrentTimeMicros());
  record.set_offset(block_offset_);
  record.set_length(block_length_);
  return container_->AppendMetadata(record);
}

////////////////////////////////////////////////////////////
// LogReadableBlock
////////////////////////////////////////////////////////////

// A log-backed block that has been opened for reading.
//
// Refers to a LogBlock representing the block's persisted metadata.
class LogReadableBlock : public ReadableBlock {
 public:
  LogReadableBlock(LogBlockContainer* container,
                   const scoped_refptr<LogBlock>& log_block);

  virtual ~LogReadableBlock();

  virtual Status Close() OVERRIDE;

  virtual const BlockId& id() const OVERRIDE;

  virtual Status Size(uint64_t* sz) const OVERRIDE;

  virtual Status Read(uint64_t offset, size_t length,
                      Slice* result, uint8_t* scratch) const OVERRIDE;

  virtual size_t memory_footprint() const OVERRIDE;

 private:
  // The owning container. Must outlive this block.
  LogBlockContainer* container_;

  // A reference to this block's metadata.
  scoped_refptr<internal::LogBlock> log_block_;

  // Whether or not this block has been closed. Close() is thread-safe, so
  // this must be an atomic primitive.
  AtomicBool closed_;

  DISALLOW_COPY_AND_ASSIGN(LogReadableBlock);
};

LogReadableBlock::LogReadableBlock(LogBlockContainer* container,
                                   const scoped_refptr<LogBlock>& log_block)
  : container_(container),
    log_block_(log_block),
    closed_(false) {
  if (container_->metrics()) {
    container_->metrics()->generic_metrics.blocks_open_reading->Increment();
    container_->metrics()->generic_metrics.total_readable_blocks->Increment();
  }
}

LogReadableBlock::~LogReadableBlock() {
  WARN_NOT_OK(Close(), Substitute("Failed to close block $0",
                                  id().ToString()));
}

Status LogReadableBlock::Close() {
  if (closed_.CompareAndSet(false, true)) {
    log_block_.reset();
    if (container_->metrics()) {
      container_->metrics()->generic_metrics.blocks_open_reading->Decrement();
    }
  }

  return Status::OK();
}

const BlockId& LogReadableBlock::id() const {
  return log_block_->block_id();
}

Status LogReadableBlock::Size(uint64_t* sz) const {
  DCHECK(!closed_.Load());

  *sz = log_block_->length();
  return Status::OK();
}

Status LogReadableBlock::Read(uint64_t offset, size_t length,
                              Slice* result, uint8_t* scratch) const {
  DCHECK(!closed_.Load());

  uint64_t read_offset = log_block_->offset() + offset;
  if (log_block_->length() < offset + length) {
    return Status::IOError("Out-of-bounds read",
                           Substitute("read of [$0-$1) in block [$2-$3)",
                                      read_offset,
                                      read_offset + length,
                                      log_block_->offset(),
                                      log_block_->offset() + log_block_->length()));
  }

  MicrosecondsInt64 start_time = GetMonoTimeMicros();
  RETURN_NOT_OK(container_->ReadData(read_offset, length, result, scratch));
  MicrosecondsInt64 end_time = GetMonoTimeMicros();

  int64_t dur = end_time - start_time;
  TRACE_COUNTER_INCREMENT("lbm_read_time_us", dur);

  const char* counter = BUCKETED_COUNTER_NAME("lbm_reads", dur);
  TRACE_COUNTER_INCREMENT(counter, 1);

  if (container_->metrics()) {
    container_->metrics()->generic_metrics.total_bytes_read->IncrementBy(length);
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

const char* LogBlockManager::kContainerMetadataFileSuffix = ".metadata";
const char* LogBlockManager::kContainerDataFileSuffix = ".data";

static const char* kBlockManagerType = "log";

// These values were arrived at via experimentation. See commit 4923a74 for
// more details.
const map<int64_t, int64_t> LogBlockManager::per_fs_block_size_block_limits({
  { 1024, 673 },
  { 2048, 1353 },
  { 4096, 2721 }});

LogBlockManager::LogBlockManager(Env* env, const BlockManagerOptions& opts)
  : mem_tracker_(MemTracker::CreateTracker(-1,
                                           "log_block_manager",
                                           opts.parent_mem_tracker)),
    dd_manager_(env, opts.metric_entity, kBlockManagerType, opts.root_paths),
    blocks_by_block_id_(10,
                        BlockMap::hasher(),
                        BlockMap::key_equal(),
                        BlockAllocator(mem_tracker_)),
    env_(DCHECK_NOTNULL(env)),
    read_only_(opts.read_only),
    buggy_el6_kernel_(IsBuggyEl6Kernel(env->GetKernelRelease())),
    next_block_id_(1) {
  blocks_by_block_id_.set_deleted_key(BlockId());

  int64_t file_cache_capacity = GetFileCacheCapacityForBlockManager(env_);
  if (file_cache_capacity != kint64max) {
    file_cache_.reset(new FileCache<RWFile>("lbm",
                                            env_,
                                            file_cache_capacity,
                                            opts.metric_entity));
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

  if (opts.metric_entity) {
    metrics_.reset(new internal::LogBlockManagerMetrics(opts.metric_entity));
  }
}

LogBlockManager::~LogBlockManager() {
  // Release all of the memory accounted by the blocks.
  int64_t mem = 0;
  for (const auto& entry : blocks_by_block_id_) {
    mem += kudu_malloc_usable_size(entry.second.get());
  }
  mem_tracker_->Release(mem);

  // A LogBlock's destructor depends on its container, so all LogBlocks must be
  // destroyed before their containers.
  blocks_by_block_id_.clear();

  // Containers may have outstanding tasks running on data directories; shut
  // them down before destroying the containers.
  dd_manager_.Shutdown();

  STLDeleteValues(&all_containers_by_name_);
}

Status LogBlockManager::Create() {
  CHECK(!read_only_);
  return dd_manager_.Create(FLAGS_enable_data_block_fsync ?
      DataDirManager::FLAG_CREATE_TEST_HOLE_PUNCH | DataDirManager::FLAG_CREATE_FSYNC :
      DataDirManager::FLAG_CREATE_TEST_HOLE_PUNCH);
}

Status LogBlockManager::Open(FsReport* report) {
  DataDirManager::LockMode mode;
  if (!FLAGS_block_manager_lock_dirs) {
    mode = DataDirManager::LockMode::NONE;
  } else if (read_only_) {
    mode = DataDirManager::LockMode::OPTIONAL;
  } else {
    mode = DataDirManager::LockMode::MANDATORY;
  }
  RETURN_NOT_OK(dd_manager_.Open(kuint32max, mode));

  if (file_cache_) {
    RETURN_NOT_OK(file_cache_->Init());
  }

  // Establish (and log) block limits for each data directory using kernel,
  // filesystem, and gflags information.
  for (const auto& dd : dd_manager_.data_dirs()) {
    boost::optional<int64_t> limit;

    if (FLAGS_log_container_max_blocks == -1) {
      // No limit, unless this is KUDU-1508.

      // The log block manager requires hole punching and, of the ext
      // filesystems, only ext4 supports it. Thus, if this is an ext
      // filesystem, it's ext4 by definition.
      bool is_on_ext4;
      RETURN_NOT_OK(env_->IsOnExtFilesystem(dd->dir(), &is_on_ext4));
      if (buggy_el6_kernel_ && is_on_ext4) {
        uint64_t fs_block_size =
            dd->instance()->metadata()->filesystem_block_size_bytes();
        bool untested_block_size =
            !ContainsKey(per_fs_block_size_block_limits, fs_block_size);
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

  vector<FsReport> reports(dd_manager_.data_dirs().size());
  vector<Status> statuses(dd_manager_.data_dirs().size());
  int i = 0;
  for (const auto& dd : dd_manager_.data_dirs()) {
    // Open the data dir asynchronously.
    dd->ExecClosure(
        Bind(&LogBlockManager::OpenDataDir,
             Unretained(this),
             dd.get(),
             &reports[i],
             &statuses[i]));
    i++;
  }

  // Wait for the opens to complete.
  for (const auto& dd : dd_manager_.data_dirs()) {
    dd->WaitOnClosures();
  }

  // Ensure that no open failed.
  for (const auto& s : statuses) {
    if (!s.ok()) {
      return s;
    }
  }

  // Merge the reports and either return or log the result.
  FsReport merged_report;
  for (const auto& r : reports) {
    merged_report.MergeFrom(r);
  }
  if (report) {
    *report = std::move(merged_report);
  } else {
    RETURN_NOT_OK(merged_report.LogAndCheckForFatalErrors());
  }

  return Status::OK();
}

Status LogBlockManager::CreateBlock(const CreateBlockOptions& opts,
                                    unique_ptr<WritableBlock>* block) {
  CHECK(!read_only_);

  // Find a free container. If one cannot be found, create a new one.
  //
  // TODO(unknown): should we cap the number of outstanding containers and
  // force callers to block if we've reached it?
  LogBlockContainer* container;
  RETURN_NOT_OK(GetOrCreateContainer(&container));

  // Generate a free block ID.
  // We have to loop here because earlier versions used non-sequential block IDs,
  // and thus we may have to "skip over" some block IDs that are claimed.
  BlockId new_block_id;
  do {
    new_block_id.SetId(next_block_id_.Increment());
  } while (!TryUseBlockId(new_block_id));

  block->reset(new internal::LogWritableBlock(container,
                                              new_block_id,
                                              container->next_block_offset()));
  VLOG(3) << "Created block " << (*block)->id() << " in container "
          << container->ToString();
  return Status::OK();
}

Status LogBlockManager::CreateBlock(unique_ptr<WritableBlock>* block) {
  return CreateBlock(CreateBlockOptions(), block);
}

Status LogBlockManager::OpenBlock(const BlockId& block_id,
                                  unique_ptr<ReadableBlock>* block) {
  scoped_refptr<LogBlock> lb;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    lb = FindPtrOrNull(blocks_by_block_id_, block_id);
  }
  if (!lb) {
    return Status::NotFound("Can't find block", block_id.ToString());
  }

  block->reset(new internal::LogReadableBlock(lb->container(),
                                              lb.get()));
  VLOG(3) << "Opened block " << (*block)->id()
          << " from container " << lb->container()->ToString();
  return Status::OK();
}

Status LogBlockManager::DeleteBlock(const BlockId& block_id) {
  CHECK(!read_only_);

  scoped_refptr<LogBlock> lb(RemoveLogBlock(block_id));
  if (!lb) {
    return Status::NotFound("Can't find block", block_id.ToString());
  }
  VLOG(3) << "Deleting block " << block_id;
  lb->Delete();
  lb->container()->BlockDeleted(lb);

  // Record the on-disk deletion.
  //
  // TODO(unknown): what if this fails? Should we restore the in-memory block?
  BlockRecordPB record;
  block_id.CopyToPB(record.mutable_block_id());
  record.set_op_type(DELETE);
  record.set_timestamp_us(GetCurrentTimeMicros());
  RETURN_NOT_OK_PREPEND(lb->container()->AppendMetadata(record),
                        "Unable to append deletion record to block metadata");

  // We don't bother fsyncing the metadata append for deletes in order to avoid
  // the disk overhead. Even if we did fsync it, we'd still need to account for
  // garbage at startup time (in the event that we crashed just before the
  // fsync).
  //
  // TODO(KUDU-829): Implement GC of orphaned blocks.

  return Status::OK();
}

Status LogBlockManager::CloseBlocks(const std::vector<WritableBlock*>& blocks) {
  VLOG(3) << "Closing " << blocks.size() << " blocks";
  if (FLAGS_block_coalesce_close) {
    // Ask the kernel to begin writing out each block's dirty data. This is
    // done up-front to give the kernel opportunities to coalesce contiguous
    // dirty pages.
    for (WritableBlock* block : blocks) {
      RETURN_NOT_OK(block->FlushDataAsync());
    }
  }

  // Now close each block, waiting for each to become durable.
  for (WritableBlock* block : blocks) {
    RETURN_NOT_OK(block->Close());
  }
  return Status::OK();
}

Status LogBlockManager::GetAllBlockIds(vector<BlockId>* block_ids) {
  std::lock_guard<simple_spinlock> l(lock_);
  block_ids->assign(open_block_ids_.begin(), open_block_ids_.end());
  AppendKeysFromMap(blocks_by_block_id_, block_ids);
  return Status::OK();
}

void LogBlockManager::AddNewContainerUnlocked(LogBlockContainer* container) {
  DCHECK(lock_.is_locked());
  InsertOrDie(&all_containers_by_name_, container->ToString(), container);
  if (metrics()) {
    metrics()->containers->Increment();
    if (container->full()) {
      metrics()->full_containers->Increment();
    }
  }
}

Status LogBlockManager::GetOrCreateContainer(LogBlockContainer** container) {
  DataDir* dir;
  RETURN_NOT_OK(dd_manager_.GetNextDataDir(&dir));

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
  unique_ptr<LogBlockContainer> new_container;
  RETURN_NOT_OK_PREPEND(LogBlockContainer::Create(this,
                                                  dir,
                                                  &new_container),
                        "Could not create new log block container at " + dir->dir());
  {
    std::lock_guard<simple_spinlock> l(lock_);
    dirty_dirs_.insert(dir->dir());
    AddNewContainerUnlocked(new_container.get());
  }
  *container = new_container.release();
  return Status::OK();
}

void LogBlockManager::MakeContainerAvailable(LogBlockContainer* container) {
  std::lock_guard<simple_spinlock> l(lock_);
  MakeContainerAvailableUnlocked(container);
}

void LogBlockManager::MakeContainerAvailableUnlocked(LogBlockContainer* container) {
  DCHECK(lock_.is_locked());
  if (container->full()) {
    return;
  }
  available_containers_by_data_dir_[container->data_dir()].push_back(container);
}

Status LogBlockManager::SyncContainer(const LogBlockContainer& container) {
  Status s;
  bool to_sync = false;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    to_sync = dirty_dirs_.erase(container.data_dir()->dir());
  }

  if (to_sync && FLAGS_enable_data_block_fsync) {
    s = env_->SyncDir(container.data_dir()->dir());

    // If SyncDir fails, the container directory must be restored to
    // dirty_dirs_. Otherwise a future successful LogWritableBlock::Close()
    // on this container won't call SyncDir again, and the container might
    // be lost on crash.
    //
    // In the worst case (another block synced this container as we did),
    // we'll sync it again needlessly.
    if (!s.ok()) {
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

  std::lock_guard<simple_spinlock> l(lock_);
  if (ContainsKey(blocks_by_block_id_, block_id)) {
    return false;
  }
  return InsertIfNotPresent(&open_block_ids_, block_id);
}

scoped_refptr<LogBlock> LogBlockManager::AddLogBlock(
    LogBlockContainer* container,
    const BlockId& block_id,
    int64_t offset,
    int64_t length) {
  std::lock_guard<simple_spinlock> l(lock_);
  scoped_refptr<LogBlock> lb(new LogBlock(container, block_id, offset, length));
  mem_tracker_->Consume(kudu_malloc_usable_size(lb.get()));

  if (AddLogBlockUnlocked(lb)) {
    return lb;
  }
  return nullptr;
}

bool LogBlockManager::AddLogBlockUnlocked(const scoped_refptr<LogBlock>& lb) {
  DCHECK(lock_.is_locked());

  if (!InsertIfNotPresent(&blocks_by_block_id_, lb->block_id(), lb)) {
    return false;
  }

  VLOG(2) << Substitute("Added block: offset $0, length $1",
                        lb->offset(), lb->length());

  // There may already be an entry in open_block_ids_ (e.g. we just finished
  // writing out a block).
  open_block_ids_.erase(lb->block_id());
  if (metrics()) {
    metrics()->blocks_under_management->Increment();
    metrics()->bytes_under_management->IncrementBy(lb->length());
  }
  return true;
}

scoped_refptr<LogBlock> LogBlockManager::RemoveLogBlock(const BlockId& block_id) {
  std::lock_guard<simple_spinlock> l(lock_);
  scoped_refptr<LogBlock> result =
      EraseKeyReturnValuePtr(&blocks_by_block_id_, block_id);
  if (result) {
    VLOG(2) << Substitute("Removed block: offset $0, length $1",
                          result->offset(), result->length());

    mem_tracker_->Release(kudu_malloc_usable_size(result.get()));

    if (metrics()) {
      metrics()->blocks_under_management->Decrement();
      metrics()->bytes_under_management->DecrementBy(result->length());
    }
  }

  return result;
}

void LogBlockManager::OpenDataDir(DataDir* dir,
                                  FsReport* report,
                                  Status* result_status) {
  FsReport local_report;
  local_report.data_dirs.push_back(dir->dir());

  // We are going to perform these checks.
  //
  // Note: this isn't necessarily the complete set of FsReport checks; there
  // may be checks that the LBM cannot perform.
  local_report.full_container_space_check.emplace();
  local_report.incomplete_container_check.emplace();
  local_report.malformed_record_check.emplace();
  local_report.misaligned_block_check.emplace();
  local_report.partial_record_check.emplace();

  // Keep track of deleted blocks that we may need to repunch.
  vector<scoped_refptr<internal::LogBlock>> need_repunching;

  // Find all containers and open them.
  unordered_set<string> containers_seen;
  vector<string> children;
  Status s = env_->GetChildren(dir->dir(), &children);
  if (!s.ok()) {
    *result_status = s.CloneAndPrepend(Substitute(
        "Could not list children of $0", dir->dir()));
    return;
  }
  for (const string& child : children) {
    string container_name;
    if (!TryStripSuffixString(
            child, LogBlockManager::kContainerDataFileSuffix, &container_name) &&
        !TryStripSuffixString(
            child, LogBlockManager::kContainerMetadataFileSuffix, &container_name)) {
      continue;
    }
    if (!InsertIfNotPresent(&containers_seen, container_name)) {
      continue;
    }

    unique_ptr<LogBlockContainer> container;
    s = LogBlockContainer::Open(
        this, dir, &local_report, container_name, &container);
    if (s.IsAborted()) {
      // Skip the container. Open() added a record of it to 'local_report' for us.
      continue;
    }
    if (!s.ok()) {
      *result_status = s.CloneAndPrepend(Substitute(
          "Could not open container $0", container_name));
      return;
    }

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
    vector<scoped_refptr<internal::LogBlock>> dead_blocks;
    uint64_t max_block_id = 0;
    s = container->ProcessRecords(&local_report,
                                  &live_blocks,
                                  &dead_blocks,
                                  &max_block_id);
    if (!s.ok()) {
      *result_status = s.CloneAndPrepend(Substitute(
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
        local_report.misaligned_block_check->entries.emplace_back(
            container->ToString(), e.first);

      }
    }

    // Having processed the block records, let's check whether any full
    // containers have any extra space (left behind after a crash or from an
    // older version of Kudu).
    if (container->full()) {
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
        *result_status = s.CloneAndPrepend(Substitute(
            "Could not get on-disk file size of container $0", container->ToString()));
        return;
      }
      int64_t cleanup_threshold_size = container->live_bytes_aligned() *
          (1 + FLAGS_log_container_excess_space_before_cleanup_fraction);
      if (reported_size > cleanup_threshold_size) {
        local_report.full_container_space_check->entries.emplace_back(
            container->ToString(), reported_size - container->live_bytes_aligned());
        need_repunching.insert(need_repunching.end(),
                               dead_blocks.begin(), dead_blocks.end());
      }

      local_report.stats.lbm_full_container_count++;
    }
    local_report.stats.live_block_bytes += container->live_bytes();
    local_report.stats.live_block_bytes_aligned += container->live_bytes_aligned();
    local_report.stats.live_block_count += container->live_blocks();
    local_report.stats.lbm_container_count++;

    next_block_id_.StoreMax(max_block_id + 1);

    // Under the lock, merge this map into the main block map and add
    // the container.
    {
      std::lock_guard<simple_spinlock> l(lock_);
      // To avoid cacheline contention during startup, we aggregate all of the
      // memory in a local and add it to the mem-tracker in a single increment
      // at the end of this loop.
      int64_t mem_usage = 0;
      for (const UntrackedBlockMap::value_type& e : live_blocks) {
        if (!AddLogBlockUnlocked(e.second)) {
          // TODO(adar): track as an inconsistency?
          LOG(FATAL) << "Found duplicate CREATE record for block " << e.first
                     << " which already is alive from another container when "
                     << " processing container " << container->ToString();
        }
        mem_usage += kudu_malloc_usable_size(e.second.get());
      }

      mem_tracker_->Consume(mem_usage);
      AddNewContainerUnlocked(container.get());
      MakeContainerAvailableUnlocked(container.release());
    }
  }

  // Like the rest of Open(), repairs are performed per data directory to take
  // advantage of parallelism.
  s = Repair(&local_report, std::move(need_repunching));
  if (!s.ok()) {
    *result_status = s.CloneAndPrepend(Substitute(
        "fatal error while repairing inconsistencies in data directory $0",
        dir->dir()));
    return;
  }

  *report = std::move(local_report);
  *result_status = Status::OK();
}

Status LogBlockManager::Repair(
    FsReport* report,
    vector<scoped_refptr<internal::LogBlock>> need_repunching) {
  if (read_only_) {
    LOG(INFO) << "Read-only block manager, skipping repair";
    return Status::OK();
  }
  if (report->HasFatalErrors()) {
    LOG(WARNING) << "Found fatal and irreparable errors, skipping repair";
    return Status::OK();
  }

  // From here on out we're committed to repairing.

  // Fetch all the containers we're going to need.
  unordered_map<std::string, internal::LogBlockContainer*> containers_by_name;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    if (report->partial_record_check) {
      for (const auto& pr : report->partial_record_check->entries) {
        containers_by_name[pr.container] =
            FindOrDie(all_containers_by_name_, pr.container);
      }
    }
    if (report->full_container_space_check) {
      for (const auto& fcp : report->full_container_space_check->entries) {
        containers_by_name[fcp.container] =
            FindOrDie(all_containers_by_name_, fcp.container);
      }
    }
  }

  // Truncate partial metadata records.
  //
  // This is a fatal inconsistency; if the repair fails, we cannot proceed.
  if (report->partial_record_check) {
    for (auto& pr : report->partial_record_check->entries) {
      unique_ptr<RWFile> file;
      RWFileOptions opts;
      opts.mode = Env::OPEN_EXISTING;
      internal::LogBlockContainer* container = FindOrDie(containers_by_name,
                                                         pr.container);
      RETURN_NOT_OK_PREPEND(
          env_->NewRWFile(opts,
                          StrCat(pr.container, kContainerMetadataFileSuffix),
                          &file),
          "could not reopen container to truncate partial metadata record");

      RETURN_NOT_OK_PREPEND(file->Truncate(pr.offset),
                            "could not truncate partial metadata record");

      // Technically we've "repaired" the inconsistency if the truncation
      // succeeded, even if the following logic fails.
      pr.repaired = true;

      RETURN_NOT_OK_PREPEND(
          file->Close(),
          "could not close container after truncating partial metadata record");

      // Reopen the PB writer so that it will refresh its metadata about the
      // underlying file and resume appending to the new end of the file.
      RETURN_NOT_OK_PREPEND(
          container->ReopenMetadataWriter(),
          "could not reopen container metadata file");
    }
  }

  // Delete any incomplete container files.
  //
  // This is a non-fatal inconsistency; we can just as easily ignore the
  // leftover container files.
  if (report->incomplete_container_check) {
    for (auto& ic : report->incomplete_container_check->entries) {
      Status s = env_->DeleteFile(
          StrCat(ic.container, kContainerMetadataFileSuffix));
      if (!s.ok() && !s.IsNotFound()) {
        WARN_NOT_OK(s, "could not delete incomplete container metadata file");
      }

      s = env_->DeleteFile(StrCat(ic.container, kContainerDataFileSuffix));
      if (!s.ok() && !s.IsNotFound()) {
        WARN_NOT_OK(s, "could not delete incomplete container data file");
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
      internal::LogBlockContainer* container = FindOrDie(containers_by_name,
                                                         fcp.container);
      Status s = container->TruncateDataToNextBlockOffset();
      if (s.ok()) {
        fcp.repaired = true;
      }
      WARN_NOT_OK(s, "could not truncate excess preallocated space");
    }
  }

  // Repunch all requested holes. Any excess space reclaimed was already
  // tracked by LBMFullContainerSpaceCheck.
  //
  // TODO(adar): can be more efficient (less fs work and more space reclamation
  // in case of misaligned blocks) via hole coalescing first, but this is easy.
  for (const auto& b : need_repunching) {
    b->Delete();
  }

  // Clearing this vector drops the last references to the LogBlocks within,
  // triggering the repunching operations.
  need_repunching.clear();

  return Status::OK();
}

std::string LogBlockManager::ContainerPathForTests(internal::LogBlockContainer* container) {
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
  const int64_t* limit = FindFloorOrNull(per_fs_block_size_block_limits,
                                         fs_block_size);
  if (limit) {
    return *limit;
  }

  // Block size must have been less than the very first key. Return the
  // first recorded entry and hope for the best.
  return per_fs_block_size_block_limits.begin()->second;
}

} // namespace fs
} // namespace kudu
