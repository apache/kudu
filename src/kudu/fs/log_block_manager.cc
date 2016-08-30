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
#include <mutex>

#include "kudu/fs/block_manager_metrics.h"
#include "kudu/fs/block_manager_util.h"
#include "kudu/gutil/callback.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/alignment.h"
#include "kudu/util/atomic.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
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

// TODO: How should this be configured? Should provide some guidance.
DEFINE_uint64(log_container_max_size, 10LU * 1024 * 1024 * 1024,
              "Maximum size (soft) of a log container");
TAG_FLAG(log_container_max_size, advanced);

DEFINE_uint64(log_container_preallocate_bytes, 32LU * 1024 * 1024,
              "Number of bytes to preallocate in a log container when "
              "creating new blocks. Set to 0 to disable preallocation");
TAG_FLAG(log_container_preallocate_bytes, advanced);

DEFINE_bool(log_block_manager_test_hole_punching, true,
            "Ensure hole punching is supported by the underlying filesystem");
TAG_FLAG(log_block_manager_test_hole_punching, advanced);
TAG_FLAG(log_block_manager_test_hole_punching, unsafe);

DEFINE_int32(log_block_manager_full_disk_cache_seconds, 30,
             "Number of seconds we cache the full-disk status in the block manager. "
             "During this time, writes to the corresponding root path will not be attempted.");
TAG_FLAG(log_block_manager_full_disk_cache_seconds, advanced);
TAG_FLAG(log_block_manager_full_disk_cache_seconds, evolving);

DEFINE_int64(fs_data_dirs_reserved_bytes, 0,
             "Number of bytes to reserve on each data directory filesystem for non-Kudu usage. "
             "Only works with the log block manager and when --log_container_preallocate_bytes "
             "is non-zero.");
TAG_FLAG(fs_data_dirs_reserved_bytes, runtime);
TAG_FLAG(fs_data_dirs_reserved_bytes, evolving);

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

using kudu::env_util::ScopedFileDeleter;
using kudu::fs::internal::LogBlock;
using kudu::fs::internal::LogBlockContainer;
using kudu::pb_util::ReadablePBContainerFile;
using kudu::pb_util::WritablePBContainerFile;
using std::unordered_map;
using std::unordered_set;
using strings::Substitute;

namespace kudu {

namespace fs {
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
  scoped_refptr<Counter> unavailable_containers;
};

#define MINIT(x) x(METRIC_log_block_manager_##x.Instantiate(metric_entity))
#define GINIT(x) x(METRIC_log_block_manager_##x.Instantiate(metric_entity, 0))
LogBlockManagerMetrics::LogBlockManagerMetrics(const scoped_refptr<MetricEntity>& metric_entity)
  : generic_metrics(metric_entity),
    GINIT(bytes_under_management),
    GINIT(blocks_under_management),
    MINIT(containers),
    MINIT(full_containers),
    MINIT(unavailable_containers) {
}
#undef GINIT
#undef MINIT

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
                       PathInstanceMetadataPB* instance,
                       const std::string& dir,
                       gscoped_ptr<LogBlockContainer>* container);

  // Opens an existing block container in 'dir'.
  //
  // Every container is comprised of two files: "<dir>/<id>.data" and
  // "<dir>/<id>.metadata". Together, 'dir' and 'id' fully describe both files.
  static Status Open(LogBlockManager* block_manager,
                     PathInstanceMetadataPB* instance,
                     const std::string& dir,
                     const std::string& id,
                     gscoped_ptr<LogBlockContainer>* container);

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
  Status DeleteBlock(int64_t offset, int64_t length);

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
  // TODO: Add support to just flush a range.
  Status FlushMetadata();

  // Synchronize this container's data file with the disk. On success,
  // guarantees that the data is made durable.
  //
  // TODO: Add support to synchronize just a range.
  Status SyncData();

  // Synchronize this container's metadata file with the disk. On success,
  // guarantees that the metadata is made durable.
  //
  // TODO: Add support to synchronize just a range.
  Status SyncMetadata();

  // Ensure that 'length' bytes are preallocated in this container,
  // beginning from the position where the last written block ended.
  Status Preallocate(size_t length);

  // Returns the path to the metadata file.
  string MetadataFilePath() const;

  // Returns the path to the data file.
  string DataFilePath() const;

  // Reads the container's metadata from disk, sanity checking and
  // returning the records.
  Status ReadContainerRecords(deque<BlockRecordPB>* records) const;

  // Updates 'total_bytes_written_', marking this container as full if
  // needed. Should only be called when a block is fully written, as it
  // will round up the container data file's position.
  //
  // This function is thread unsafe.
  void UpdateBytesWritten(int64_t more_bytes);

  // Run a task on this container's root path thread pool.
  //
  // Normally the task is performed asynchronously. However, if submission to
  // the pool fails, it runs synchronously on the current thread.
  void ExecClosure(const Closure& task);

  // Simple accessors.
  std::string dir() const { return DirName(path_); }
  const std::string& ToString() const { return path_; }
  LogBlockManager* block_manager() const { return block_manager_; }
  int64_t total_bytes_written() const { return total_bytes_written_; }
  bool full() const {
    return total_bytes_written_ >= FLAGS_log_container_max_size;
  }
  const LogBlockManagerMetrics* metrics() const { return metrics_; }
  const PathInstanceMetadataPB* instance() const { return instance_; }
  const std::string& root_path() const { return root_path_; }

 private:
  LogBlockContainer(LogBlockManager* block_manager, PathInstanceMetadataPB* instance,
                    std::string root_path, std::string path,
                    gscoped_ptr<WritablePBContainerFile> metadata_writer,
                    gscoped_ptr<RWFile> data_file);

  // Performs sanity checks on a block record.
  void CheckBlockRecord(const BlockRecordPB& record,
                        uint64_t data_file_size) const;

  // The owning block manager. Must outlive the container itself.
  LogBlockManager* const block_manager_;

  // The path to the container's root path. This is the root directory under
  // which the container lives.
  const std::string root_path_;

  // The path to the container's files. Equivalent to "<dir>/<id>" (see the
  // container constructor).
  const std::string path_;

  // Offset up to which we have preallocated bytes.
  int64_t preallocated_offset_ = 0;

  // Opened file handles to the container's files.
  //
  // RWFile is not thread safe so access to each writer must be
  // serialized through a (sleeping) mutex. We use different mutexes to
  // avoid contention in cases where only one writer is needed.
  gscoped_ptr<WritablePBContainerFile> metadata_pb_writer_;
  Mutex metadata_pb_writer_lock_;
  Mutex data_writer_lock_;
  gscoped_ptr<RWFile> data_file_;

  // The amount of data written thus far in the container.
  int64_t total_bytes_written_ = 0;

  // The metrics. Not owned by the log container; it has the same lifespan
  // as the block manager.
  const LogBlockManagerMetrics* metrics_;

  const PathInstanceMetadataPB* instance_;

  DISALLOW_COPY_AND_ASSIGN(LogBlockContainer);
};

LogBlockContainer::LogBlockContainer(
    LogBlockManager* block_manager, PathInstanceMetadataPB* instance,
    string root_path, string path, gscoped_ptr<WritablePBContainerFile> metadata_writer,
    gscoped_ptr<RWFile> data_file)
    : block_manager_(block_manager),
      root_path_(std::move(root_path)),
      path_(std::move(path)),
      metadata_pb_writer_(std::move(metadata_writer)),
      data_file_(std::move(data_file)),
      metrics_(block_manager->metrics()),
      instance_(instance) {}

Status LogBlockContainer::Create(LogBlockManager* block_manager,
                                 PathInstanceMetadataPB* instance,
                                 const string& root_path,
                                 gscoped_ptr<LogBlockContainer>* container) {
  string common_path;
  string metadata_path;
  string data_path;
  Status metadata_status;
  Status data_status;
  gscoped_ptr<RWFile> metadata_writer;
  gscoped_ptr<RWFile> data_file;
  RWFileOptions wr_opts;
  wr_opts.mode = Env::CREATE_NON_EXISTING;

  // Repeat in the event of a container id collision (unlikely).
  //
  // When looping, we delete any created-and-orphaned files.
  do {
    if (metadata_writer) {
      block_manager->env()->DeleteFile(metadata_path);
    }
    common_path = JoinPathSegments(root_path, block_manager->oid_generator()->Next());
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
    gscoped_ptr<WritablePBContainerFile> metadata_pb_writer(
        new WritablePBContainerFile(std::move(metadata_writer)));
    RETURN_NOT_OK(metadata_pb_writer->Init(BlockRecordPB()));
    container->reset(new LogBlockContainer(block_manager,
                                           instance,
                                           root_path,
                                           common_path,
                                           std::move(metadata_pb_writer),
                                           std::move(data_file)));
    VLOG(1) << "Created log block container " << (*container)->ToString();
  }

  // Prefer metadata status (arbitrarily).
  return !metadata_status.ok() ? metadata_status : data_status;
}

Status LogBlockContainer::Open(LogBlockManager* block_manager,
                               PathInstanceMetadataPB* instance,
                               const string& root_path, const string& id,
                               gscoped_ptr<LogBlockContainer>* container) {
  string common_path = JoinPathSegments(root_path, id);

  // Open the existing metadata and data files for writing.
  string metadata_path = StrCat(common_path, LogBlockManager::kContainerMetadataFileSuffix);
  gscoped_ptr<RWFile> metadata_writer;
  RWFileOptions wr_opts;
  wr_opts.mode = Env::OPEN_EXISTING;

  RETURN_NOT_OK(block_manager->env()->NewRWFile(wr_opts,
                                                metadata_path,
                                                &metadata_writer));
  gscoped_ptr<WritablePBContainerFile> metadata_pb_writer(
      new WritablePBContainerFile(std::move(metadata_writer)));
  RETURN_NOT_OK(metadata_pb_writer->Reopen());

  string data_path = StrCat(common_path, LogBlockManager::kContainerDataFileSuffix);
  gscoped_ptr<RWFile> data_file;
  RWFileOptions rw_opts;
  rw_opts.mode = Env::OPEN_EXISTING;
  RETURN_NOT_OK(block_manager->env()->NewRWFile(rw_opts,
                                                data_path,
                                                &data_file));

  // Create the in-memory container and populate it.
  gscoped_ptr<LogBlockContainer> open_container(new LogBlockContainer(block_manager,
                                                                      instance,
                                                                      root_path,
                                                                      common_path,
                                                                      std::move(metadata_pb_writer),
                                                                      std::move(data_file)));
  VLOG(1) << "Opened log block container " << open_container->ToString();
  container->reset(open_container.release());
  return Status::OK();
}

string LogBlockContainer::MetadataFilePath() const {
  return StrCat(path_, LogBlockManager::kContainerMetadataFileSuffix);
}

string LogBlockContainer::DataFilePath() const {
  return StrCat(path_, LogBlockManager::kContainerDataFileSuffix);
}

Status LogBlockContainer::ReadContainerRecords(deque<BlockRecordPB>* records) const {
  string metadata_path = MetadataFilePath();
  gscoped_ptr<RandomAccessFile> metadata_reader;
  RETURN_NOT_OK(block_manager()->env()->NewRandomAccessFile(metadata_path, &metadata_reader));
  ReadablePBContainerFile pb_reader(std::move(metadata_reader));
  RETURN_NOT_OK(pb_reader.Open());

  uint64_t data_file_size;
  RETURN_NOT_OK(data_file_->Size(&data_file_size));
  deque<BlockRecordPB> local_records;
  Status read_status;
  while (true) {
    local_records.resize(local_records.size() + 1);
    read_status = pb_reader.ReadNextPB(&local_records.back());
    if (!read_status.ok()) {
      // Drop the last element; we didn't use it.
      local_records.pop_back();
      break;
    }
    CheckBlockRecord(local_records.back(), data_file_size);
  }
  // NOTE: 'read_status' will never be OK here.
  if (PREDICT_TRUE(read_status.IsEndOfFile())) {
    // We've reached the end of the file without any problems.
    records->swap(local_records);
    return Status::OK();
  }
  if (read_status.IsIncomplete()) {
    // We found a partial trailing record in a version of the pb container file
    // format that can reliably detect this. Consider this a failed partial
    // write and truncate the metadata file to remove this partial record.
    uint64_t truncate_offset = pb_reader.offset();
    LOG(WARNING) << "Log block manager: Found partial trailing metadata record in container "
                  << ToString() << ": "
                  << "Truncating metadata file to last valid offset: " << truncate_offset;
    gscoped_ptr<RWFile> file;
    RWFileOptions opts;
    opts.mode = Env::OPEN_EXISTING;
    RETURN_NOT_OK(block_manager_->env()->NewRWFile(opts, metadata_path, &file));
    RETURN_NOT_OK(file->Truncate(truncate_offset));
    RETURN_NOT_OK(file->Close());
    // Reopen the PB writer so that it will refresh its metadata about the
    // underlying file and resume appending to the new end of the file.
    RETURN_NOT_OK(metadata_pb_writer_->Reopen());
    records->swap(local_records);
    return Status::OK();
  }
  // If we've made it here, we've found (and are returning) an unrecoverable error.
  return read_status;
}

void LogBlockContainer::CheckBlockRecord(const BlockRecordPB& record,
                                         uint64_t data_file_size) const {
  if (record.op_type() == CREATE &&
      (!record.has_offset()  ||
       !record.has_length()  ||
        record.offset() < 0  ||
        record.length() < 0  ||
        record.offset() + record.length() > data_file_size)) {
    LOG(FATAL) << "Found malformed block record: " << record.DebugString();
  }
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

  CHECK(block_manager()->AddLogBlock(this, block->id(),
                                     total_bytes_written(), block->BytesAppended()));
  UpdateBytesWritten(block->BytesAppended());
  if (full() && block_manager()->metrics()) {
    block_manager()->metrics()->full_containers->Increment();
  }
  return Status::OK();
}

Status LogBlockContainer::DeleteBlock(int64_t offset, int64_t length) {
  DCHECK_GE(offset, 0);
  DCHECK_GE(length, 0);

  // Guaranteed by UpdateBytesWritten().
  DCHECK_EQ(0, offset % instance()->filesystem_block_size_bytes());

  // It is invalid to punch a zero-size hole.
  if (length) {
    std::lock_guard<Mutex> l(data_writer_lock_);
    // Round up to the nearest filesystem block so that the kernel will
    // actually reclaim disk space.
    //
    // It's OK if we exceed the file's total size; the kernel will truncate
    // our request.
    return data_file_->PunchHole(offset, KUDU_ALIGN_UP(
        length, instance()->filesystem_block_size_bytes()));
  }
  return Status::OK();
}

Status LogBlockContainer::WriteData(int64_t offset, const Slice& data) {
  DCHECK_GE(offset, 0);

  std::lock_guard<Mutex> l(data_writer_lock_);
  return data_file_->Write(offset, data);
}

Status LogBlockContainer::ReadData(int64_t offset, size_t length,
                                   Slice* result, uint8_t* scratch) const {
  DCHECK_GE(offset, 0);

  return data_file_->Read(offset, length, result, scratch);
}

Status LogBlockContainer::AppendMetadata(const BlockRecordPB& pb) {
  // Note: We don't check for sufficient disk space for metadata writes in
  // order to allow for block deletion on full disks.
  std::lock_guard<Mutex> l(metadata_pb_writer_lock_);
  return metadata_pb_writer_->Append(pb);
}

Status LogBlockContainer::FlushData(int64_t offset, int64_t length) {
  DCHECK_GE(offset, 0);
  DCHECK_GE(length, 0);

  std::lock_guard<Mutex> l(data_writer_lock_);
  return data_file_->Flush(RWFile::FLUSH_ASYNC, offset, length);
}

Status LogBlockContainer::FlushMetadata() {
  std::lock_guard<Mutex> l(metadata_pb_writer_lock_);
  return metadata_pb_writer_->Flush();
}

Status LogBlockContainer::SyncData() {
  if (FLAGS_enable_data_block_fsync) {
    std::lock_guard<Mutex> l(data_writer_lock_);
    return data_file_->Sync();
  }
  return Status::OK();
}

Status LogBlockContainer::SyncMetadata() {
  if (FLAGS_enable_data_block_fsync) {
    std::lock_guard<Mutex> l(metadata_pb_writer_lock_);
    return metadata_pb_writer_->Sync();
  }
  return Status::OK();
}

Status LogBlockContainer::Preallocate(size_t length) {
  RETURN_NOT_OK(data_file_->PreAllocate(total_bytes_written(), length));
  preallocated_offset_ = total_bytes_written() + length;
  return Status::OK();
}

void LogBlockContainer::UpdateBytesWritten(int64_t more_bytes) {
  DCHECK_GE(more_bytes, 0);

  // The number of bytes is rounded up to the nearest filesystem block so
  // that each Kudu block is guaranteed to be on a filesystem block
  // boundary. This guarantees that the disk space can be reclaimed when
  // the block is deleted.
  total_bytes_written_ += KUDU_ALIGN_UP(more_bytes,
                                        instance()->filesystem_block_size_bytes());
  if (full()) {
    VLOG(1) << "Container " << ToString() << " with size "
            << total_bytes_written_ << " is now full, max size is "
            << FLAGS_log_container_max_size;
  }
}

void LogBlockContainer::ExecClosure(const Closure& task) {
  ThreadPool* pool = FindOrDie(block_manager()->thread_pools_by_root_path_,
                               dir());
  Status s = pool->SubmitClosure(task);
  if (!s.ok()) {
    WARN_NOT_OK(
        s, "Could not submit task to thread pool, running it synchronously");
    task.Run();
  }
}

////////////////////////////////////////////////////////////
// LogBlock
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

LogBlock::LogBlock(LogBlockContainer* container, BlockId block_id,
                   int64_t offset, int64_t length)
    : container_(container),
      block_id_(std::move(block_id)),
      offset_(offset),
      length_(length),
      deleted_(false) {
  DCHECK_GE(offset, 0);
  DCHECK_GE(length, 0);
}

static void DeleteBlockAsync(LogBlockContainer* container,
                             BlockId block_id,
                             int64_t offset,
                             int64_t length) {
  // We don't call SyncData() to synchronize the deletion because it's
  // expensive, and in the worst case, we'll just leave orphaned data
  // behind to be cleaned up in the next GC.
  VLOG(3) << "Freeing space belonging to block " << block_id;
  WARN_NOT_OK(container->DeleteBlock(offset, length),
              Substitute("Could not delete block $0", block_id.ToString()));
}

LogBlock::~LogBlock() {
  if (deleted_) {
    container_->ExecClosure(Bind(&DeleteBlockAsync, container_, block_id_,
                                 offset_, length_));
  }
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
      block_id_(std::move(block_id)),
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

  MicrosecondsInt64 start_time = GetMonoTimeMicros();
  RETURN_NOT_OK(container_->WriteData(block_offset_ + block_length_, data));
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

    // TODO: Flush just the range we care about.
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

      // TODO: Sync just this block's dirty data.
      s = container_->SyncData();
      RETURN_NOT_OK(s);

      // TODO: Sync just this block's dirty metadata.
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

LogBlockManager::LogBlockManager(Env* env, const BlockManagerOptions& opts)
  : mem_tracker_(MemTracker::CreateTracker(-1,
                                           "log_block_manager",
                                           opts.parent_mem_tracker)),
    // TODO: C++11 provides a single-arg constructor
    blocks_by_block_id_(10,
                        BlockMap::hasher(),
                        BlockMap::key_equal(),
                        BlockAllocator(mem_tracker_)),
    env_(DCHECK_NOTNULL(env)),
    read_only_(opts.read_only),
    root_paths_(opts.root_paths),
    root_paths_idx_(0),
    next_block_id_(1) {

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

  DCHECK_GT(root_paths_.size(), 0);
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

  // As LogBlock destructors run, some blocks may be deleted, so we might be
  // waiting here for a little while.
  LOG_SLOW_EXECUTION(INFO, 1000,
                     Substitute("waiting on $0 log block manager thread pools",
                                thread_pools_by_root_path_.size())) {
    for (const ThreadPoolMap::value_type& e :
                  thread_pools_by_root_path_) {
      ThreadPool* p = e.second;
      p->Wait();
      p->Shutdown();
    }
  }

  STLDeleteElements(&all_containers_);
  STLDeleteValues(&thread_pools_by_root_path_);
  STLDeleteValues(&instances_by_root_path_);
  mem_tracker_->UnregisterFromParent();
}

static const char kHolePunchErrorMsg[] =
    "Error during hole punch test. The log block manager requires a "
    "filesystem with hole punching support such as ext4 or xfs. On el6, "
    "kernel version 2.6.32-358 or newer is required. To run without hole "
    "punching (at the cost of some efficiency and scalability), reconfigure "
    "Kudu with --block_manager=file. Refer to the Kudu documentation for more "
    "details. Raw error message follows";

Status LogBlockManager::Create() {
  CHECK(!read_only_);

  RETURN_NOT_OK(Init());

  deque<ScopedFileDeleter*> delete_on_failure;
  ElementDeleter d(&delete_on_failure);

  // The UUIDs and indices will be included in every instance file.
  vector<string> all_uuids(root_paths_.size());
  for (string& u : all_uuids) {
    u = oid_generator()->Next();
  }
  int idx = 0;

  // Ensure the data paths exist and create the instance files.
  unordered_set<string> to_sync;
  for (const string& root_path : root_paths_) {
    bool created;
    RETURN_NOT_OK_PREPEND(env_util::CreateDirIfMissing(env_, root_path, &created),
                          Substitute("Could not create directory $0", root_path));
    if (created) {
      delete_on_failure.push_front(new ScopedFileDeleter(env_, root_path));
      to_sync.insert(DirName(root_path));
    }

    if (FLAGS_log_block_manager_test_hole_punching) {
      RETURN_NOT_OK_PREPEND(CheckHolePunch(root_path), kHolePunchErrorMsg);
    }

    string instance_filename = JoinPathSegments(
        root_path, kInstanceMetadataFileName);
    PathInstanceMetadataFile metadata(env_, kBlockManagerType,
                                      instance_filename);
    RETURN_NOT_OK_PREPEND(metadata.Create(all_uuids[idx], all_uuids), instance_filename);
    delete_on_failure.push_front(new ScopedFileDeleter(env_, instance_filename));
    idx++;
  }

  // Ensure newly created directories are synchronized to disk.
  if (FLAGS_enable_data_block_fsync) {
    for (const string& dir : to_sync) {
      RETURN_NOT_OK_PREPEND(env_->SyncDir(dir),
                            Substitute("Unable to synchronize directory $0", dir));
    }
  }

  // Success: don't delete any files.
  for (ScopedFileDeleter* deleter : delete_on_failure) {
    deleter->Cancel();
  }
  return Status::OK();
}

Status LogBlockManager::Open() {
  RETURN_NOT_OK(Init());

  vector<Status> statuses(root_paths_.size());
  unordered_map<string, PathInstanceMetadataFile*> metadata_files;
  ValueDeleter deleter(&metadata_files);
  for (const string& root_path : root_paths_) {
    InsertOrDie(&metadata_files, root_path, nullptr);
  }

  // Submit each open to its own thread pool and wait for them to complete.
  int i = 0;
  for (const string& root_path : root_paths_) {
    ThreadPool* pool = FindOrDie(thread_pools_by_root_path_, root_path);
    RETURN_NOT_OK_PREPEND(pool->SubmitClosure(
        Bind(&LogBlockManager::OpenRootPath,
             Unretained(this),
             root_path,
             &statuses[i],
             &FindOrDie(metadata_files, root_path))),
                          Substitute("Could not open root path $0", root_path));
    i++;
  }
  for (const ThreadPoolMap::value_type& e :
                thread_pools_by_root_path_) {
    e.second->Wait();
  }

  // Ensure that no tasks failed.
  for (const Status& s : statuses) {
    if (!s.ok()) {
      return s;
    }
  }

  instances_by_root_path_.swap(metadata_files);
  return Status::OK();
}

Status LogBlockManager::CreateBlock(const CreateBlockOptions& opts,
                                    gscoped_ptr<WritableBlock>* block) {
  CHECK(!read_only_);

  // Root paths that are below their reserved space threshold. Initialize the
  // paths from the FullDiskCache. This function-local cache is necessary for
  // correctness in case the FullDiskCache expiration time is set to 0.
  unordered_set<string> full_root_paths(root_paths_.size());
  for (int i = 0; i < root_paths_.size(); i++) {
    if (full_disk_cache_.IsRootFull(root_paths_[i])) {
      InsertOrDie(&full_root_paths, root_paths_[i]);
    }
  }

  // Find a free container. If one cannot be found, create a new one.
  // In case one or more root paths have hit their reserved space limit, we
  // retry until we have exhausted all root paths.
  //
  // TODO: should we cap the number of outstanding containers and force
  // callers to block if we've reached it?
  LogBlockContainer* container = nullptr;
  while (!container) {
    container = GetAvailableContainer(full_root_paths);
    if (!container) {
      // If all root paths are full, we cannot allocate a block.
      if (full_root_paths.size() == root_paths_.size()) {
        return Status::IOError("Unable to allocate block: All data directories are full. "
                               "Please free some disk space or consider changing the "
                               "fs_data_dirs_reserved_bytes configuration parameter",
                               "", ENOSPC);
      }
      // Round robin through the root paths to select where the next
      // container should live.
      // TODO: Consider a more random scheme for block placement.
      int32 cur_idx;
      int32 next_idx;
      do {
        cur_idx = root_paths_idx_.Load();
        next_idx = (cur_idx + 1) % root_paths_.size();
      } while (!root_paths_idx_.CompareAndSet(cur_idx, next_idx) ||
               ContainsKey(full_root_paths, root_paths_[cur_idx]));
      string root_path = root_paths_[cur_idx];
      if (full_disk_cache_.IsRootFull(root_path)) {
        InsertOrDie(&full_root_paths, root_path);
        continue;
      }

      // Guaranteed by LogBlockManager::Open().
      PathInstanceMetadataFile* instance = FindOrDie(instances_by_root_path_, root_path);

      gscoped_ptr<LogBlockContainer> new_container;
      RETURN_NOT_OK_PREPEND(LogBlockContainer::Create(this,
                                                      instance->metadata(),
                                                      root_path,
                                                      &new_container),
                            "Could not create new log block container at " + root_path);
      container = new_container.release();
      {
        std::lock_guard<simple_spinlock> l(lock_);
        dirty_dirs_.insert(root_path);
        AddNewContainerUnlocked(container);
      }
    }

    // By preallocating with each CreateBlock(), we're effectively
    // maintaining a rolling buffer of preallocated data just ahead of where
    // the next write will fall.
    if (FLAGS_log_container_preallocate_bytes) {
      // TODO: The use of FLAGS_log_container_preallocate_bytes may be a poor
      // estimate for the number of bytes we are about to consume for a block.
      // In the future, we may also want to implement some type of "hard" limit
      // to ensure that a giant block doesn't blow through the configured
      // reserved disk space.
      Status s = env_util::VerifySufficientDiskSpace(env_, container->DataFilePath(),
                                                     FLAGS_log_container_preallocate_bytes,
                                                     FLAGS_fs_data_dirs_reserved_bytes);
      if (PREDICT_FALSE(s.IsIOError() && s.posix_code() == ENOSPC)) {
        LOG(ERROR) << Substitute("Log block manager: Insufficient disk space under path $0: "
                                 "Creation of new data blocks under this path can be retried after "
                                 "$1 seconds: $2", container->root_path(),
                                 FLAGS_log_block_manager_full_disk_cache_seconds, s.ToString());
        // Blacklist this root globally and locally.
        full_disk_cache_.MarkRootFull(container->root_path());
        InsertOrDie(&full_root_paths, container->root_path());
        MakeContainerAvailable(container);
        container = nullptr;
        continue;
      }
      RETURN_NOT_OK(s); // Catch other types of IOErrors, etc.
      RETURN_NOT_OK(container->Preallocate(FLAGS_log_container_preallocate_bytes));
    }
  }

  // Generate a free block ID.
  // We have to loop here because earlier versions used non-sequential block IDs,
  // and thus we may have to "skip over" some block IDs that are claimed.
  BlockId new_block_id;
  do {
    new_block_id.SetId(next_block_id_.Increment());
  } while (!TryUseBlockId(new_block_id));

  block->reset(new internal::LogWritableBlock(container,
                                              new_block_id,
                                              container->total_bytes_written()));
  VLOG(3) << "Created block " << (*block)->id() << " in container "
          << container->ToString();
  return Status::OK();
}

Status LogBlockManager::CreateBlock(gscoped_ptr<WritableBlock>* block) {
  return CreateBlock(CreateBlockOptions(), block);
}

Status LogBlockManager::OpenBlock(const BlockId& block_id,
                                  gscoped_ptr<ReadableBlock>* block) {
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

  // Record the on-disk deletion.
  //
  // TODO: what if this fails? Should we restore the in-memory block?
  BlockRecordPB record;
  block_id.CopyToPB(record.mutable_block_id());
  record.set_op_type(DELETE);
  record.set_timestamp_us(GetCurrentTimeMicros());
  RETURN_NOT_OK_PREPEND(lb->container()->AppendMetadata(record),
                        "Unable to append deletion record to block metadata");

  // We don't bother fsyncing the metadata append for deletes in order to avoid
  // the disk overhead. Even if we did fsync it, we'd still need to account for
  // garbage at startup time (in the event that we crashed just before the
  // fsync). TODO: Implement GC of orphaned blocks. See KUDU-829.

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

int64_t LogBlockManager::CountBlocksForTests() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return blocks_by_block_id_.size();
}

void LogBlockManager::AddNewContainerUnlocked(LogBlockContainer* container) {
  DCHECK(lock_.is_locked());
  all_containers_.push_back(container);
  if (metrics()) {
    metrics()->containers->Increment();
    if (container->full()) {
      metrics()->full_containers->Increment();
    }
  }
}

LogBlockContainer* LogBlockManager::GetAvailableContainer(
    const unordered_set<string>& full_root_paths) {
  LogBlockContainer* container = nullptr;
  int64_t disk_full_containers_delta = 0;
  MonoTime now = MonoTime::Now();
  {
    std::lock_guard<simple_spinlock> l(lock_);
    // Move containers from disk_full -> available.
    while (!disk_full_containers_.empty() &&
           disk_full_containers_.top().second < now) {
      available_containers_.push_back(disk_full_containers_.top().first);
      disk_full_containers_.pop();
      disk_full_containers_delta -= 1;
    }

    // Return the first currently-available non-full-disk container (according to
    // our full-disk cache).
    while (!container && !available_containers_.empty()) {
      container = available_containers_.front();
      available_containers_.pop_front();
      MonoTime expires;
      // Note: We must check 'full_disk_cache_' before 'full_root_paths' in
      // order to correctly use the expiry time provided by 'full_disk_cache_'.
      if (full_disk_cache_.IsRootFull(container->root_path(), &expires) ||
          ContainsKey(full_root_paths, container->root_path())) {
        if (!expires.Initialized()) {
          // It's no longer in the cache but we still consider it unusable.
          // It will be moved back into 'available_containers_' on the next call.
          expires = now;
        }
        disk_full_containers_.emplace(container, expires);
        disk_full_containers_delta += 1;
        container = nullptr;
      }
    }
  }

  // Update the metrics in a batch.
  if (metrics()) {
    metrics()->unavailable_containers->IncrementBy(disk_full_containers_delta);
  }

  // Return the container we found, or null if we don't have anything available.
  return container;
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
  available_containers_.push_back(container);
}

Status LogBlockManager::SyncContainer(const LogBlockContainer& container) {
  Status s;
  bool to_sync = false;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    to_sync = dirty_dirs_.erase(container.dir());
  }

  if (to_sync && FLAGS_enable_data_block_fsync) {
    s = env_->SyncDir(container.dir());

    // If SyncDir fails, the container directory must be restored to
    // dirty_dirs_. Otherwise a future successful LogWritableBlock::Close()
    // on this container won't call SyncDir again, and the container might
    // be lost on crash.
    //
    // In the worst case (another block synced this container as we did),
    // we'll sync it again needlessly.
    if (!s.ok()) {
      std::lock_guard<simple_spinlock> l(lock_);
      dirty_dirs_.insert(container.dir());
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

bool LogBlockManager::AddLogBlock(LogBlockContainer* container,
                                  const BlockId& block_id,
                                  int64_t offset,
                                  int64_t length) {
  std::lock_guard<simple_spinlock> l(lock_);
  scoped_refptr<LogBlock> lb(new LogBlock(container, block_id, offset, length));
  mem_tracker_->Consume(kudu_malloc_usable_size(lb.get()));

  return AddLogBlockUnlocked(lb);
}

bool LogBlockManager::AddLogBlockUnlocked(const scoped_refptr<LogBlock>& lb) {
  DCHECK(lock_.is_locked());

  if (!InsertIfNotPresent(&blocks_by_block_id_, lb->block_id(), lb)) {
    return false;
  }

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
    mem_tracker_->Release(kudu_malloc_usable_size(result.get()));

    if (metrics()) {
      metrics()->blocks_under_management->Decrement();
      metrics()->bytes_under_management->DecrementBy(result->length());
    }
  }

  return result;
}

void LogBlockManager::OpenRootPath(const string& root_path,
                                   Status* result_status,
                                   PathInstanceMetadataFile** result_metadata) {
  if (!env_->FileExists(root_path)) {
    *result_status = Status::NotFound(Substitute(
        "LogBlockManager at $0 not found", root_path));
    return;
  }

  // Open and lock the metadata instance file.
  string instance_filename = JoinPathSegments(
      root_path, kInstanceMetadataFileName);
  gscoped_ptr<PathInstanceMetadataFile> metadata(
      new PathInstanceMetadataFile(env_, kBlockManagerType,
                                   instance_filename));
  Status s = metadata->LoadFromDisk();
  if (!s.ok()) {
    *result_status = s.CloneAndPrepend(Substitute(
        "Could not open $0", instance_filename));
    return;
  }
  if (FLAGS_block_manager_lock_dirs) {
    s = metadata->Lock();
    if (!s.ok()) {
      Status new_status = s.CloneAndPrepend(Substitute(
          "Could not lock $0", instance_filename));
      if (read_only_) {
        // Not fatal in read-only mode.
        LOG(WARNING) << new_status.ToString();
        LOG(WARNING) << "Proceeding without lock";
      } else {
        *result_status = new_status;
        return;
      }
    }
  }

  // Find all containers and open them.
  vector<string> children;
  s = env_->GetChildren(root_path, &children);
  if (!s.ok()) {
    *result_status = s.CloneAndPrepend(Substitute(
        "Could not list children of $0", root_path));
    return;
  }
  for (const string& child : children) {
    string id;
    if (!TryStripSuffixString(child, LogBlockManager::kContainerMetadataFileSuffix, &id)) {
      continue;
    }
    gscoped_ptr<LogBlockContainer> container;
    s = LogBlockContainer::Open(this, metadata->metadata(),
                                root_path, id, &container);
    if (!s.ok()) {
      *result_status = s.CloneAndPrepend(Substitute(
          "Could not open container $0", id));
      return;
    }

    // Populate the in-memory block maps using each container's records.
    deque<BlockRecordPB> records;
    s = container->ReadContainerRecords(&records);
    if (!s.ok()) {
      *result_status = s.CloneAndPrepend(Substitute(
          "Could not read records from container $0", container->ToString()));
      return;
    }

    // Process the records, building a container-local map.
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
    UntrackedBlockMap blocks_in_container;
    uint64_t max_block_id = 0;
    for (const BlockRecordPB& r : records) {
      ProcessBlockRecord(r, container.get(), &blocks_in_container);
      max_block_id = std::max(max_block_id, r.block_id().id());
    }
    next_block_id_.StoreMax(max_block_id + 1);

    // Under the lock, merge this map into the main block map and add
    // the container.
    {
      std::lock_guard<simple_spinlock> l(lock_);
      // To avoid cacheline contention during startup, we aggregate all of the
      // memory in a local and add it to the mem-tracker in a single increment
      // at the end of this loop.
      int64_t mem_usage = 0;
      for (const UntrackedBlockMap::value_type& e : blocks_in_container) {
        if (!AddLogBlockUnlocked(e.second)) {
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

  *result_status = Status::OK();
  *result_metadata = metadata.release();
}

void LogBlockManager::ProcessBlockRecord(const BlockRecordPB& record,
                                         LogBlockContainer* container,
                                         UntrackedBlockMap* block_map) {
  BlockId block_id(BlockId::FromPB(record.block_id()));
  switch (record.op_type()) {
    case CREATE: {
      scoped_refptr<LogBlock> lb(new LogBlock(container, block_id,
                                              record.offset(), record.length()));
      if (!InsertIfNotPresent(block_map, block_id, lb)) {
        LOG(FATAL) << "Found duplicate CREATE record for block "
                   << block_id.ToString() << " in container "
                   << container->ToString() << ": "
                   << record.DebugString();
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
      container->UpdateBytesWritten(record.length());
      break;
    }
    case DELETE:
      if (block_map->erase(block_id) != 1) {
        LOG(FATAL) << "Found DELETE record for invalid block "
                   << block_id.ToString() << " in container "
                   << container->ToString() << ": "
                   << record.DebugString();
      }
      VLOG(2) << Substitute("Found DELETE block $0", block_id.ToString());
      break;
    default:
      LOG(FATAL) << "Found unknown op type in block record: "
                 << record.DebugString();
  }
}

Status LogBlockManager::CheckHolePunch(const string& path) {
  // Arbitrary constants.
  static uint64_t kFileSize = 4096 * 4;
  static uint64_t kHoleOffset = 4096;
  static uint64_t kHoleSize = 8192;
  static uint64_t kPunchedFileSize = kFileSize - kHoleSize;

  // Open the test file.
  string filename = JoinPathSegments(path, "hole_punch_test_file");
  gscoped_ptr<RWFile> file;
  RWFileOptions opts;
  RETURN_NOT_OK(env_->NewRWFile(opts, filename, &file));

  // The file has been created; delete it on exit no matter what happens.
  ScopedFileDeleter file_deleter(env_, filename);

  // Preallocate it, making sure the file's size is what we'd expect.
  uint64_t sz;
  RETURN_NOT_OK(file->PreAllocate(0, kFileSize));
  RETURN_NOT_OK(env_->GetFileSizeOnDisk(filename, &sz));
  if (sz != kFileSize) {
    return Status::IOError(Substitute(
        "Unexpected pre-punch file size for $0: expected $1 but got $2",
        filename, kFileSize, sz));
  }

  // Punch the hole, testing the file's size again.
  RETURN_NOT_OK(file->PunchHole(kHoleOffset, kHoleSize));
  RETURN_NOT_OK(env_->GetFileSizeOnDisk(filename, &sz));
  if (sz != kPunchedFileSize) {
    return Status::IOError(Substitute(
        "Unexpected post-punch file size for $0: expected $1 but got $2",
        filename, kPunchedFileSize, sz));
  }

  return Status::OK();
}

Status LogBlockManager::Init() {
  // Initialize thread pools.
  ThreadPoolMap pools;
  ValueDeleter d(&pools);
  int i = 0;
  for (const string& root : root_paths_) {
    gscoped_ptr<ThreadPool> p;
    RETURN_NOT_OK_PREPEND(ThreadPoolBuilder(Substitute("lbm root $0", i++))
                          .set_max_threads(1)
                          .Build(&p),
                          "Could not build thread pool");
    InsertOrDie(&pools, root, p.release());
  }
  thread_pools_by_root_path_.swap(pools);

  return Status::OK();
}

std::string LogBlockManager::ContainerPathForTests(internal::LogBlockContainer* container) {
  return container->ToString();
}

bool FullDiskCache::IsRootFull(const std::string& root_path, MonoTime* expires_out) const {
  const MonoTime* expires;
  {
    shared_lock<rw_spinlock> l(lock_.get_lock());
    expires = FindOrNull(cache_, root_path);
  }
  if (expires == nullptr) return false; // No entry exists.
  if (*expires < MonoTime::Now()) return false; // Expired.
  if (expires_out != nullptr) {
    *expires_out = *expires;
  }
  return true; // Root is still full according to the cache.
}

void FullDiskCache::MarkRootFull(const string& root_path) {
  MonoTime expires = MonoTime::Now() +
      MonoDelta::FromSeconds(FLAGS_log_block_manager_full_disk_cache_seconds);
  std::lock_guard<percpu_rwlock> l(lock_);
  InsertOrUpdate(&cache_, root_path, expires); // Last one wins.
}

} // namespace fs
} // namespace kudu
